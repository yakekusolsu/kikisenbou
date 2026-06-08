from __future__ import annotations

import asyncio
import json
import logging
import secrets
import sqlite3
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from fastapi import Depends, FastAPI, Form, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import Response


BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"
DB_PATH = BASE_DIR / "database.db"
LOG_DIR = BASE_DIR / "logs"
AUDIO_DIR = BASE_DIR / "audio"
DISCORD_API = "https://discord.com/api/v10"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_config() -> dict:
    with CONFIG_PATH.open("r", encoding="utf-8") as fp:
        return json.load(fp)


CONFIG = load_config()


def setup_logging() -> None:
    LOG_DIR.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(LOG_DIR / "kikisenbou.log", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def init_database() -> None:
    DB_PATH.parent.mkdir(exist_ok=True)
    AUDIO_DIR.mkdir(exist_ok=True)
    LOG_DIR.mkdir(exist_ok=True)
    with sqlite3.connect(DB_PATH) as con:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA foreign_keys=ON")
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                avatar TEXT,
                last_login_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS bot_guilds (
                guild_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                icon_url TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id TEXT PRIMARY KEY,
                guild_name TEXT NOT NULL,
                public INTEGER NOT NULL DEFAULT 1,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS allowed_roles (
                guild_id TEXT NOT NULL,
                role_id TEXT NOT NULL,
                role_name TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (guild_id, role_id)
            );

            CREATE TABLE IF NOT EXISTS denied_roles (
                guild_id TEXT NOT NULL,
                role_id TEXT NOT NULL,
                role_name TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (guild_id, role_id)
            );

            CREATE TABLE IF NOT EXISTS voice_connections (
                guild_id TEXT PRIMARY KEY,
                channel_id TEXT NOT NULL,
                channel_name TEXT NOT NULL,
                connected_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS listener_counts (
                guild_id TEXT PRIMARY KEY,
                count INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS audit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                guild_id TEXT,
                user_id TEXT,
                details TEXT,
                created_at TEXT NOT NULL
            );
            """
        )


def db_execute(sql: str, params: tuple = ()) -> None:
    with sqlite3.connect(DB_PATH) as con:
        con.execute(sql, params)
        con.commit()


def db_fetchone(sql: str, params: tuple = ()) -> sqlite3.Row | None:
    with sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        return con.execute(sql, params).fetchone()


def db_fetchall(sql: str, params: tuple = ()) -> list[sqlite3.Row]:
    with sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        return con.execute(sql, params).fetchall()


def audit(event_type: str, guild_id: str | None = None, user_id: str | None = None, details: str = "") -> None:
    db_execute(
        "INSERT INTO audit_logs(event_type, guild_id, user_id, details, created_at) VALUES (?, ?, ?, ?, ?)",
        (event_type, guild_id, user_id, details, utc_now()),
    )


class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: FastAPI, limit_per_minute: int):
        super().__init__(app)
        self.limit = limit_per_minute
        self.window = 60
        self.clients: dict[str, deque[float]] = defaultdict(deque)

    async def dispatch(self, request: Request, call_next) -> Response:
        if request.url.path.startswith("/static/"):
            return await call_next(request)
        ip = request.client.host if request.client else "unknown"
        now = time.monotonic()
        bucket = self.clients[ip]
        while bucket and now - bucket[0] > self.window:
            bucket.popleft()
        if len(bucket) >= self.limit:
            return Response("rate limit exceeded", status_code=429)
        bucket.append(now)
        return await call_next(request)


class CSRFMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
            session_token = request.session.get("csrf_token")
            form_token = None
            content_type = request.headers.get("content-type", "")
            if "application/x-www-form-urlencoded" in content_type or "multipart/form-data" in content_type:
                form = await request.form()
                form_token = form.get("csrf_token")
            header_token = request.headers.get("x-csrf-token")
            if not session_token or not secrets.compare_digest(session_token, str(form_token or header_token or "")):
                return Response("invalid csrf token", status_code=403)
        return await call_next(request)


class ClientStream:
    def __init__(self, websocket: WebSocket):
        self.websocket = websocket
        self.queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=120)
        self.task = asyncio.create_task(self._send_loop())

    async def enqueue(self, frame: bytes) -> None:
        if self.queue.full():
            with suppress(asyncio.QueueEmpty):
                self.queue.get_nowait()
        with suppress(asyncio.QueueFull):
            self.queue.put_nowait(frame)

    async def close(self) -> None:
        self.task.cancel()
        with suppress(asyncio.CancelledError):
            await self.task

    async def _send_loop(self) -> None:
        while True:
            frame = await self.queue.get()
            await self.websocket.send_bytes(frame)


class AudioHub:
    def __init__(self):
        self.subscribers: dict[str, set[ClientStream]] = defaultdict(set)
        self.ws_attempts: dict[str, deque[float]] = defaultdict(deque)

    def count(self, guild_id: str) -> int:
        return len(self.subscribers.get(guild_id, set()))

    async def add(self, guild_id: str, client: ClientStream) -> None:
        self.subscribers[guild_id].add(client)
        self._write_count(guild_id)

    async def remove(self, guild_id: str, client: ClientStream) -> None:
        self.subscribers[guild_id].discard(client)
        await client.close()
        self._write_count(guild_id)

    async def broadcast(self, guild_id: str, frame: bytes) -> None:
        clients = list(self.subscribers.get(guild_id, set()))
        for client in clients:
            await client.enqueue(frame)

    def websocket_rate_allowed(self, ip: str) -> bool:
        now = time.monotonic()
        bucket = self.ws_attempts[ip]
        while bucket and now - bucket[0] > 60:
            bucket.popleft()
        if len(bucket) >= 60:
            return False
        bucket.append(now)
        return True

    def _write_count(self, guild_id: str) -> None:
        db_execute(
            """
            INSERT INTO listener_counts(guild_id, count, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET count = excluded.count, updated_at = excluded.updated_at
            """,
            (guild_id, self.count(guild_id), utc_now()),
        )


audio_hub = AudioHub()
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    init_database()
    yield


app = FastAPI(lifespan=lifespan)
app.add_middleware(RateLimitMiddleware, limit_per_minute=int(CONFIG.get("rate_limit_per_minute", 120)))
app.add_middleware(CSRFMiddleware)
app.add_middleware(
    SessionMiddleware,
    secret_key=CONFIG.get("session_secret") or secrets.token_urlsafe(32),
    https_only=CONFIG.get("redirect_uri", "").startswith("https://"),
    same_site="lax",
    max_age=60 * 60 * 24 * 7,
)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


def ensure_csrf(request: Request) -> str:
    token = request.session.get("csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        request.session["csrf_token"] = token
    return token


def current_user(request: Request) -> dict[str, Any] | None:
    return request.session.get("user")


def require_user(request: Request) -> dict[str, Any]:
    user = current_user(request)
    if not user:
        raise HTTPException(status_code=307, headers={"Location": "/login"})
    return user


def user_guild_ids(request: Request) -> set[str]:
    return {str(guild["id"]) for guild in request.session.get("guilds", [])}


def user_guild_info(request: Request, guild_id: str) -> dict[str, Any] | None:
    for guild in request.session.get("guilds", []):
        if str(guild["id"]) == str(guild_id):
            return guild
    return None


def user_has_admin_guild_permission(guild: dict[str, Any] | None) -> bool:
    if not guild:
        return False
    if guild.get("owner"):
        return True
    permissions = int(guild.get("permissions", 0))
    return bool(permissions & 0x8)


def avatar_url(user: dict[str, Any]) -> str | None:
    avatar = user.get("avatar")
    if not avatar:
        return None
    extension = "gif" if str(avatar).startswith("a_") else "png"
    return f"https://cdn.discordapp.com/avatars/{user['id']}/{avatar}.{extension}?size=64"


async def discord_oauth_exchange(code: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    async with httpx.AsyncClient(timeout=15) as client:
        token_resp = await client.post(
            f"{DISCORD_API}/oauth2/token",
            data={
                "client_id": CONFIG["client_id"],
                "client_secret": CONFIG["client_secret"],
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": CONFIG["redirect_uri"],
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        token_resp.raise_for_status()
        token = token_resp.json()["access_token"]
        headers = {"Authorization": f"Bearer {token}"}
        user_resp, guilds_resp = await asyncio.gather(
            client.get(f"{DISCORD_API}/users/@me", headers=headers),
            client.get(f"{DISCORD_API}/users/@me/guilds", headers=headers),
        )
        user_resp.raise_for_status()
        guilds_resp.raise_for_status()
        return user_resp.json(), guilds_resp.json()


async def fetch_member_role_ids(guild_id: str, user_id: str) -> set[str]:
    token = CONFIG.get("token")
    if not token:
        return set()
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(
            f"{DISCORD_API}/guilds/{guild_id}/members/{user_id}",
            headers={"Authorization": f"Bot {token}"},
        )
    if resp.status_code >= 400:
        logging.warning("failed to fetch member roles: guild=%s user=%s status=%s body=%s", guild_id, user_id, resp.status_code, resp.text[:200])
        return set()
    return {str(role_id) for role_id in resp.json().get("roles", [])}


async def can_listen(request: Request, guild_id: str, user: dict[str, Any]) -> bool:
    if guild_id not in user_guild_ids(request):
        return False
    settings = db_fetchone("SELECT public FROM guild_settings WHERE guild_id = ?", (guild_id,))
    public = True if settings is None else bool(settings["public"])
    denied = {row["role_id"] for row in db_fetchall("SELECT role_id FROM denied_roles WHERE guild_id = ?", (guild_id,))}
    allowed = {row["role_id"] for row in db_fetchall("SELECT role_id FROM allowed_roles WHERE guild_id = ?", (guild_id,))}
    roles = await fetch_member_role_ids(guild_id, str(user["id"]))

    if denied and denied.intersection(roles):
        return False
    if public:
        return True
    if allowed.intersection(roles):
        return True
    return user_has_admin_guild_permission(user_guild_info(request, guild_id))


def login_redirect_url(request: Request) -> str:
    state = secrets.token_urlsafe(24)
    request.session["oauth_state"] = state
    params = {
        "client_id": CONFIG["client_id"],
        "redirect_uri": CONFIG["redirect_uri"],
        "response_type": "code",
        "scope": "identify guilds",
        "state": state,
        "prompt": "none",
    }
    return f"{DISCORD_API}/oauth2/authorize?{httpx.QueryParams(params)}"


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    if current_user(request):
        return RedirectResponse("/dashboard", status_code=302)
    return RedirectResponse("/login", status_code=302)


@app.get("/login", response_class=HTMLResponse)
async def login(request: Request):
    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "login_url": "/auth/discord",
            "csrf_token": ensure_csrf(request),
        },
    )


@app.get("/auth/discord")
async def auth_discord(request: Request):
    return RedirectResponse(login_redirect_url(request), status_code=302)


@app.get("/callback")
async def callback(request: Request, code: str | None = None, state: str | None = None):
    expected_state = request.session.get("oauth_state")
    if not code or not state or not expected_state or not secrets.compare_digest(state, expected_state):
        audit("エラー", details="invalid oauth state")
        raise HTTPException(status_code=400, detail="invalid oauth state")
    try:
        user, guilds = await discord_oauth_exchange(code)
    except Exception as exc:
        logging.exception("oauth callback failed")
        audit("エラー", details=f"oauth callback failed: {exc}")
        raise HTTPException(status_code=400, detail="Discord認証に失敗しました") from exc

    request.session.pop("oauth_state", None)
    request.session["user"] = {
        "id": str(user["id"]),
        "username": user["username"],
        "avatar": user.get("avatar"),
        "avatar_url": avatar_url(user),
    }
    request.session["guilds"] = guilds
    db_execute(
        """
        INSERT INTO users(user_id, username, avatar, last_login_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username = excluded.username,
            avatar = excluded.avatar,
            last_login_at = excluded.last_login_at
        """,
        (str(user["id"]), user["username"], user.get("avatar"), utc_now()),
    )
    audit("ログイン", user_id=str(user["id"]), details=user["username"])
    return RedirectResponse("/dashboard", status_code=302)


@app.post("/logout")
async def logout(request: Request, csrf_token: str = Form(...)):
    user = current_user(request)
    if user:
        audit("ログアウト", user_id=str(user["id"]), details=user["username"])
    request.session.clear()
    return RedirectResponse("/login", status_code=302)


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request, user: dict[str, Any] = Depends(require_user)):
    bot_rows = db_fetchall("SELECT guild_id, name, icon_url FROM bot_guilds ORDER BY name COLLATE NOCASE")
    allowed_ids = user_guild_ids(request)
    guilds = [dict(row) for row in bot_rows if row["guild_id"] in allowed_ids]
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "user": user,
            "guilds": guilds,
            "csrf_token": ensure_csrf(request),
        },
    )


@app.get("/guild/{guild_id}", response_class=HTMLResponse)
async def guild_page(request: Request, guild_id: str, user: dict[str, Any] = Depends(require_user)):
    if guild_id not in user_guild_ids(request):
        raise HTTPException(status_code=403, detail="このサーバーにアクセスできません")
    guild = db_fetchone("SELECT guild_id, name, icon_url FROM bot_guilds WHERE guild_id = ?", (guild_id,))
    if not guild:
        raise HTTPException(status_code=404, detail="Botが参加していないサーバーです")
    channels = db_fetchall("SELECT channel_id, channel_name, connected_at FROM voice_connections WHERE guild_id = ?", (guild_id,))
    permitted = await can_listen(request, guild_id, user)
    return templates.TemplateResponse(
        "guild.html",
        {
            "request": request,
            "user": user,
            "guild": dict(guild),
            "channels": [dict(row) for row in channels],
            "permitted": permitted,
            "listen_channel": None,
            "csrf_token": ensure_csrf(request),
        },
    )


@app.get("/guild/{guild_id}/listen/{channel_id}", response_class=HTMLResponse)
async def listen_page(request: Request, guild_id: str, channel_id: str, user: dict[str, Any] = Depends(require_user)):
    row = db_fetchone(
        "SELECT channel_id, channel_name FROM voice_connections WHERE guild_id = ? AND channel_id = ?",
        (guild_id, channel_id),
    )
    if not row:
        raise HTTPException(status_code=404, detail="BotはこのVCに接続していません")
    if not await can_listen(request, guild_id, user):
        raise HTTPException(status_code=403, detail="視聴権限がありません")
    guild = db_fetchone("SELECT guild_id, name, icon_url FROM bot_guilds WHERE guild_id = ?", (guild_id,))
    return templates.TemplateResponse(
        "guild.html",
        {
            "request": request,
            "user": user,
            "guild": dict(guild) if guild else {"guild_id": guild_id, "name": "Unknown", "icon_url": None},
            "channels": [],
            "permitted": True,
            "listen_channel": dict(row),
            "csrf_token": ensure_csrf(request),
        },
    )


@app.websocket("/internal/audio/{guild_id}")
async def internal_audio(websocket: WebSocket, guild_id: str):
    secret = websocket.query_params.get("secret", "")
    expected = CONFIG.get("internal_audio_secret", "")
    if not expected or not secrets.compare_digest(secret, expected):
        await websocket.close(code=1008)
        return
    await websocket.accept()
    logging.info("internal audio producer connected: guild=%s", guild_id)
    try:
        while True:
            frame = await websocket.receive_bytes()
            await audio_hub.broadcast(guild_id, frame)
    except WebSocketDisconnect:
        logging.info("internal audio producer disconnected: guild=%s", guild_id)
    except Exception as exc:
        logging.exception("internal audio error")
        audit("エラー", guild_id=guild_id, details=f"internal audio: {exc}")


@app.websocket("/ws/listen/{guild_id}/{channel_id}")
async def listen_ws(websocket: WebSocket, guild_id: str, channel_id: str):
    ip = websocket.client.host if websocket.client else "unknown"
    if not audio_hub.websocket_rate_allowed(ip):
        await websocket.close(code=1008)
        return
    session = websocket.session
    user = session.get("user")
    if not user:
        await websocket.close(code=1008)
        return
    request_like = type("RequestLike", (), {"session": session})()
    row = db_fetchone(
        "SELECT channel_id FROM voice_connections WHERE guild_id = ? AND channel_id = ?",
        (guild_id, channel_id),
    )
    if not row or not await can_listen(request_like, guild_id, user):
        await websocket.close(code=1008)
        return

    await websocket.accept()
    client = ClientStream(websocket)
    await audio_hub.add(guild_id, client)
    audit("視聴開始", guild_id=guild_id, user_id=str(user["id"]), details=channel_id)
    try:
        while True:
            # The browser may send keepalive text; audio is server-to-client only.
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        logging.warning("listener websocket error: guild=%s user=%s error=%s", guild_id, user["id"], exc)
    finally:
        await audio_hub.remove(guild_id, client)
        audit("視聴終了", guild_id=guild_id, user_id=str(user["id"]), details=channel_id)


def main() -> None:
    import uvicorn

    setup_logging()
    init_database()
    session_secret = CONFIG.get("session_secret", "")
    internal_secret = CONFIG.get("internal_audio_secret", "")
    if session_secret.startswith("change-this") or internal_secret.startswith("change-this"):
        logging.warning("config.json の session_secret と internal_audio_secret は本番前に必ず変更してください。")
    uvicorn.run(
        "web:app",
        host=CONFIG.get("host", "0.0.0.0"),
        port=int(CONFIG.get("port", 8000)),
        reload=False,
        app_dir=str(BASE_DIR),
    )


if __name__ == "__main__":
    main()
