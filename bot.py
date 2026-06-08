from __future__ import annotations

import asyncio
import json
import logging
import secrets
import sqlite3
import sys
from array import array
from collections import defaultdict, deque
from contextlib import suppress
from datetime import datetime, timezone
from pathlib import Path
from typing import Deque
from urllib.parse import urlencode

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands

try:
    from discord.ext import voice_recv
except Exception as exc:  # pragma: no cover - startup guard
    voice_recv = None
    VOICE_RECV_IMPORT_ERROR = exc
else:
    VOICE_RECV_IMPORT_ERROR = None


BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"
DB_PATH = BASE_DIR / "database.db"
LOG_DIR = BASE_DIR / "logs"
AUDIO_DIR = BASE_DIR / "audio"
PCM_FRAME_BYTES = 3840  # 20 ms, 48 kHz, stereo, signed 16-bit PCM.


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"config.json not found: {CONFIG_PATH}")
    with CONFIG_PATH.open("r", encoding="utf-8") as fp:
        return json.load(fp)


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


def audit(event_type: str, guild_id: int | str | None = None, user_id: int | str | None = None, details: str = "") -> None:
    db_execute(
        "INSERT INTO audit_logs(event_type, guild_id, user_id, details, created_at) VALUES (?, ?, ?, ?, ?)",
        (event_type, str(guild_id) if guild_id else None, str(user_id) if user_id else None, details, utc_now()),
    )


def mix_pcm_frames(frames: list[bytes]) -> bytes:
    if len(frames) == 1:
        return frames[0]
    sample_count = len(frames[0]) // 2
    totals = [0] * sample_count
    for frame in frames:
        samples = array("h")
        samples.frombytes(frame)
        if sys.byteorder != "little":
            samples.byteswap()
        for index, sample in enumerate(samples):
            totals[index] += sample
    mixed = array("h", (max(-32768, min(32767, sample)) for sample in totals))
    if sys.byteorder != "little":
        mixed.byteswap()
    return mixed.tobytes()


class InternalAudioProducer:
    """Maintains one producer WebSocket from the bot to web.py for a guild."""

    def __init__(self, guild_id: int, config: dict, logger: logging.Logger):
        self.guild_id = guild_id
        self.config = config
        self.logger = logger
        self.queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=200)
        self.task: asyncio.Task | None = None
        self.closed = asyncio.Event()

    def url(self) -> str:
        base = self.config.get("internal_ws_url") or "ws://127.0.0.1:8000/internal/audio/{guild_id}"
        base = base.format(guild_id=self.guild_id)
        separator = "&" if "?" in base else "?"
        return f"{base}{separator}{urlencode({'secret': self.config['internal_audio_secret']})}"

    def start(self) -> None:
        if not self.task or self.task.done():
            self.closed.clear()
            self.task = asyncio.create_task(self._run(), name=f"audio-producer-{self.guild_id}")

    async def stop(self) -> None:
        self.closed.set()
        if self.task:
            self.task.cancel()
            with suppress(asyncio.CancelledError):
                await self.task

    def send_nowait(self, frame: bytes) -> None:
        self.start()
        if self.queue.full():
            with suppress(asyncio.QueueEmpty):
                self.queue.get_nowait()
        with suppress(asyncio.QueueFull):
            self.queue.put_nowait(frame)

    async def _run(self) -> None:
        retry_seconds = 2
        while not self.closed.is_set():
            try:
                timeout = aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=None)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.ws_connect(self.url(), heartbeat=20) as ws:
                        self.logger.info("connected internal audio producer for guild %s", self.guild_id)
                        retry_seconds = 2
                        while not self.closed.is_set():
                            frame = await self.queue.get()
                            await ws.send_bytes(frame)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.warning("internal audio producer disconnected for guild %s: %s", self.guild_id, exc)
                audit("エラー", self.guild_id, details=f"internal audio producer: {exc}")
                await asyncio.sleep(retry_seconds)
                retry_seconds = min(retry_seconds * 2, 30)


class AudioMixer:
    """Receives per-speaker PCM frames and emits mixed guild-level PCM frames."""

    def __init__(self, config: dict):
        self.config = config
        self.logger = logging.getLogger("kikisenbou.audio")
        self.buffers: dict[int, dict[int, Deque[bytes]]] = defaultdict(lambda: defaultdict(lambda: deque(maxlen=12)))
        self.tasks: dict[int, asyncio.Task] = {}
        self.producers: dict[int, InternalAudioProducer] = {}

    def start_guild(self, guild_id: int) -> None:
        if guild_id not in self.producers:
            self.producers[guild_id] = InternalAudioProducer(guild_id, self.config, self.logger)
        if guild_id not in self.tasks or self.tasks[guild_id].done():
            self.tasks[guild_id] = asyncio.create_task(self._mix_loop(guild_id), name=f"mixer-{guild_id}")

    async def stop_guild(self, guild_id: int) -> None:
        task = self.tasks.pop(guild_id, None)
        if task:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        producer = self.producers.pop(guild_id, None)
        if producer:
            await producer.stop()
        self.buffers.pop(guild_id, None)

    def add_pcm(self, guild_id: int, user_id: int, pcm: bytes) -> None:
        if not pcm:
            return
        # The receive extension usually provides exactly one 20 ms frame.
        # Split larger packets defensively so browser scheduling stays stable.
        for offset in range(0, len(pcm), PCM_FRAME_BYTES):
            frame = pcm[offset : offset + PCM_FRAME_BYTES]
            if len(frame) == PCM_FRAME_BYTES:
                self.buffers[guild_id][user_id].append(frame)

    async def _mix_loop(self, guild_id: int) -> None:
        self.logger.info("started PCM mixer for guild %s", guild_id)
        next_tick = asyncio.get_running_loop().time()
        while True:
            next_tick += 0.02
            await asyncio.sleep(max(0, next_tick - asyncio.get_running_loop().time()))
            user_buffers = self.buffers.get(guild_id, {})
            frames = []
            inactive_users = []
            for user_id, queue in user_buffers.items():
                if queue:
                    frames.append(queue.popleft())
                elif len(queue) == 0:
                    inactive_users.append(user_id)
            for user_id in inactive_users:
                if not user_buffers[user_id]:
                    user_buffers.pop(user_id, None)
            if not frames:
                continue

            self.producers[guild_id].send_nowait(mix_pcm_frames(frames))


class DiscordReceiveSink(voice_recv.AudioSink if voice_recv else object):
    def __init__(self, mixer: AudioMixer, guild_id: int, loop: asyncio.AbstractEventLoop):
        super().__init__()
        self.mixer = mixer
        self.guild_id = guild_id
        self.loop = loop

    def wants_opus(self) -> bool:
        return False

    def write(self, user: discord.User | discord.Member | None, data) -> None:
        user_id = int(user.id) if user else 0
        pcm = getattr(data, "pcm", None)
        if pcm:
            self.loop.call_soon_threadsafe(self.mixer.add_pcm, self.guild_id, user_id, pcm)

    def cleanup(self) -> None:
        pass


class KikisenBouBot(commands.Bot):
    def __init__(self, config: dict):
        intents = discord.Intents.default()
        intents.guilds = True
        intents.voice_states = True
        intents.members = True
        super().__init__(command_prefix="!", intents=intents)
        self.config = config
        self.mixer = AudioMixer(config)
        self.listen_group = app_commands.Group(name="listen", description="聞き専坊の操作")
        self._register_commands()

    async def setup_hook(self) -> None:
        self.tree.add_command(self.listen_group)
        await self.tree.sync()

    async def on_ready(self) -> None:
        assert self.user is not None
        logging.info("logged in as %s (%s)", self.user, self.user.id)
        for guild in self.guilds:
            upsert_bot_guild(guild)
        audit("起動", details=f"bot ready: {self.user}")

    async def on_guild_join(self, guild: discord.Guild) -> None:
        upsert_bot_guild(guild)
        audit("サーバー参加", guild.id, details=guild.name)

    async def on_guild_remove(self, guild: discord.Guild) -> None:
        db_execute("DELETE FROM bot_guilds WHERE guild_id = ?", (str(guild.id),))
        db_execute("DELETE FROM voice_connections WHERE guild_id = ?", (str(guild.id),))
        audit("サーバー退出", guild.id, details=guild.name)

    async def on_voice_state_update(self, member: discord.Member, before: discord.VoiceState, after: discord.VoiceState) -> None:
        if self.user and member.id == self.user.id and before.channel and not after.channel:
            db_execute("DELETE FROM voice_connections WHERE guild_id = ?", (str(member.guild.id),))
            await self.mixer.stop_guild(member.guild.id)
            audit("VC切断", member.guild.id, details=before.channel.name)

    def _register_commands(self) -> None:
        @self.listen_group.command(name="join", description="実行者がいるVCへ接続")
        @app_commands.default_permissions(manage_guild=True)
        async def join(interaction: discord.Interaction) -> None:
            await interaction.response.defer(ephemeral=True)
            if not interaction.guild or not isinstance(interaction.user, discord.Member):
                await interaction.followup.send("サーバー内で実行してください。", ephemeral=True)
                return
            if not interaction.user.voice or not interaction.user.voice.channel:
                await interaction.followup.send("先にボイスチャンネルへ参加してください。", ephemeral=True)
                return
            if voice_recv is None:
                await interaction.followup.send(f"音声受信拡張を読み込めません: {VOICE_RECV_IMPORT_ERROR}", ephemeral=True)
                return

            channel = interaction.user.voice.channel
            current = interaction.guild.voice_client
            if current and current.is_connected():
                if current.channel and current.channel.id == channel.id:
                    await interaction.followup.send(f"すでに {channel.name} に接続中です。", ephemeral=True)
                    return
                await current.disconnect(force=True)

            try:
                voice_client = await channel.connect(cls=voice_recv.VoiceRecvClient, self_deaf=False)
                sink = DiscordReceiveSink(self.mixer, interaction.guild.id, self.loop)
                voice_client.listen(sink)
                self.mixer.start_guild(interaction.guild.id)
                upsert_bot_guild(interaction.guild)
                db_execute(
                    """
                    INSERT INTO voice_connections(guild_id, channel_id, channel_name, connected_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(guild_id) DO UPDATE SET
                        channel_id = excluded.channel_id,
                        channel_name = excluded.channel_name,
                        connected_at = excluded.connected_at
                    """,
                    (str(interaction.guild.id), str(channel.id), channel.name, utc_now()),
                )
                audit("VC接続", interaction.guild.id, interaction.user.id, channel.name)
                await interaction.followup.send(f"{channel.name} に接続しました。", ephemeral=True)
            except Exception as exc:
                logging.exception("failed to join voice")
                audit("エラー", interaction.guild.id, interaction.user.id, f"join failed: {exc}")
                await interaction.followup.send(f"VC接続に失敗しました: {exc}", ephemeral=True)

        @self.listen_group.command(name="leave", description="VCから退出")
        @app_commands.default_permissions(manage_guild=True)
        async def leave(interaction: discord.Interaction) -> None:
            await interaction.response.defer(ephemeral=True)
            if not interaction.guild:
                await interaction.followup.send("サーバー内で実行してください。", ephemeral=True)
                return
            voice_client = interaction.guild.voice_client
            if not voice_client or not voice_client.is_connected():
                await interaction.followup.send("接続中のVCはありません。", ephemeral=True)
                return
            channel_name = voice_client.channel.name if voice_client.channel else "unknown"
            await voice_client.disconnect(force=True)
            await self.mixer.stop_guild(interaction.guild.id)
            db_execute("DELETE FROM voice_connections WHERE guild_id = ?", (str(interaction.guild.id),))
            audit("VC切断", interaction.guild.id, interaction.user.id, channel_name)
            await interaction.followup.send("VCから退出しました。", ephemeral=True)

        @self.listen_group.command(name="status", description="接続状態表示")
        async def status(interaction: discord.Interaction) -> None:
            if not interaction.guild:
                await interaction.response.send_message("サーバー内で実行してください。", ephemeral=True)
                return
            row = db_fetchone("SELECT channel_name FROM voice_connections WHERE guild_id = ?", (str(interaction.guild.id),))
            count_row = db_fetchone("SELECT count FROM listener_counts WHERE guild_id = ?", (str(interaction.guild.id),))
            channel_name = row["channel_name"] if row else "未接続"
            listener_count = count_row["count"] if count_row else 0
            await interaction.response.send_message(
                f"接続中VC:\n{channel_name}\n\nリスナー数:\n{listener_count}人",
                ephemeral=True,
            )

        @self.listen_group.command(name="allow", description="視聴許可ロールを追加")
        @app_commands.default_permissions(manage_guild=True)
        async def allow(interaction: discord.Interaction, role: discord.Role) -> None:
            if not interaction.guild:
                await interaction.response.send_message("サーバー内で実行してください。", ephemeral=True)
                return
            ensure_guild_settings(interaction.guild)
            db_execute(
                """
                INSERT INTO allowed_roles(guild_id, role_id, role_name, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(guild_id, role_id) DO UPDATE SET role_name = excluded.role_name, updated_at = excluded.updated_at
                """,
                (str(interaction.guild.id), str(role.id), role.name, utc_now()),
            )
            audit("権限追加", interaction.guild.id, interaction.user.id, f"allow {role.name}")
            await interaction.response.send_message(f"{role.name} を視聴許可ロールに追加しました。", ephemeral=True)

        @self.listen_group.command(name="deny", description="視聴拒否ロールを追加")
        @app_commands.default_permissions(manage_guild=True)
        async def deny(interaction: discord.Interaction, role: discord.Role) -> None:
            if not interaction.guild:
                await interaction.response.send_message("サーバー内で実行してください。", ephemeral=True)
                return
            ensure_guild_settings(interaction.guild)
            db_execute(
                """
                INSERT INTO denied_roles(guild_id, role_id, role_name, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(guild_id, role_id) DO UPDATE SET role_name = excluded.role_name, updated_at = excluded.updated_at
                """,
                (str(interaction.guild.id), str(role.id), role.name, utc_now()),
            )
            audit("権限追加", interaction.guild.id, interaction.user.id, f"deny {role.name}")
            await interaction.response.send_message(f"{role.name} を視聴拒否ロールに追加しました。", ephemeral=True)

        @self.listen_group.command(name="public", description="全員視聴可能にする")
        @app_commands.default_permissions(manage_guild=True)
        async def public(interaction: discord.Interaction) -> None:
            if not interaction.guild:
                await interaction.response.send_message("サーバー内で実行してください。", ephemeral=True)
                return
            ensure_guild_settings(interaction.guild)
            db_execute(
                "UPDATE guild_settings SET public = 1, updated_at = ? WHERE guild_id = ?",
                (utc_now(), str(interaction.guild.id)),
            )
            audit("公開設定", interaction.guild.id, interaction.user.id, "public")
            await interaction.response.send_message("全員視聴可能にしました。拒否ロールは引き続き優先されます。", ephemeral=True)

        @self.listen_group.command(name="private", description="許可ロールのみ視聴可能にする")
        @app_commands.default_permissions(manage_guild=True)
        async def private(interaction: discord.Interaction) -> None:
            if not interaction.guild:
                await interaction.response.send_message("サーバー内で実行してください。", ephemeral=True)
                return
            ensure_guild_settings(interaction.guild)
            db_execute(
                "UPDATE guild_settings SET public = 0, updated_at = ? WHERE guild_id = ?",
                (utc_now(), str(interaction.guild.id)),
            )
            audit("公開設定", interaction.guild.id, interaction.user.id, "private")
            await interaction.response.send_message("許可ロールのみ視聴可能にしました。", ephemeral=True)


def upsert_bot_guild(guild: discord.Guild) -> None:
    icon_url = str(guild.icon.url) if guild.icon else None
    db_execute(
        """
        INSERT INTO bot_guilds(guild_id, name, icon_url, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(guild_id) DO UPDATE SET
            name = excluded.name,
            icon_url = excluded.icon_url,
            updated_at = excluded.updated_at
        """,
        (str(guild.id), guild.name, icon_url, utc_now()),
    )
    ensure_guild_settings(guild)


def ensure_guild_settings(guild: discord.Guild) -> None:
    db_execute(
        """
        INSERT INTO guild_settings(guild_id, guild_name, public, updated_at)
        VALUES (?, ?, 1, ?)
        ON CONFLICT(guild_id) DO UPDATE SET guild_name = excluded.guild_name, updated_at = excluded.updated_at
        """,
        (str(guild.id), guild.name, utc_now()),
    )


def validate_config(config: dict) -> None:
    required = ["token", "client_id", "client_secret", "redirect_uri", "internal_audio_secret"]
    missing = [key for key in required if not config.get(key) or str(config.get(key)).startswith("change-this")]
    if "token" in missing:
        raise RuntimeError("config.json の token を設定してください。")
    if config.get("internal_audio_secret", "").startswith("change-this"):
        config["internal_audio_secret"] = secrets.token_urlsafe(32)
        logging.warning("internal_audio_secret is using an ephemeral value. Set it in config.json for production.")


def main() -> None:
    setup_logging()
    init_database()
    config = load_config()
    validate_config(config)
    if voice_recv is None:
        raise RuntimeError(f"discord-ext-voice-recv could not be imported: {VOICE_RECV_IMPORT_ERROR}")
    bot = KikisenBouBot(config)
    bot.run(config["token"], log_handler=None)


if __name__ == "__main__":
    main()
