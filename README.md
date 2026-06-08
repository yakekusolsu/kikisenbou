# 聞き専坊 (KikisenBou)

Discord Bot が接続しているボイスチャンネルの音声を受信し、Discord OAuth2 でログインしたユーザーへブラウザの WebSocket 経由でリアルタイム配信するシステムです。

## 構成

```text
KikisenBou/
├─ bot.py
├─ web.py
├─ config.json
├─ database.db
├─ templates/
│  ├─ login.html
│  ├─ dashboard.html
│  └─ guild.html
├─ static/
│  ├─ style.css
│  └─ script.js
├─ audio/
├─ logs/
└─ requirements.txt
```

## 初回起動手順

1. Python 3.12 以上を用意します。

2. 依存関係をインストールします。

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Linux の場合は `libsodium` と Opus 関連ライブラリが必要になることがあります。Windows では通常 PyNaCl の wheel で動作します。

3. Discord Developer Portal でアプリケーションを作成します。

4. Bot を作成し、Bot Token を `config.json` の `token` に設定します。

5. Bot の Privileged Gateway Intents で `SERVER MEMBERS INTENT` を有効にします。ロール権限判定でメンバー情報を取得するために使います。

6. OAuth2 の Redirects に次を追加します。

```text
http://127.0.0.1:8000/callback
```

7. `config.json` を設定します。

```json
{
  "token": "BOT_TOKEN",
  "client_id": "DISCORD_APPLICATION_CLIENT_ID",
  "client_secret": "DISCORD_APPLICATION_CLIENT_SECRET",
  "redirect_uri": "http://127.0.0.1:8000/callback",
  "host": "0.0.0.0",
  "port": 8000,
  "session_secret": "十分に長いランダム文字列",
  "internal_audio_secret": "十分に長いランダム文字列",
  "internal_ws_url": "ws://127.0.0.1:8000/internal/audio/{guild_id}",
  "rate_limit_per_minute": 120
}
```

`session_secret` と `internal_audio_secret` は本番運用前に必ず変更してください。

8. Bot をサーバーに招待します。

OAuth2 URL Generator で `bot` と `applications.commands` を選択し、Bot Permissions は最低限以下を付与します。

- Connect
- View Channel
- Use Slash Commands

9. Web と Bot を別ターミナルで起動します。

```bash
python web.py
python bot.py
```

10. ブラウザで開きます。

```text
http://127.0.0.1:8000
```

## 使い方

Discord の VC に参加した状態で、サーバー内で次を実行します。

```text
/listen join
```

Bot が VC に参加すると、Web ダッシュボードのサーバー詳細に接続中 VC が表示されます。VC 名をクリックし、`▶ 聞き専開始` を押すと再生が始まります。

退出:

```text
/listen leave
```

状態確認:

```text
/listen status
```

## 権限コマンド

全員視聴可能:

```text
/listen public
```

許可ロールのみ:

```text
/listen private
```

許可ロール追加:

```text
/listen allow @Role
```

拒否ロール追加:

```text
/listen deny @Role
```

拒否ロールは公開設定より優先されます。

## 音声配信の流れ

```text
Discord Voice
↓
Opus
↓ discord-ext-voice-recv が PCM へデコード
PCM 48kHz stereo s16le
↓ bot.py 内部 WebSocket producer
web.py AudioHub
↓ WebSocket
Browser Web Audio API
```

Bot は受信したユーザー別 PCM フレームを 20ms 単位でミックスし、Web 側の内部 WebSocket `/internal/audio/{guild_id}` に送ります。Web はログイン済みかつ権限確認済みのブラウザ WebSocket `/ws/listen/{guild_id}/{channel_id}` へバイナリ PCM を中継します。

## セキュリティ

実装済み:

- Discord OAuth2 `identify guilds`
- OAuth state による CSRF 対策
- POST フォーム CSRF トークン
- Starlette SessionMiddleware によるセッション管理
- Jinja2 自動エスケープによる XSS 対策
- SQLite パラメータクエリによる SQL Injection 対策
- IP ベース Rate Limit
- 内部音声 WebSocket の shared secret 認証
- 監査ログを SQLite と `logs/kikisenbou.log` に記録

本番運用時は HTTPS 終端を置き、`redirect_uri` を HTTPS に変更してください。HTTPS の場合、セッション Cookie は Secure 属性になります。

## ログ

次のイベントを記録します。

- ログイン
- ログアウト
- 視聴開始
- 視聴終了
- VC接続
- VC切断
- 権限変更
- エラー

ログファイルは `logs/kikisenbou.log`、監査ログは SQLite の `audit_logs` テーブルです。

## 注意点

標準の `discord.py` には公式の音声受信 API がないため、この実装では `discord.py` の音声クライアント拡張である `discord-ext-voice-recv` を使用します。Discord 側の仕様変更やライブラリ更新により、音声受信部分は追従が必要になる可能性があります。
