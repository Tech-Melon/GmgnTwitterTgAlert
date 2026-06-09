# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development commands

### Environment setup
```bash
uv venv
uv pip install -r requirements.txt
uv run playwright install chromium
sudo uv run playwright install-deps chromium
```

### Run the monitor
Preferred entrypoint:
```bash
uv run python -m gmgn_twitter_monitor
```

Legacy-compatible entrypoint:
```bash
uv run python gmgn_twitter_monitor.py
```

### First-login bootstrap
For a fresh server, set `gmgn_twitter_monitor/config.py`:
- `FIRST_RUN_LOGIN = True`
- `AUTH_URL = ...` with a valid GMGN login URL

Then run:
```bash
uv run python -m gmgn_twitter_monitor
```
After browser state is written into `browser_data/`, switch `FIRST_RUN_LOGIN` back to `False`.

### External runtime dependency
This project expects a local SOCKS5 proxy at `socks5://127.0.0.1:40000` (`gmgn_twitter_monitor/config.py`). The deployment guide documents Cloudflare WARP setup for this.

### Tests / lint
There is currently no test suite or lint configuration checked into the repository.

## High-level architecture

This is a Playwright-based GMGN Twitter monitor that runs a persistent Chromium session behind Xvfb, listens to GMGN WebSocket traffic from the browser page, normalizes the upstream payloads, and republishes them to local outputs including a token-protected WebSocket broadcast server.

### Main runtime flow
- `gmgn_twitter_monitor/__main__.py` and `gmgn_twitter_monitor.py` are thin entrypoints that call `gmgn_twitter_monitor.app:main`.
- `gmgn_twitter_monitor/app.py` is the orchestration layer:
  - initializes logging
  - kills leftover Chromium processes on startup
  - starts Xvfb for a virtual display
  - launches the browser via `BrowserManager`
  - starts the downstream WebSocket distribution server
  - subscribes to Playwright page WebSocket events
  - parses and standardizes incoming GMGN frames
  - **IMPORTANT**: passes parsed items to `MessageDeduplicator` to handle `cp=0` (snapshot) vs `cp=1` (complete) duplication.
  - publishes normalized messages through a distributor hub
  - uses a watchdog to detect stalled upstream traffic and trigger page reload recovery

### Module responsibilities
- `gmgn_twitter_monitor/config.py`: all runtime configuration lives here, including URLs, browser state paths, proxy, watchdog timing, screenshot path, downstream WebSocket settings, Telegram Bot configs, and DeepSeek translator settings.
- `gmgn_twitter_monitor/app.py`: orchestrates the lifecycle and handles `MessageDeduplicator` (buffers `cp=0` snapshots for 0.8s (Feishu) ~ 5s (TG) to wait for `cp=1` complete payloads to avoid duplicate pushes and missing text).
- `gmgn_twitter_monitor/browser.py`: Playwright browser lifecycle and page interaction helpers. Handles persistent context launch, optional first-login flow, monitor-page navigation, popup dismissal, switching to the "Mine/我的" tab, screenshot capture, and watchdog-triggered recovery reloads.
- `gmgn_twitter_monitor/parser.py`: converts raw Socket.IO/WebSocket frames into parsed GMGN payloads and then into normalized message objects. Handles 7 actions: `tweet`, `repost`, `reply`, `quote`, `unfollow`, `delete_post`, `photo`.
- `gmgn_twitter_monitor/models.py`: dataclasses for the normalized event schema (`StandardizedMessage`, `Author`, `Content`, `Media`, `Reference`, `AvatarChange`).
- `gmgn_twitter_monitor/analyzer.py`: AI analysis module for specified handles. Uses DeepSeek to classify investment tracks (e.g., A-share, US stocks), extract specific stock names/codes, generate summaries, and translate texts in a single call.
- `gmgn_twitter_monitor/distributor.py`: fan-out layer for normalized messages. Includes:
  - `LoggingDistributor` for debug logging.
  - `WebSocketDistributor` for authenticated broadcast to connected clients.
  - `TelegramDistributor` for TG channel alerts (uses `sendMessage` with `link_preview_options` for text+preview, `sendMediaGroup` only for avatar comparisons. For photo-only tweets, uses raw image URLs directly; for video tweets, falls back to FxTwitter. Supports multi-action formatting, original post links, and asynchronous translation/analysis appending).
  - `WebhookDistributor` for HTTP POST integrations.
  - `DistributorHub` to publish to multiple distributors simultaneously.
- `gmgn_twitter_monitor/translator.py`: async DeepSeek translation module (low temperature, strict prompts) to translate English tweets to Chinese, skipping naturally Chinese text.
- `gmgn_twitter_monitor/watchdog.py`: timeout tracking for missing upstream messages.
- `gmgn_twitter_monitor/logging_setup.py`: Loguru configuration for console (colored) + rotating file logs (milliseconds precision).

## Key Architectural Decisions & Gotchas
1. **Action Types**: GMGN has standard (`tweet`, `repost`, `reply`, `quote`) and special actions (`unfollow` uses custom `f` fields; `delete_post` uses `stw` for original action; `photo` changes avatar). The system never filters out unknown actions but maps them as best as possible.
2. **Snapshot vs Complete Pass (`cp=0` vs `cp=1`)**: GMGN sends `cp=0` the instant a tweet launches (fast but often lacks `reference` or truncates text), followed by `cp=1` about 100ms later. We MUST deduplicate to avoid double-firing, and prefer `cp=1` to guarantee complete information.
3. **Telegram Rate Limiting**: The `TelegramDistributor` has an internal auto-backoff handler for `429 Too Many Requests`.
4. **Translation & Analysis Strategy**: DeepSeek translation and AI analysis are completely strictly asynchronous. Telegram sends the original tweet immediately to get a 0-delay response, captures the `message_id`, and then schedules an asynchronous bot `editMessageText/Caption` to append the translation and AI analysis (e.g., A-share track, stock code) 1-2 seconds later. This avoids delaying the original alert and avoids triggering double mobile notifications.
5. **System Time**: Since time difference tracing is critical for high-frequency crypto trading, timestamps leverage strict millisecond comparisons + system time synced via strict `ntpdate / chrony`.

## Operational notes
- Browser session state is persisted in `browser_data/`; this directory is essential for reusing authenticated GMGN sessions.
- Runtime artifacts are written in the repo root:
  - `twitter_monitor.log`
  - `monitor_running.png`
- The downstream WebSocket server does not buffer messages for offline clients; broadcasts are real-time only.
- Client authentication is performed by sending `{"token": "..."}` immediately after connecting to the downstream WebSocket server.

## Deployment reference
`DEPLOY_GUIDE.md` contains the repository’s actual server bootstrap process, including:
- `uv` environment setup
- Playwright browser/dependency installation
- Cloudflare WARP proxy setup
- first-run login procedure
- downstream WebSocket client authentication expectations
