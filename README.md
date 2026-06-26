# GmgnTwitterClaw 🦅

**基于 GMGN.ai 的实时 Twitter KOL 监控引擎**，通过浏览器自动化拦截 WebSocket 数据流，将推特动态标准化后实时分发至 Telegram 频道、WebSocket 广播和 Webhook 三大通道。

### ✨ 核心特性

- **全动作捕获**：覆盖发推、转推、回复、引用、关注/取关、删帖、换头像、改昵称、改简介、置顶/取消置顶共 12 种推特行为
- **智能图文预览**：纯图推文优先使用原图直链确保 100% 准确预览，含视频推文通过 FxTwitter 实现内嵌播放，关注/取关等主页类动作通过 vxTwitter 渲染为用户名片
- **DeepSeek 实时翻译与 AI 分析**：非阻塞异步翻译，推送完成后自动追加中文译文。支持对指定博主进行投资赛道分析（如 A 股股票名称代码提取）与智能摘要
- **多频道智能路由**：按推特 Handle 分组路由到不同 Telegram 频道，同一博主可同时推送至多个频道
- **双轨数据捕获**：WebSocket 实时监听 + HTTP Polling 降级拦截，重连间隙零丢失
- **去重引擎**：基于 `internal_id` 的快照/完整版智能去重，0.8s (飞书) ~ 5s (TG) 窗口内自动选优
- **三通道扇出分发**：Telegram、WebSocket、Webhook 并行推送，任一通道故障不影响其余
- **12 小时自动刷新**：systemd `RuntimeMaxSec` 定时重启，防止长时间运行导致浏览器内存泄漏

---

## 💡 FAQ：首次授权与账号准备必读

在开始部署之前，你需要了解 GMGN 的底层授权机制：

- **GMGN 官网**: [https://gmgn.ai/r/1RFSf1fc?chain=bsc](https://gmgn.ai/r/1RFSf1fc?chain=bsc)
- **获取授权链接**: 首次使用时，你需要在 Telegram 中找到 GMGN Bot 提供的专属登录/授权链接（右键复制链接），并将其填入到本项目的配置文件 `config.py` 中的 `AUTH_URL` 里（详见下文第 5 步）。
- **⚠️ 账号风控注意**: 强烈建议使用一个 **空 TG / 小号** 来扫码授权隔离风险。但请注意 GMGN 官方规则：对于没有任何交易量的纯空号，GMGN 会限制其关注小众博主（需要有交易量才能解锁）。相关限制规则请自行了解。
- **📹 推特演示说明**: [点此查看视频说明演示](https://x.com/0xTechMelon/status/2049114161498726883?s=20)

---

## 📂 项目结构

```
GmgnTwitterClaw/
├── gmgn_twitter_monitor/          # 核心包
│   ├── __init__.py
│   ├── __main__.py                # python -m 入口
│   ├── app.py                     # 主循环：浏览器启动 + WS/Polling 双轨拦截 + 去重引擎
│   ├── browser.py                 # Playwright 浏览器生命周期管理（启动/登录/截图/恢复）
│   ├── config.py                  # 配置中心：从 .env 读取环境变量 + 路由分组解析
│   ├── distributor.py             # 五大分发器：Logging / Telegram / Feishu / WebSocket / Webhook
│   ├── analyzer.py                # DeepSeek AI 深度分析（赛道分类/摘要/A股提取/翻译）
│   ├── logging_setup.py           # loguru 日志格式化
│   ├── models.py                  # StandardizedMessage 数据模型（dataclass）
│   ├── parser.py                  # 原始 WS 数据 → 标准化 JSON 转换器
│   ├── translator.py              # DeepSeek 异步翻译引擎（纯翻译链路）
│   └── watchdog.py                # 看门狗：超时无数据自动刷新页面
├── gmgn_twitter_monitor.py        # 兼容入口（等价于 python -m gmgn_twitter_monitor）
├── ctl.py                         # 交互式运维控制台（服务管理/日志查看/截图等）
├── gmgn-twitter-monitor.service   # systemd 服务单元文件
├── .env.example                   # 环境变量模板
├── requirements.txt               # Python 依赖清单
└── browser_data/                  # 浏览器登录态持久化目录（自动生成，勿删）
```

---

## 🚀 部署指南

### 1. 安装基础依赖和 Python 工具 `uv`

`uv` 是比原生的 `pip` 快几百倍的现代化 Python 环境管理工具，本程序使用它来隔离虚拟环境。

```bash
# 安装 uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# 使 uv 在当前终端立即生效
source $HOME/.local/bin/env

# 进入项目目录 (假设项目已经 clone 到了服务器的主目录下)
cd ~/workspace/GmgnTwitterClaw

# 创建独立虚拟环境并安装 requirements.txt 声明的 Python 库
uv venv
uv pip install -r requirements.txt
```

### 2. 安装 Playwright 内核与 Linux 缺失的底层桌面包

因为程序的核心本质是操纵真的浏览器进行抓取，所以我们需要安装浏览器内核及在 Linux 裸机运行虚拟桌面所必须的 C 语言底层库。

```bash
# 下载 Chromium 浏览器内核
uv run playwright install chromium

# 一键安装 Linux 运行 Chrome 所必需的全套底层依赖 (例如 libatk, libgbm, libdrm 等，会自动调用 apt)
sudo uv run playwright install-deps chromium
```

### 3. 设置 Cloudflare WARP 代理（突破 IP 盾防御核心）

如果不配置这一步，机房 VPS 的 IP 访问 gmgn.ai 会被 Cloudflare 100% 出现盾阻断（"Sorry, you have been blocked"），甚至连验证码都不会给。通过挂载官方 WARP 服务，并将其转化为本地 Proxy，脚本将可以获得家庭宽带级别的隐身穿透能力。

```bash
# 1. 注入 Cloudflare 的 GPG 密钥并添加官方 APT 源 (仅限 Ubuntu/Debian 系)
curl -fsSL https://pkg.cloudflareclient.com/pubkey.gpg | sudo gpg --yes --dearmor --output /usr/share/keyrings/cloudflare-warp-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/cloudflare-warp-archive-keyring.gpg] https://pkg.cloudflareclient.com/ $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/cloudflare-client.list

# 2. 安装并注册 Warp 客户端
sudo apt-get update
sudo apt-get install -y cloudflare-warp

warp-cli registration new
# (中途如果遇到隐私提示，输入 Y 回车同意)

# 3. 将 Warp 设定为本地 Socks5 代理模式，把端口绑定到 40000
warp-cli mode proxy
warp-cli proxy port 40000

# 4. 连接
warp-cli connect

# 5. (可选) 测试代理是否通顺
curl -x socks5://127.0.0.1:40000 https://cloudflare.com/cdn-cgi/trace
# 如果输出的信息中有 warp=on 字眼，说明穿透成功。
```

### 4. 配置环境变量

所有敏感信息通过 `.env` 文件管理，**严禁提交到 Git**（已在 `.gitignore` 中屏蔽）。

```bash
# 复制模板并填入真实值
cp .env.example .env
nano .env
```

完整的环境变量说明：

| 变量名 | 必填 | 说明 |
|--------|:----:|------|
| `WS_TOKEN` | ✅ | WebSocket 鉴权 Token，建议用 `python3 -c "import secrets; print(secrets.token_urlsafe(32))"` 生成 |
| `TG_BOT_TOKEN` | ✅ | Telegram Bot API Token |
| `FEISHU_APP_ID` | ❌ | 飞书企业自建应用 App ID (用于卡片原图提取与上传) |
| `FEISHU_APP_SECRET` | ❌ | 飞书企业自建应用 App Secret |
| `TG_ENABLE_DEFAULT` | ❌ | 默认频道推送开关（`True`/`False`），默认 `False` |
| `TG_CHANNEL_ID` | ❌ | 默认/兜底频道 ID（未命中任何路由分组时使用） |
| `TG_ENABLE_<GROUP>` | ❌ | 分组推送开关，如 `TG_ENABLE_BINANCE=True` |
| `TG_CHANNEL_ID_<GROUP>` | ❌ | 分组目标频道 ID，如 `TG_CHANNEL_ID_BINANCE=-100xxx` |
| `TG_ROUTING_<GROUP>` | ❌ | 分组内的推特 Handle 列表（逗号分隔），如 `TG_ROUTING_BINANCE=cz_binance` |
| `TG_TRACK_FILTER_<GROUP>` | ❌ | TG 分组赛道过滤关键词（逗号分隔），如 `A股`；配置后仅推送 AI 赛道命中的内容 |
| `FEISHU_TRACK_FILTER_<GROUP>` | ❌ | 飞书分组赛道过滤关键词（逗号分隔），如 `A股`；配置后仅推送 AI 赛道命中的内容 |
| `TG_FILTER_HANDLES` | ❌ | 附加白名单（一般留空，路由分组的 Handle 会自动合并） |
| `BINANCE_SQUARE_HANDLES` | ❌ | 币安广场等非 Twitter 账号的 ID 列表（逗号分隔），由于 FxTwitter 无法解析，会自动提取原图渲染为大图 |
| `DEEPSEEK_API_KEY` | ❌ | DeepSeek API Key，留空则跳过翻译 |
| `AI_ANALYZE_HANDLES`| ❌ | 启用深度 AI 分析（赛道分类、摘要、A股提取）的 Handle 列表（逗号分隔） |
| `SUMMARY_ENABLE` | ❌ | 定时频道总结开关（`True`/`False`），默认 `False` |
| `SUMMARY_TIMES` | ❌ | 每日总结时间，逗号分隔，如 `07:30,20:00` |
| `SUMMARY_GROUPS` | ❌ | 要总结的路由分组后缀，如 `BINANCE`；默认读取该组 TG 频道作为数据源 |
| `SUMMARY_DB_PATH` | ❌ | SQLite 数据库路径，默认 `twitter_monitor.db` |
| `SUMMARY_AI_TIMEOUT_SECONDS` | ❌ | 频道总结 AI 请求超时时间，默认 `180` 秒 |
| `SUMMARY_TWEET_TEXT_LIMIT` | ❌ | 每条推文喂给总结 AI 的正文+关联原文总长度限制，默认 `500` 字 |
| `WEBHOOK_URL` | ❌ | Webhook 推送目标 URL，留空则禁用 |
| `WEBHOOK_SECRET` | ❌ | HMAC-SHA256 签名密钥 |

#### 多频道路由分组规则

每个路由分组需要定义**一致的后缀**（例如后缀为 `BINANCE`）：

```env
# TG 频道配置
TG_ENABLE_BINANCE=True                              # 开关
TG_CHANNEL_ID_BINANCE=-1001234567891                 # 目标频道
TG_ROUTING_BINANCE=cz_binance,heyibinance            # 博主列表
TG_TRACK_FILTER_BINANCE=                             # 可选：如 A股

# 飞书群聊配置 (支持原生提取大图渲染)
FEISHU_ENABLE_BINANCE=True                           # 飞书开关
FEISHU_WEBHOOK_BINANCE=https://open.feishu.cn/...    # 飞书群自定义机器人 Webhook
FEISHU_SECRET_BINANCE=your-secret                    # 飞书安全签名密钥
FEISHU_TRACK_FILTER_BINANCE=                         # 可选：如 A股
```

- 同一个 Handle 可以出现在多个分组中，会同时推送到所有匹配的频道
- `TG_FILTER_HANDLES` 无需手动填写路由分组里的 Handle，系统会自动合并
- 赛道过滤依赖 `AI_ANALYZE_HANDLES` 产出 `category`；如果某个分组配置了 `TG_TRACK_FILTER_<GROUP>` 或 `FEISHU_TRACK_FILTER_<GROUP>`，请把该分组的 Handle 加入 `AI_ANALYZE_HANDLES`

#### 定时频道总结

系统会将标准化推文和成功投递记录保存到 SQLite，用于后续按频道生成摘要。以下配置会每天 07:30 和 20:00 总结 `BINANCE` 组的 TG 频道内容，并将同一份 AI 摘要推送到该组 TG 与飞书：

```env
SUMMARY_ENABLE=True
SUMMARY_TIMEZONE=Asia/Shanghai
SUMMARY_TIMES=07:30,20:00
SUMMARY_GROUPS=BINANCE
SUMMARY_LABEL_BINANCE=Binance
SUMMARY_AI_TIMEOUT_SECONDS=180
SUMMARY_TWEET_TEXT_LIMIT=500
```

默认规则：
- 数据源：`TG_CHANNEL_ID_<GROUP>`，例如 `TG_CHANNEL_ID_BINANCE`
- TG 目标：同一个 `TG_CHANNEL_ID_<GROUP>`
- 飞书目标：同组 `FEISHU_WEBHOOK_<GROUP>` / `FEISHU_SECRET_<GROUP>`
- 时间窗口：07:30 总结前一晚 20:00 到当天 07:30；20:00 总结当天 07:30 到 20:00
- AI 输入：每条记录包含推文正文；如果是回复、引用、转推、删帖等动作，也会携带关联原文、关联作者和关联链接

#### 非推特账号特殊处理 (如币安广场)

对于非推特源的账号（如币安广场的 `cz`、`heyi`），由于它们不是真实的推特用户名，依赖推特链接（`fxtwitter.com`）解析图片会失败。
你可以在 `.env` 中配置 `BINANCE_SQUARE_HANDLES=cz,heyi`。当系统检测到这些账号时，会跳过推特链接拼接，直接从数据源的 JSON 中抽取真实的图片直链（Raw Image URL）交给 Telegram 渲染。这样既保证了能看到大图预览，又维持了 4096 的文本容量。

### 飞书群组配置与避坑指南

为了实现类似 Telegram Channel 的**纯净只读情报群**体验，并且避开企业内网的权限墙，请严格遵循以下配置指南：

1. **创建外部群聊（强烈建议）**：在飞书创建群聊时，**务必选择创建“外部群聊”**。如果你创建的是企业内部群，自定义机器人可能会受限于企业组织架构的安全策略而无法正常发言或被外部人员查看。
2. **添加机器人**：在飞书群设置的“群机器人”中添加“自定义机器人”，获取 Webhook URL 和安全签名 Secret 填入 `.env` 中对应的分组。
3. **开启全员禁言**：进入群设置 -> 群管理 -> 谁可以在此群发言，将其修改为 **“仅群主和群管理员”**。
4. **安全与纯净加固**（可选）：将群管理中的“谁可以@所有人”、“发起视频会议”、“Pin”、“编辑群信息”等也全部改为 **“仅群主和群管理员”**。

**原理解析：**
经过实测，由群主（或管理员）创建的**自定义 Webhook 机器人**默认继承高级权限，能够无视“仅群主和群管理员可发言”的全局禁言锁，继续向群内顺畅推送情报，而普通群员则完全无法打字发言。

**避坑：原生大图与服务器网络配置**
为了弥补飞书不支持直接渲染外部图片链接的缺陷，本系统在底层设计了独特的**双线程并发机制**：
1. **借尸还魂**：利用 `.env` 中的 `FEISHU_APP_ID` 和 `FEISHU_APP_SECRET`（该应用需在飞书开发者后台开通 `im:resource:upload` 获取与上传图片或文件权限并发布新版本），将 Twitter 的外网原图（包括视频的封面）极速下载并以应用的身份上传给飞书，获取合法内网 `img_key`。
2. **瞒天过海**：与此同时，并发调用 DeepSeek 获取中文翻译。当两路异步任务瞬间完成后，系统会将带有内网图片的精美卡片，依然使用能穿透禁言的 Webhook 机器人发送。

> **⚠️ 网络代理避坑警告 (非常重要)**: 
> 飞书上传大图的前提是：系统能够成功下载推特的原图（pbs.twimg.com）。
> - **如果你的服务器在海外（如 AWS 东京/美国节点）**：程序会自动直连，千万不要在环境变量中乱配 `http_proxy` 或在系统中开启没用的本地翻墙代理端口，否则会导致下载图片请求超时失败，最终导致飞书卡片渲染不出任何图片和视频封面（变成干瘪的纯文字或空卡片）。
> - **如果你的服务器在国内**：必须正确配置并导出全局的 `http_proxy` / `https_proxy` 环境变量（如 `export http_proxy=http://127.0.0.1:7890`），程序会自动读取并走代理通道下载原图。



### 5. 首次运行与授权

当你的新服务器第一次打算跑脚本时，你需要让程序获得你具体的身份登录状态。

1. 修改 `gmgn_twitter_monitor/config.py`，将 `FIRST_RUN_LOGIN` 改为 `True`。
2. 在同一个文件里，将 `AUTH_URL` 赋值为你最新的（未过期）授权链接 `https://gmgn.ai/tglogin?...&id=...`。
3. 执行监控脚本：

```bash
uv run python -m gmgn_twitter_monitor
```

> 该程序会自动利用 `xvfbwrapper` 在后台开启隐形的虚拟桌面，使用有头模式（突破 CF 封锁）访问该授权页面，并等候 8 秒将登录凭证序列化写入当前目录下的 `./browser_data` 文件夹。此后它会自动关闭可能会弹出的弹窗，切换到【我的】标签进行监听。
>
> 兼容方式仍然保留：如果你已有旧脚本依赖，也可以继续执行 `uv run python gmgn_twitter_monitor.py`。

**【重要】**
一旦第一次看到日志输出获取成功，为了加速以后重启的流程，建议你回去把 `gmgn_twitter_monitor/config.py` 里的 `FIRST_RUN_LOGIN` 重新改回 `False`。只要 `browser_data` 文件夹不被删，服务器就可以在接下来的很长一段时间内复用该状态免密直接连接。

### 6. systemd 服务自动守护

```bash
# 将 service 文件链接到 systemd 目录
sudo ln -sf $(pwd)/gmgn-twitter-monitor.service /etc/systemd/system/
sudo systemctl daemon-reload

# 开机自启 + 立即启动
sudo systemctl enable gmgn-twitter-monitor.service
sudo systemctl start gmgn-twitter-monitor.service

# 查看运行状态
sudo systemctl status gmgn-twitter-monitor.service

# 查看实时日志
sudo journalctl -u gmgn-twitter-monitor -f
```

**服务守护策略：**
- 崩溃后 **10 秒**自动重启（`RestartSec=10`）
- 每 **12 小时**自动重启一次（`RuntimeMaxSec=43200`），防止浏览器长时间运行导致内存泄漏或 WebSocket 老化
- 启动日志会打印当前启动时间和下次预计重启时间

### 7. Nginx + TLS 配置（WSS）

如果在新服务器上需要重新配置 WSS：

```bash
# 安装 Nginx 和 Certbot
sudo apt-get install -y nginx certbot python3-certbot-nginx

# 创建站点配置（参考 /etc/nginx/sites-available/your-domain.com）
# 启用站点
sudo ln -sf /etc/nginx/sites-available/your-domain.com /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default

# 申请 SSL 证书（自动注入 Nginx 配置）
sudo certbot --nginx -d your-domain.com --non-interactive --agree-tos --email your@email.com --redirect

# 测试配置
sudo nginx -t && sudo systemctl reload nginx
```

证书由 Certbot 的 systemd timer 自动续期，无需手动干预。

---

## 🏗️ 系统架构

### 四通道分发架构

```
   gmgn.ai WebSocket / HTTP Polling
              │
              ▼
   ┌─────────────────────┐
   │  Parser 标准化 JSON   │ ← 12 种 Twitter 动作全解析
   └──────────┬──────────┘
              │
    MessageDeduplicator      ← 500ms 窗口智能去重
              │
    DistributorHub.publish()
     ┌────────┼────────┬──────────┐
     │        │        │          │
┌────▼───┐ ┌──▼────┐ ┌─▼──────┐ ┌─▼───────┐
│Logging │ │  TG   │ │Webhook │ │  WSS    │
│ 日志   │ │ 频道  │ │HTTP POST│ │ 广播   │
│        │ │ 多频道│ │HMAC签名│ │Token鉴权│
│        │ │ 路由  │ │        │ │         │
└────────┘ └──┬────┘ └────────┘ └─────────┘
              │
        DeepSeek 异步翻译
       (推送后追加译文)
```

### Telegram 推送特性

- **智能图片预览**：纯图推文直接使用 WebSocket 底层提取的真实图片直链 (`pbs.twimg.com`) 作为 `link_preview_options.url`，100% 准确展示原图（多图时展示首图）；含视频推文自动降级到 `fxtwitter.com` 实现内嵌播放
- **vxTwitter 主页名片**：关注、取关、改昵称、改简介等动作自动通过 `vxtwitter.com` 渲染为用户头像+简介的名片卡
- **原帖直达链接**：卡片底部统一附带 `x.com` 原帖/主页链接，支持点击直达
- **换头像对比图**：头像变更动作保留原生 `sendMediaGroup`，展示新旧头像的并列对比
- **DeepSeek 实时翻译与深度分析**：推文发送后异步调用 DeepSeek API 翻译。对指定的博主额外进行投资赛道分析、智能摘要及 A 股个股提取，完成后自动编辑原消息追加分析与译文，主推送流程零阻塞
- **429 退避重试**：遇到 Telegram Rate Limit 时自动等待并重试

---

## 📡 推送数据格式（标准化 JSON）

每条消息对应一个 Twitter 动作，三大通道（Telegram/WebSocket/Webhook）使用完全一致的 JSON 结构：

```json
{
  "action": "tweet",
  "original_action": null,
  "tweet_id": "1234567890123456789",
  "internal_id": "abc123def456",
  "timestamp": 1712300000,
  "author": {
    "handle": "cz_binance",
    "name": "CZ 🔶 BNB",
    "avatar": "https://pbs.twimg.com/profile_images/xxx/photo.jpg",
    "followers": 12800000,
    "tags": ["Smart_kol"]
  },
  "content": {
    "text": "推文正文内容...",
    "media": [
      { "type": "photo", "url": "https://pbs.twimg.com/media/xxx.jpg" }
    ]
  },
  "reference": {
    "tweet_id": "9876543210",
    "author_handle": "elonmusk",
    "author_name": "Elon Musk",
    "author_avatar": "https://pbs.twimg.com/...",
    "author_followers": 239600000,
    "text": "被引用/回复/转推的原文...",
    "media": [],
    "type": "quoted"
  },
  "unfollow_target": null,
  "avatar_change": null,
  "bio_change": null
}
```

### `action` 字段枚举（共 12 种）

| 值 | 含义 | 说明 |
|----|------|------|
| `tweet` | 发布新推文 | 原创推文，`content.text` 有正文 |
| `repost` | 转推（RT） | `reference` 包含被转推的原推信息 |
| `reply` | 回复 | `reference` 包含被回复的原推信息 |
| `quote` | 引用推文 | `content.text` 有引用评论，`reference` 有原推 |
| `follow` | 新增关注 | `unfollow_target` 包含被关注者信息 |
| `unfollow` | 取消关注 | `unfollow_target` 包含被取关者信息 |
| `delete_post` | 删除推文 | `original_action` 记录被删推文的原始类型 |
| `photo` | 更换头像 | `avatar_change` 包含 `before`/`after` 头像 URL |
| `description` | 简介更新 | `bio_change` 包含 `before`/`after` 简介文本 |
| `name` | 更改昵称 | 作者信息中包含新昵称 |
| `pin` | 置顶推文 | `tweet_id` 包含被置顶的推文 ID |
| `unpin` | 取消置顶 | `tweet_id` 包含被取消置顶的推文 ID |

### 条件字段说明

| 字段 | 出现条件 |
|------|----------|
| `reference` | `repost` / `reply` / `quote` / `delete_post` |
| `unfollow_target` | `follow` / `unfollow` |
| `avatar_change` | `photo` |
| `bio_change` | `description` |
| `original_action` | `delete_post` |

---

## 🔌 WSS 客户端接入示例

```python
import asyncio
import json

import websockets
from loguru import logger

WS_URL = "wss://your-domain.com/ws"
TOKEN  = "your-ws-token"  # 与 .env 中 WS_TOKEN 一致

async def handle_signal(msg: dict):
    action = msg["action"]
    handle = msg["author"]["handle"]
    text   = msg["content"]["text"] or ""
    logger.info(f"[{action}] @{handle}: {text[:80]}")

async def listen_forever():
    while True:
        try:
            async with websockets.connect(WS_URL) as ws:
                await ws.send(json.dumps({"token": TOKEN}))
                resp = json.loads(await ws.recv())
                assert resp.get("status") == "connected", f"鉴权失败: {resp}"
                logger.success("✅ 已连接，开始接收信号...")
                async for raw in ws:
                    await handle_signal(json.loads(raw))
        except (websockets.exceptions.ConnectionClosed,
                OSError, asyncio.TimeoutError) as e:
            logger.warning(f"⚠️ 连接断开: {e}，5秒后重连...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(listen_forever())
```

### Webhook 签名验证示例

```python
import hmac
import hashlib

def verify_signature(body: bytes, secret: str, received_signature: str) -> bool:
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, received_signature)

# 在你的接收端：
# signature = request.headers.get("X-Signature-SHA256")
# is_valid = verify_signature(request.body, "your-secret", signature)
```

---

## 📋 配置速查

| 配置项 | 值 |
|--------|-----|
| WSS 地址 | `wss://your-domain.com/ws` |
| 鉴权 Token | `.env → WS_TOKEN` |
| TG 推送 | `.env → TG_BOT_TOKEN` + 路由分组变量 |
| 翻译 | `.env → DEEPSEEK_API_KEY` |
| Webhook | `.env → WEBHOOK_URL` |
| 心跳间隔 | 30 秒 |
| 看门狗超时 | 120 秒（无消息自动刷新页面） |
| 服务自动重启 | 每 12 小时（`RuntimeMaxSec=43200`） |
| 监控目标 | `gmgn.ai/follow?target=xTracker&chain=bsc` |
| WARP 代理 | `socks5://127.0.0.1:40000` |
| SSL 证书 | Let's Encrypt，Certbot 自动续期 |

---

## 📜 License

MIT
