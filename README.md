# Netcup Control REST API

基于 Flask 的高性能 Netcup 流量监控、限速联动处理与磁盘告警系统。

当 Netcup 服务器流量超限时，自动暂停/删除 qBittorrent 任务或直接停止远程系统服务；支持每日上传/下载精准统计、qB 磁盘剩余空间三阶梯智能告警、Webhook 秒级即时联动以及 Telegram 交互式机器人。

---

## 🌟 核心特性

- 🚀 **极致性能与低负载**：优化 Netcup SCP API 调用逻辑，单次 GET 聚合获取服务器详情与流量状态，**请求量减少 66.7%**，数据采集在 0.5 秒内极速完成，彻底避免触发官方频控。
- 📊 **每日流量精准统计**：自动统计每台服务器每日跑量（**今日上传 / 今日下载 / 今日总计**），自动跨日归档与持久化（`daily_traffic.json`），并在 Web 面板与 Telegram 中清晰展示。
- 💽 **qB 磁盘空间 3 阶梯智能告警**：自动监控 qBittorrent 磁盘剩余空间，精准识别 **≤ 5GB（黄色告警）**、**≤ 1GB（橙色严重）**、**0GB（红色致命已满）** 三级危险状态；支持恶化立即报警、同级别冷却防刷屏，以及**磁盘清理后的空间恢复自动通知**。
- ⚡ **Webhook 秒级即时联动**：Netcup SCP 官方触发超限 Webhook 回调时，后台立即异步触发强制检查与限速联动，告别被动等待轮询，**实现秒级处置**。
- 🔌 **6 种灵活限速策略**：支持暂停所有种子、删除下载中种子、暂停恢复、仅处理下载中做种保持，以及**直接停止远程/本机 systemd 服务（`service` 策略）**。
- 🎛️ **Vertex (VT) 控制开关**：支持 `vertex.manage_downloader` 配置开关，可灵活选择是否在限速时联动暂停 VT 中的下载器，或仅作为只读运行报告。
- 🤖 **Telegram 交互式 Bot**：
  - `/status`：查看所有服务器状态、剩余磁盘、今日跑量与累计总流量
  - `/check`：立即强制连接 Netcup 刷新并返回最新状态
  - `/daily`：查看今日全服流量简报（每晚 23:55 自动定时推送）
  - `/report`：查看 Vertex 今日运行数据报告
  - `/help`：查看所有可用指令
  - **白名单安全鉴权**：严格校验 `chat_id`，防止外部陌生人窥探服务器信息。
- 🔄 **Token 自动续期落盘**：Netcup Token 轮转刷新成功后，自动持久化写回 `config.json` 并安全 `fsync`，解决长期运行后容器重启导致 Token 失效需重新扫码的痛点。
- 🛡️ **日志滚动切割防爆盘**：内置 `RotatingFileHandler`（单文件 10MB，自动保留 3 个历史备份），防止日志无限膨胀占满云服务器磁盘。
- 🎴 **响应式 Web 监控面板**：纯前端内联设计，支持卡片、列表、紧凑、表格多视图切换，支持按今日流量、累计流量等多种维度排序。

---

## 目录

- [部署方式一：Docker 部署（推荐）](#部署方式一docker-部署推荐)
- [部署方式二：直接 Python 运行](#部署方式二直接-python-运行)
- [获取 Netcup API Token（核心步骤）](#获取-netcup-api-token核心步骤)
- [配置文件完整说明](#配置文件完整说明)
- [策略对比表与 service 配置](#策略对比表与-service-配置)
- [Telegram Bot 命令指南](#telegram-bot-命令指南)
- [Webhook 配置](#webhook-配置)
- [常见问题与排坑指南 (FAQ)](#常见问题与排坑指南-faq)

---

## 部署方式一：Docker 部署（推荐）

### 1. 安装 Docker 与 Compose

```bash
# Debian / Ubuntu 一键安装
curl -fsSL https://get.docker.com | sh
apt install -y docker-compose-plugin
```

### 2. 克隆项目

```bash
git clone https://github.com/WonderMaker123/netcup-control-restapi.git
cd netcup-control-restapi
```

### 3. 获取 Netcup API Token

> 详细图文操作见下文 [获取 Netcup API Token（核心步骤）](#获取-netcup-api-token核心步骤)。

```bash
pip install requests
python3 gen-token-config.py
```
按照终端提示在浏览器完成授权，Token 会自动写入 `config.json`。

### 4. 编辑完整配置文件

```bash
nano config.json
```
补全你的 Telegram、Vertex 或限速策略配置（配置说明见后文）。

### 5. 构建并启动容器

```bash
# 构建镜像
docker build -t netcup-control-restapi .

# 创建 docker-compose.yml
cat > docker-compose.yml << 'EOF'
services:
  netcupcontrol-restapi:
    image: netcup-control-restapi:latest
    container_name: netcupcontrol-restapi
    environment:
      - TZ=Asia/Shanghai
    ports:
      - "56578:56578"
    volumes:
      - ./config.json:/app/config.json
      - ./dashboard.html:/app/dashboard.html
      - ./daily_traffic.json:/app/daily_traffic.json
      - ./throttle_history.json:/app/throttle_history.json
      - /etc/localtime:/etc/localtime:ro
    restart: unless-stopped
EOF

# 启动容器
docker compose up -d

# 查看运行日志
docker logs -f netcupcontrol-restapi
```

### 6. 访问验证

- **Web 监控面板**：`http://你的服务器IP:56578/`（⚠️ 注意带上 `http://`，非 HTTPS）
- **API 状态接口**：`http://你的服务器IP:56578/api/status`

---

## 部署方式二：直接 Python 运行

### 1. 安装环境与依赖

- 系统要求：Debian 11+ / Ubuntu 20.04+ / CentOS 8+
- Python 版本：Python 3.9+

```bash
apt update && apt install -y python3 python3-pip python3-venv git sshpass

git clone https://github.com/WonderMaker123/netcup-control-restapi.git
cd netcup-control-restapi

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. 获取 Token 与配置

```bash
python3 gen-token-config.py
nano config.json
```

### 3. 配置开机自启（Systemd）

```bash
cat > /etc/systemd/system/netcup-monitor.service << 'EOF'
[Unit]
Description=Netcup Traffic Monitor Service
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/netcup-control-restapi
ExecStart=/root/netcup-control-restapi/venv/bin/python3 /root/netcup-control-restapi/netcup_monitor.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable netcup-monitor
systemctl start netcup-monitor
```

---

## 获取 Netcup API Token（核心步骤）

程序需要 Netcup SCP 的 API Token 才能查询服务器流量状态。项目提供了 `gen-token-config.py` 脚本实现全自动授权轮询。

1. **编辑脚本，填入你的账号 ID**：
   ```bash
   nano gen-token-config.py
   ```
   修改 `ACCOUNTS` 列表中的 `name`（填入你的 Netcup 客户号 Customer ID，如 `269495`）：
   ```python
   ACCOUNTS = [
       {"name": "269495", "client_id": "scp"},
       # 多账号继续往下加
   ]
   ```

2. **运行脚本**：
   ```bash
   python3 gen-token-config.py
   ```

3. **浏览器打开授权**：
   控制台会打印出授权链接与一次性用户代码：
   ```text
   🔗 请在浏览器打开以下链接完成授权：
       https://www.servercontrolpanel.de/...
   🔑 用户代码：XXXX-XXXX
   ```
   登录 Netcup SCP，输入代码确认授权，脚本会自动检测授权成功，并将 `access_token` 和 `refresh_token` 自动保存进 `config.json`。

---

## 配置文件完整说明

`config.json` 结构与字段详细对照表：

```json
{
  "webhook_path": "/webhook/wonder",
  "port": 56578,
  "check_interval": 300,
  "rest_accounts": [
    {
      "account_id": "269495",
      "access_token": "自动填入",
      "refresh_token": "自动填入"
    }
  ],
  "vertex": {
    "base_url": "http://159.195.28.128:3000",
    "cookie": "connect.sid=s%3A...",
    "manage_downloader": false
  },
  "disk_alarm": {
    "enabled": true,
    "cooldown_hours": 2,
    "thresholds_gib": [5.0, 1.0, 0.1]
  },
  "telegram": {
    "bot_token": "123456789:ABC...",
    "chat_id": "5677454102"
  },
  "throttle_action": {
    "strategy": "service",
    "delete_files": true,
    "exclude_categories": "keep,tl",
    "service": {
      "name": "qbittorrent-nox@wonder.service",
      "host": "159.195.28.128",
      "port": 22,
      "username": "root",
      "password": "your_password"
    }
  }
}
```

### 完整参数表

| 参数名 | 必填 | 默认值 | 说明 |
| :--- | :---: | :---: | :--- |
| `webhook_path` | **是** | - | Webhook 路径，建议使用自定义字符防扫，如 `/webhook/my_secret` |
| `port` | **是** | `56578` | 本服务监听端口 |
| `check_interval` | 否 | `300` | 后台定时数据轮询间隔（秒），最低可设为 `30` 秒 |
| `rest_accounts` | **是** | - | Netcup SCP 账户 Token 列表，由 `gen-token-config.py` 自动生成写入 |
| `vertex.base_url` | 否 | - | Vertex 地址（**末尾不要加斜杠和 `/api/v1`**），如 `http://IP:3000` |
| `vertex.cookie` | 否 | - | Vertex 登录 Session Cookie（浏览器按 F12 抓取的 `connect.sid=...`） |
| `vertex.manage_downloader` | 否 | `false` | **VT 下载器联动开关**。`false` 为不操作 VT 下载器；`true` 为限速时在 VT 暂停下载器、解除限速自动恢复 |
| `disk_alarm.enabled` | 否 | `true` | 是否启用 qB 磁盘剩余空间告警功能 |
| `disk_alarm.cooldown_hours`| 否 | `2` | 同级别磁盘告警的防刷屏冷却时间（小时） |
| `disk_alarm.thresholds_gib`| 否 | `[5.0, 1.0, 0.1]`| 磁盘空间告警阈值阶梯（分别对应 5GB黄色、1GB严重、0GB已满） |
| `telegram.bot_token` | 否 | - | Telegram Bot Token（通过 @BotFather 创建获取） |
| `telegram.chat_id` | 否 | - | 接收通知与执行交互命令的 Telegram 账号 ID（通过 @userinfobot 获取） |
| `throttle_action.strategy` | 否 | `pause` | 限速策略，支持 `service`、`pause`、`delete`、`pause_resume`、`pause_downloading`、`delete_downloading` |
| `throttle_action.delete_files`| 否 | `false`| 删除种子时是否连同本地文件一并删除（`true`/`false`） |
| `throttle_action.exclude_categories`| 否 | - | 排除操作的 qBittorrent 分类，英文逗号分隔，如 `"keep,tl"` |
| `throttle_action.service` | 否 | - | 当策略为 `service` 时的服务控制配置（见下方） |

---

## 策略对比表与 service 配置

### 6 种限速策略一览

| 策略代码 | 触发限速时 | 解除限速恢复时 | 适用场景 |
| :--- | :--- | :--- | :--- |
| **`service`** | 🛑 **直接停止 systemd 服务（qB 进程退出）** | ▶️ **自动启动系统服务** | **【推荐】完全停止 qB，彻底零网络零资源，超限自动断流保号** |
| `pause` | ⏸ 暂停所有种子（下载中+做种中） | ▶️ 恢复所有种子 | 临时超限停流，恢复后全部继续 |
| `delete` | 🗑 删除下载中种子，⏸ 暂停做种任务 | 保持现状 | 删掉占大带宽的下载任务，保留做种 |
| `pause_resume`| 🗑 删除下载中种子，⏸ 暂停已完成种子 | ▶️ 仅恢复已完成种子做种 | 彻底断掉下载流量，恢复后只做种 |
| `pause_downloading`| ⏸ 仅暂停下载中种子，做种任务不动 | ▶️ 恢复下载中种子 | 保护下载带宽，不影响做种分享率 |
| `delete_downloading`| 🗑 仅删除下载中种子，做种任务不动 | 保持现状 | 彻底清理下载中任务，做种正常 |

### service 策略配置详解

```json
"throttle_action": {
  "strategy": "service",
  "service": {
    "name": "qbittorrent-nox@wonder.service",
    "host": "159.195.28.128",
    "port": 22,
    "username": "root",
    "password": "your_ssh_password"
  }
}
```
- **控制本机服务**：不填 `host` 或填 `127.0.0.1`，直接在本机通过 `systemctl start/stop` 控制；
- **控制远程服务器**：填入远程 `host`、`port`、`username`、`password`，程序自动通过 `sshpass` 安全调用远程 systemd 命令。

---

## Telegram Bot 命令指南

配置好 `telegram.bot_token` 与 `telegram.chat_id` 后，机器人在首次启动时会自动注册菜单，在聊天窗口输入 `/` 即可触发：

| 命令 | 描述 | 功能亮点 |
| :--- | :--- | :--- |
| **`/status`** | 📊 查看所有服务器状态与跑量 | 输出服务器名、IP、状态、**💽 剩余磁盘空间**、**📅 今日流量（上传/下载）**、**💾 累计总流量**、当前策略与 VT 联动状态 |
| **`/check`** | 🔄 强制立即检测一次 | 立即穿透请求 Netcup 官方接口抓取最新数据并当场回传，无需等待定时轮询 |
| **`/daily`** | 📅 查看今日全服流量统计 | 汇总全服今日上传量、今日下载量与总跑量，支持随时手动查询 |
| **`/report`** | 📋 查看 Vertex 今日运行报告 | 查询 VT 当日做种上传、下载、分享率及各站点 Tracker Top 10 统计 |
| **`/help`** | ❓ 查看帮助信息 | 显示帮助与指令指南 |

> 🛡️ **安全提示**：程序内置管理员鉴权拦截，任何未经允许的外部 Telegram 用户向你的 Bot 发送命令均会被直接拦截，保护服务器隐私与数据安全。

---

## Webhook 配置

在 Netcup SCP 后台为服务器配置流量警告 Webhook，当官方检测到超出限制时会立即发送通知：

1. 登录 [Netcup SCP 控制面板](https://www.servercontrolpanel.de/)；
2. 找到对应服务器 ➡️ 流量监控 / Webhook 设置；
3. 填入你的 Webhook 地址（支持 GET 与 POST）：
   ```text
   http://你的监控服务IP:56578/webhook/你的密钥路径?ipv4IP=目标服务器IP
   ```
   **示例**：
   ```text
   http://129.153.208.230:56578/webhook/wonder?ipv4IP=159.195.28.128
   ```
4. **即时处置**：一旦 Netcup 触发该 Webhook，程序后台会立即异步对目标服务器执行紧急状态拉取与限速联动，**秒级完成断流保号**。

---

## 常见问题与排坑指南 (FAQ)

### Q1：为什么浏览器访问 `http://IP:56578/` 打不开，显示 502 或 ERR_SSL？
- **原因一：开启了代理客户端（Clash / VPN 等）**：绝大部分机场节点默认封禁 56578 等高位非标端口，导致走代理连接返回 `502 Bad Gateway`。**解决方式**：在代理软件中将监控服务器 IP 加入**直连（DIRECT）**或临时切换为直连模式。
- **原因二：浏览器强行升级 HTTPS**：现代 Chrome / Edge 默认开启 HTTPS-First，输入 IP 会自动补充 `https://...` 导致报 `ERR_SSL_PROTOCOL_ERROR`。**解决方式**：地址栏必须明确输入完整前缀 **`http://`**（如 `http://129.153.208.230:56578/`）。

### Q2：限速警告通知里显示“未知策略”是怎么回事？
新版本已完整映射所有 6 种限速与解除限速动作（包含 `service` 策略的停止/启动服务名称、做种保持等），彻底消灭了“未知策略”字样。

### Q3：Vertex (VT) Cookie 过期了怎么办？
打开浏览器访问你的 Vertex 前端，按 F12 复制 Cookie 字符串中的 `connect.sid=...`，更新进 `config.json` 的 `vertex.cookie` 并重启容器即可。新版本针对 401 鉴权失效做了全面隔离降级，**即使 Cookie 失效也绝不影响核心监控与限速控制**。

### Q4：如何让脚本在限速时不关闭 VT 里的下载器？
在 `config.json` 中配置 `"vertex": { "manage_downloader": false }`。此时限速时只会执行服务器控制（如停止 qB 服务），不会碰 VT 里的任何开关。

### Q5：如何更新代码？
```bash
cd netcup-control-restapi
git pull origin main
docker compose down
docker build -t netcup-control-restapi .
docker compose up -d
```
