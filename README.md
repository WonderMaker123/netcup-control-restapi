# Netcup Control REST API

基于 Flask 的 Netcup 流量监控与限速处理程序。当 Netcup 服务器流量超限时，自动暂停/删除 qBittorrent 下载任务。

---

## 目录

- [方式一：Docker 部署（推荐）](#方式一docker-部署推荐)
- [方式二：直接 Python 运行](#方式二直接-python-运行)
- [配置文件说明](#配置文件说明)
- [Webhook 配置](#webhook-配置)
- [常见问题](#常见问题)

---

## 方式一：Docker 部署（推荐）

### 1. 安装 Docker

```bash
# Debian / Ubuntu 一键安装
curl -fsSL https://get.docker.com | sh

# 安装 Docker Compose 插件
apt install -y docker-compose-plugin
```

### 2. 克隆项目

```bash
git clone https://github.com/WonderMaker123/netcup-control-restapi.git
cd netcup-control-restapi
```

### 3. 获取 Netcup API Token（核心步骤）

程序需要 Netcup SCP 的 API Token 才能查询服务器流量状态。项目提供了 `gen-token-config.py` 脚本自动获取。

**① 编辑脚本，填入你的账号 ID**

```bash
nano gen-token-config.py
```

找到 `ACCOUNTS` 列表，改为你的账号：

```python
ACCOUNTS = [
    {"name": "327110", "client_id": "scp"},
    # 如果有多个账号，继续添加
    # {"name": "327111", "client_id": "scp"},
]
```

> `name` 填你的 Netcup 客户号（Customer ID），`client_id` 固定为 `"scp"`。

**② 运行脚本获取 Token**

```bash
# 安装依赖（只需 requests）
pip install requests

# 运行脚本
python3 gen-token-config.py
```

**③ 浏览器授权**

脚本会输出一个链接和一个用户代码，例如：

```
🔗 请在浏览器打开以下链接完成授权：
    https://www.servercontrolpanel.de/...
🔑 用户代码：XXXX-XXXX
```

在浏览器打开链接，登录 Netcup SCP，输入用户代码，完成授权。

> 脚本会自动轮询等待授权完成，授权成功后 `access_token` 和 `refresh_token` 会自动写入 `config.json`。

**④ 确认 token 已写入**

```bash
cat config.json
```

`rest_accounts` 段应该已经自动填好了 `access_token` 和 `refresh_token`。

### 4. 编辑完整配置

```bash
nano config.json
```

补全以下内容：

```json
{
  "webhook_path": "/webhook/自定义密钥",
  "port": 56578,
  "rest_accounts": [
    {
      "account_id": "327110",
      "access_token": "上一步自动获取的access_token",
      "refresh_token": "上一步自动获取的refresh_token"
    }
  ],
  "vertex": {
    "base_url": "http://你的Vertex服务器IP:3000",
    "cookie": "你的Vertex登录cookie"
  },
  "telegram": {
    "bot_token": "你的Telegram Bot Token",
    "chat_id": "你的Telegram Chat ID"
  },
  "throttle_action": {
    "strategy": "pause_resume",
    "delete_files": true,
    "exclude_categories": "keep,tl"
  }
}
```

各配置项说明见下方 [配置文件说明](#配置文件说明)。

### 5. 构建并启动

```bash
# 构建镜像
docker build -t netcup-control-restapi .

# 创建 docker-compose.yml
cat > docker-compose.yml << 'EOF'
services:
  netcupcontrol-restapi:
    image: netcup-control-restapi:latest
    container_name: netcupcontrol-restapi
    ports:
      - "56578:56578"
    volumes:
      - ./config.json:/app/config.json
      - ./dashboard.html:/app/dashboard.html
    restart: unless-stopped
EOF

# 启动容器
docker compose up -d

# 查看日志
docker logs -f netcupcontrol-restapi
```

### 6. 验证

访问 `http://你的服务器IP:56578/api/status` 查看服务器状态，或访问 `http://你的服务器IP:56578/` 查看监控面板。

---

## 方式二：直接 Python 运行

### 1. 环境要求

- Debian 11 / Ubuntu 22.04+
- Python 3.9+
- Git

### 2. 安装依赖

```bash
apt update && apt upgrade -y
apt install -y python3 python3-pip python3-venv git

git clone https://github.com/WonderMaker123/netcup-control-restapi.git
cd netcup-control-restapi

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. 获取 Token（同 Docker 方式）

```bash
# 编辑 gen-token-config.py，填入你的账号 ID
nano gen-token-config.py

# 运行脚本
python3 gen-token-config.py
```

按提示在浏览器中打开链接完成授权，Token 会自动写入 `config.json`。

### 4. 编辑配置

```bash
nano config.json
```

补全 `vertex`、`telegram`、`throttle_action` 等配置（见下方说明）。

### 5. 运行

```bash
# 前台运行
python3 netcup_monitor.py

# 后台运行（关闭终端后继续运行）
nohup python3 netcup_monitor.py > output.log 2>&1 &
tail -f output.log
```

### 6. 开机自启（可选）

```bash
cat > /etc/systemd/system/netcup-monitor.service << 'EOF'
[Unit]
Description=Netcup Traffic Monitor
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

## 配置文件说明

`config.json` 完整配置项：

| 参数 | 必填 | 说明 |
|------|------|------|
| `webhook_path` | 是 | Webhook 路径，建议改为随机字符串，如 `/webhook/a1b2c3d4` |
| `port` | 是 | 服务端口，默认 `56578` |
| `rest_accounts` | 是 | Netcup SCP 账户配置，`gen-token-config.py` 自动填写 |
| `vertex.base_url` | 否 | Vertex 服务地址，**不要加 `/api/v1`**，直接写 `http://IP:3000` |
| `vertex.cookie` | 否 | Vertex 登录 Cookie |
| `vertex.manage_downloader` | 否 | 是否联动启停 VT 里的下载器，默认 `false`（不操作 VT 下载器）；如设为 `true` 则限速时自动暂停对应 VT 下载器，解除限速自动恢复 |
| `disk_alarm.enabled` | 否 | 是否启用 qB 磁盘剩余空间告警，默认 `true`（低于 5GB/1GB/0GB 自动向 Telegram 发送阶梯告警） |
| `disk_alarm.cooldown_hours` | 否 | 同级别磁盘告警冷却时间（小时），默认 `2` |
| `check_interval` | 否 | 服务器流量检查间隔（秒），默认 `300`，最低 `30` |
| `telegram.bot_token` | 否 | Telegram Bot Token（@BotFather 创建） |
| `telegram.chat_id` | 否 | 接收通知的 Telegram 聊天 ID |
| `throttle_action.strategy` | 否 | 限速策略，详见下方 [策略对比表](#策略对比表) |
| `throttle_action.delete_files` | 否 | 删除时是否同时删除文件（`true`/`false`） |
| `throttle_action.exclude_categories` | 否 | 排除的 qBittorrent 分类，多个用逗号分隔，如 `"keep,tl"` |

**注意：** `rest_accounts` 通过 `gen-token-config.py` 自动获取，不要手动填写。其他配置项根据需要手动补全。

---

## 策略对比表

`throttle_action.strategy` 支持以下 6 种策略：

| 策略 | 限速触发时 | 恢复时 | 适用场景 |
|------|-----------|--------|---------|
| `pause` | ⏸ 暂停所有种子（下载中+已完成） | ▶️ 恢复所有 | 临时停下载，恢复后继续 |
| `delete` | 🗑 删除下载中种子，🔄 已完成暂停做种 | 无需恢复 | 保证做种，删下载中占带宽的 |
| `pause_resume` | 🗑 删除下载中种子，⏸ 暂停已完成种子 | ▶️ 恢复已完成 | 彻底停掉所有流量 |
| `pause_downloading` | ⏸ 暂停下载中种子，➡ 已完成不动（继续做种） | ▶️ 恢复下载中 | 下载中临时停，做种不影响 |
| `delete_downloading` | 🗑 删除下载中种子，➡ 已完成不动（继续做种） | 无需恢复 | 删下载中的，做种不动 |
| `service` | 🛑 直接停止 systemd 服务（qB 进程完全退出） | ▶️ 启动服务 | 彻底停 qB，零资源占用 |

**选择建议：**

- 只是临时超限，恢复后要继续全部下载 → 用 `pause`
- 超限后只保留做种贡献，删下载中的 → 用 `delete` 或 `delete_downloading`
- 超限后完全停掉所有流量（不产生任何流量） → 用 `pause_resume`
- 下载中的暂停，做种继续 → 用 `pause_downloading`
- **想彻底关掉 qB 进程，恢复后自动续种** → 用 `service`

### service 策略配置

使用 `service` 策略时，需要在 `throttle_action` 中配置 `service` 段：

```json
"throttle_action": {
    "strategy": "service",
    "service": {
        "name": "qbittorrent-nox@wonder.service"
    }
}
```

如果 qBittorrent 在远程机器上，可以添加 SSH 配置：

```json
"throttle_action": {
    "strategy": "service",
    "service": {
        "name": "qbittorrent-nox@wonder.service",
        "host": "192.168.1.100",
        "port": 22,
        "username": "root",
        "password": "your_password"
    }
}
```

> **说明：** 不填 `host` 时默认控制本机服务。远程控制需要安装 `sshpass`：`apt install sshpass`

---

## Webhook 配置

在 Netcup SCP 控制面板中，为每台服务器配置 Webhook，地址为：

```
http://你的服务器IP:你设置的端口/你设置的webhook路径?ipv4IP=服务器IP
```

示例：

```
http://123.123.123.123:56578/webhook/a1b2c3d4?ipv4IP=159.195.28.128
```

请求方式：GET 或 POST 均可。

---

## 常见问题

### Q: 启动后日志显示 "配置文件加载失败"？

检查 `config.json` 的 JSON 格式是否正确。可以用 `python3 -m json.tool config.json` 校验。

### Q: 限速触发了但种子没被处理？

1. 检查 `config.json` 中 `throttle_action` 段配置是否正确
2. 检查 qBittorrent 的 WebUI 是否开启，且容器/本机能正常连接
3. 查看日志中是否有 `连接失败` 的提示

### Q: 如何排除某些分类的种子不被操作？

在 `config.json` 的 `throttle_action` 中设置：

```json
"exclude_categories": "keep,tl"
```

多个分类用英文逗号分隔。这些分类的种子在限速时会被跳过，不受影响。

### Q: Token 过期了怎么办？

Netcup SCP 的 Token 有效期 300 秒，程序会自动使用 `refresh_token` 刷新，无需手动处理。但如果长时间未运行，`refresh_token` 也可能过期，重新运行 `gen-token-config.py` 即可。

### Q: 如何更新代码？

```bash
cd netcup-control-restapi
git pull origin main
docker compose down
docker build -t netcup-control-restapi .
docker compose up -d
```