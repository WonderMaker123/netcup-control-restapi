# Netcup Control REST API

基于 Flask 的 Netcup 流量监控与限速处理程序。当 Netcup 服务器流量超限时，自动暂停/删除 qBittorrent 下载任务。

---

## 目录

- [方式一：直接 Python 运行（推荐）](#方式一直接-python-运行推荐)
- [方式二：Docker 部署](#方式二docker-部署)
- [配置文件说明](#配置文件说明)
- [Webhook 配置](#webhook-配置)
- [常见问题](#常见问题)

---

## 方式一：直接 Python 运行（推荐）

### 1. 环境要求

- Debian 11 / Ubuntu 22.04 或更高版本
- Python 3.9+
- Git

### 2. 安装依赖

```bash
# 更新系统
apt update && apt upgrade -y

# 安装 Python 和 Git
apt install -y python3 python3-pip python3-venv git

# 克隆项目
git clone https://github.com/WonderMaker123/netcup-control-restapi.git
cd netcup-control-restapi

# 创建虚拟环境（推荐）
python3 -m venv venv
source venv/bin/activate

# 安装 Python 依赖
pip install -r requirements.txt
```

### 3. 获取 Token 配置

先编辑 `gen-token-config.py`，填入你的 Netcup SCP 账户信息：

```python
ACCOUNTS = [
    {"name": "331022", "client_id": "scp"},
    {"name": "331058", "client_id": "scp"},
    # 如果有多个账户，继续添加
]
```

运行获取 Token 脚本：

```bash
python3 gen-token-config.py
```

按提示在浏览器中打开显示的网址，登录 SCP 账户，脚本会自动填写 `config.json`。

### 4. 编辑配置文件

`config.json` 获取 Token 后，还需要补全以下内容：

```json
{
  "webhook_path": "/webhook/自定义密钥",
  "port": 56578,
  "rest_accounts": [
    {
      "account_id": "你的账户ID",
      "access_token": "自动获取的access_token",
      "refresh_token": "自动获取的refresh_token"
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

**配置项说明：**

| 参数 | 说明 |
|------|------|
| `webhook_path` | Webhook 路径，建议修改为随机字符串防止被扫 |
| `port` | 服务端口，默认 56578 |
| `rest_accounts` | Netcup SCP 账户配置（Token 脚本自动填写） |
| `vertex.base_url` | Vertex 服务地址，**不要加 `/api/v1`**，直接写 `http://IP:3000` |
| `vertex.cookie` | Vertex 登录后的 Cookie |
| `telegram.bot_token` | Telegram Bot Token（@BotFather 创建获取） |
| `telegram.chat_id` | 接收通知的 Telegram 聊天 ID |
| `throttle_action.strategy` | 限速策略：`pause`（暂停）、`delete`（删除）、`pause_resume`（删除下载中+暂停已完成） |
| `throttle_action.delete_files` | 删除时是否同时删除文件（`true`/`false`） |
| `throttle_action.exclude_categories` | 排除的 qBittorrent 分类，多个用逗号分隔 |

### 5. 运行服务

```bash
# 确保在项目目录下，虚拟环境已激活
source venv/bin/activate  # 如果还没激活

# 直接运行
python3 netcup_monitor.py

# 或者用 nohup 后台运行（关闭终端后继续运行）
nohup python3 netcup_monitor.py > output.log 2>&1 &

# 查看日志
tail -f output.log
```

服务启动后，访问 `http://你的服务器IP:56578` 查看监控面板。

---

## 方式二：Docker 部署

### 1. 安装 Docker

```bash
# Debian 一键安装 Docker
curl -fsSL https://get.docker.com | sh

# 安装 Docker Compose 插件
apt install -y docker-compose-plugin
```

### 2. 克隆项目并准备配置

```bash
git clone https://github.com/WonderMaker123/netcup-control-restapi.git
cd netcup-control-restapi

# 编辑 config.json（同上方的配置说明）
# 编辑 dashboard.html（可选，自定义界面模板）
```

### 3. 构建镜像

```bash
docker build -t netcup-control-restapi .
```

### 4. 创建 docker-compose.yml

在项目目录下创建 `docker-compose.yml`：

```yaml
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
```

### 5. 启动容器

```bash
docker compose up -d
```

### 6. 查看日志

```bash
docker logs -f netcupcontrol-restapi
```

---

## Webhook 配置

在 Netcup SCP 控制面板中，为每台服务器配置 Webhook，地址为：

```
http://你的服务器IP:56578/webhook/你设置的密钥?ipv4IP=服务器IP
```

Webhook 请求方式：GET 或 POST 均可。

---

## 常见问题

### Q: 启动后日志显示 "配置文件加载失败"？

检查 `config.json` 的 JSON 格式是否正确，可以用在线 JSON 校验工具检查。

### Q: 限速触发了但种子没被处理？

1. 检查 `config.json` 中 `throttle_action` 段配置是否正确
2. 检查 qBittorrent 的 WebUI 是否开启，且能正常连接
3. 查看日志中是否有连接失败的提示

### Q: 如何排除某些分类的种子不被操作？

在 `config.json` 的 `throttle_action` 中设置：

```json
"exclude_categories": "keep,tl"
```

多个分类用英文逗号分隔。这些分类的种子在限速时会被跳过，不受影响。

### Q: 如何开机自启？

使用 systemd 服务（直接 Python 运行方式）：

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