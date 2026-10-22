#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram 通知与命令模块
支持推送通知 + 交互式命令响应（Bot 菜单）
"""

import threading
import time
import requests
from datetime import datetime
from logger import logger


class TelegramNotifier:
    """Telegram 通知器，支持推送通知和交互式命令"""

    # Bot 命令菜单定义
    BOT_COMMANDS = [
        {"command": "status", "description": "📊 查看所有服务器状态与今日流量"},
        {"command": "check", "description": "🔄 强制立即检测一次服务器状态"},
        {"command": "daily", "description": "📅 查看今日服务器流量统计"},
        {"command": "report", "description": "📋 获取 Vertex 运行报告"},
        {"command": "help", "description": "❓ 帮助信息"},
    ]

    def __init__(self, bot_token: str, chat_id: str):
        """
        初始化 Telegram 通知器

        Args:
            bot_token: Telegram Bot Token
            chat_id: 目标 Chat ID
        """
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
        self.api_url = f"{self.base_url}/sendMessage"

        # 数据回调函数（由外部设置，用于响应命令）
        self.get_status_callback = None           # () -> dict
        self.get_report_callback = None           # () -> dict
        self.force_check_callback = None          # () -> None (触发立即刷新)
        self.get_daily_report_callback = None     # () -> str (获取每日流量文本)

        # 上次处理的 update_id，用于轮询
        self._last_update_id = 0
        self._polling_thread = None
        self._running = False

    # ──────────────────────────────────────────
    # 命令菜单注册
    # ──────────────────────────────────────────

    def set_commands(self) -> bool:
        """
        注册 Bot 命令菜单（Telegram 输入 / 时显示）
        调用 setMyCommands API
        """
        try:
            url = f"{self.base_url}/setMyCommands"
            payload = {"commands": self.BOT_COMMANDS}
            resp = requests.post(url, json=payload, timeout=10)
            resp.raise_for_status()
            result = resp.json()
            if result.get("ok"):
                logger.info(f"[Telegram] Bot 命令菜单已注册 ({len(self.BOT_COMMANDS)} 个命令)")
                return True
            else:
                logger.warning(f"[Telegram] 注册命令菜单失败: {result}")
                return False
        except Exception as e:
            logger.error(f"[Telegram] 注册命令菜单出错: {e}")
            return False

    # ──────────────────────────────────────────
    # 消息发送
    # ──────────────────────────────────────────

    def send_message(self, text: str, parse_mode: str = "HTML") -> bool:
        """发送消息到 Telegram"""
        try:
            payload = {
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": parse_mode,
                "disable_web_page_preview": True
            }
            response = requests.post(self.api_url, json=payload, timeout=10)
            response.raise_for_status()
            result = response.json()
            if result.get("ok"):
                return True
            else:
                logger.error(f"[Telegram] 消息发送失败: {result}")
                return False
        except requests.exceptions.RequestException as e:
            logger.error(f"[Telegram] 发送消息时网络错误: {e}")
            return False
        except Exception as e:
            logger.error(f"[Telegram] 发送消息时发生错误: {e}")
            return False

    def send_reply(self, chat_id: str, text: str, parse_mode: str = "HTML") -> bool:
        """回复指定聊天的消息（用于命令响应）"""
        try:
            url = f"{self.base_url}/sendMessage"
            payload = {
                "chat_id": chat_id,
                "text": text,
                "parse_mode": parse_mode,
                "disable_web_page_preview": True
            }
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"[Telegram] 回复消息失败: {e}")
            return False

    # ──────────────────────────────────────────
    # 命令轮询
    # ──────────────────────────────────────────

    def start_polling(self):
        """在后台线程启动命令轮询"""
        if self._polling_thread and self._polling_thread.is_alive():
            logger.warning("[Telegram] 轮询线程已在运行")
            return

        self._running = True
        self._polling_thread = threading.Thread(target=self._polling_loop, daemon=True)
        self._polling_thread.start()
        logger.info("[Telegram] 命令轮询线程已启动")

    def stop_polling(self):
        """停止轮询"""
        self._running = False
        logger.info("[Telegram] 命令轮询线程已停止")

    def _polling_loop(self):
        """轮询 Telegram API 获取新消息"""
        offset = 0
        while self._running:
            try:
                url = f"{self.base_url}/getUpdates"
                params = {
                    "offset": offset,
                    "timeout": 30,       # 长轮询 30 秒
                    "allowed_updates": ["message"]
                }
                resp = requests.get(url, params=params, timeout=35)
                resp.raise_for_status()
                result = resp.json()

                if result.get("ok") and result.get("result"):
                    for update in result["result"]:
                        offset = update["update_id"] + 1
                        self._handle_update(update)

            except requests.exceptions.Timeout:
                # 长轮询超时是正常的，继续下一轮
                continue
            except requests.exceptions.ConnectionError:
                logger.warning("[Telegram] 轮询连接失败，5秒后重试")
                time.sleep(5)
            except Exception as e:
                logger.error(f"[Telegram] 轮询出错: {e}")
                time.sleep(5)

    def _handle_update(self, update: dict):
        """处理单条更新"""
        message = update.get("message")
        if not message:
            return

        # 只处理 / 开头的命令
        text = message.get("text", "").strip()
        if not text.startswith("/"):
            return

        chat_id = str(message["chat"]["id"])
        # 提取命令名（去掉参数和 @botname）
        cmd = text.split()[0].split("@")[0].lower()

        logger.info(f"[Telegram] 收到命令: {cmd} (来自 {chat_id})")

        # 权限校验：仅允许配置的目标 chat_id 执行命令
        if self.chat_id and str(chat_id) != str(self.chat_id):
            logger.warning(f"[Telegram] 拦截未授权用户 {chat_id} 的命令: {text}")
            self.send_reply(chat_id, "⛔ 您无权使用此 Bot。")
            return

        if cmd == "/start":
            self._cmd_start(chat_id)
        elif cmd == "/help":
            self._cmd_help(chat_id)
        elif cmd == "/status":
            self._cmd_status(chat_id)
        elif cmd == "/check":
            self._cmd_check(chat_id)
        elif cmd == "/daily":
            self._cmd_daily(chat_id)
        elif cmd == "/report":
            self._cmd_report(chat_id)
        else:
            self.send_reply(chat_id, f"❌ 未知命令: {cmd}\n\n发送 /help 查看可用命令")

    # ──────────────────────────────────────────
    # 命令处理
    # ──────────────────────────────────────────

    def _cmd_start(self, chat_id: str):
        """处理 /start 命令"""
        msg = (
            "👋 <b>Netcup 流量监控 Bot</b>\n\n"
            "我监控你的 Netcup 服务器流量，超限时自动处理 qBittorrent 下载任务。\n\n"
            f"发送 /help 查看可用命令"
        )
        self.send_reply(chat_id, msg)

    def _cmd_help(self, chat_id: str):
        """处理 /help 命令"""
        lines = ["<b>📋 可用命令</b>\n"]
        for cmd in self.BOT_COMMANDS:
            lines.append(f"/{cmd['command']} — {cmd['description']}")
        lines.append("\n💡 提示：在输入框打 / 可以看到菜单")
        self.send_reply(chat_id, "\n".join(lines))

    def _cmd_status(self, chat_id: str):
        """处理 /status 命令"""
        if not self.get_status_callback:
            self.send_reply(chat_id, "❌ 状态数据获取函数未注册")
            return

        try:
            raw_data = self.get_status_callback()
            if not raw_data:
                self.send_reply(chat_id, "📡 暂无服务器数据")
                return

            # 兼容嵌套格式：如果包含 "data" 键则解包，否则使用本身
            data = raw_data.get("data") if (isinstance(raw_data, dict) and "data" in raw_data) else raw_data
            if not isinstance(data, dict):
                self.send_reply(chat_id, "📡 暂无服务器数据")
                return

            servers = data.get("servers", [])
            if not servers:
                self.send_reply(chat_id, "📡 暂无服务器数据")
                return

            total = len(servers)
            throttled = sum(1 for s in servers if s.get("trafficThrottled"))
            normal = total - throttled

            strategy_raw = data.get('throttle_strategy', '未知')
            strategy_map = {
                'pause': '暂停所有种子 (pause)',
                'delete': '删除下载中种子 (delete)',
                'pause_resume': '暂停已完成/删除下载中 (pause_resume)',
                'pause_downloading': '暂停下载中 (pause_downloading)',
                'delete_downloading': '删除下载中 (delete_downloading)',
                'service': '系统服务控制 (service)'
            }
            strategy_desc = strategy_map.get(strategy_raw, strategy_raw)
            exclude_cats = data.get('exclude_categories', [])
            exclude_desc = ','.join(exclude_cats) if exclude_cats else '无'
            last_update = data.get('last_update', '未知')
            vt_manage = data.get('vertex_manage_downloader', False)
            vt_manage_str = "已开启联动" if vt_manage else "已关闭 (不操作VT下载器)"

            msg = (
                f"<b>📊 Netcup 服务器状态</b>\n\n"
                f"🖥️ 总服务器: <b>{total}</b> 台\n"
                f"🟢 正常运行: <b>{normal}</b> 台\n"
                f"🔴 限速保护: <b>{throttled}</b> 台\n"
                f"⚙️ 策略模式: <code>{strategy_desc}</code>\n"
                f"🔌 VT下载器控制: <code>{vt_manage_str}</code>\n"
                f"🏷️ 排除分类: <code>{exclude_desc}</code>\n"
                f"⏰ 更新时间: <code>{last_update}</code>\n\n"
            )

            for s in servers[:10]:  # 最多显示10台
                is_throttled = s.get("trafficThrottled", False)
                status_icon = "🔴" if is_throttled else "🟢"
                name = s.get("server_name") or s.get("hostname") or "未知"
                ip = s.get("ipv4IP", "未知IP")
                traffic = s.get("traffic_tib", 0.0)
                tx = s.get("tx_tib", 0.0)
                rx = s.get("rx_tib", 0.0)
                today_total = s.get("today_traffic_tib", 0.0)
                today_tx = s.get("today_tx_tib", 0.0)
                today_rx = s.get("today_rx_tib", 0.0)
                disk_gib = s.get("free_disk_gib")
                status_tag = "限速中" if is_throttled else "正常"

                if disk_gib is not None:
                    if disk_gib <= 0.1:
                        disk_str = f"🛑 <b>{disk_gib:.2f} GiB (已满)</b>"
                    elif disk_gib <= 1.0:
                        disk_str = f"🚨 <b>{disk_gib:.2f} GiB (严重不足)</b>"
                    elif disk_gib <= 5.0:
                        disk_str = f"⚠️ <b>{disk_gib:.2f} GiB (空间紧张)</b>"
                    else:
                        disk_str = f"<code>{disk_gib:.2f} GiB</code>"
                else:
                    disk_str = "<i>检测中</i>"

                msg += f"{status_icon} <b>{name}</b> (<code>{ip}</code>)\n"
                msg += f"   • 状态: <b>{status_tag}</b>\n"
                msg += f"   • 💽 <b>剩余磁盘</b>: {disk_str}\n"
                msg += f"   • 📅 <b>今日流量</b>: <code>{today_total:.3f} TiB</code> (↑{today_tx:.3f} / ↓{today_rx:.3f})\n"
                msg += f"   • 💾 <b>总计流量</b>: <code>{traffic:.3f} TiB</code> (↑{tx:.3f} / ↓{rx:.3f})\n"
                if is_throttled:
                    duration = s.get('current_throttle_duration', '0分钟')
                    msg += f"   • 当前限速时长: <code>{duration}</code>\n"
                msg += "\n"

            if len(servers) > 10:
                msg += f"<i>...还有 {len(servers) - 10} 台未显示</i>\n"

            self.send_reply(chat_id, msg)

        except Exception as e:
            logger.error(f"[Telegram] 处理 /status 命令出错: {e}")
            self.send_reply(chat_id, f"❌ 获取状态失败: {e}")

    def _cmd_check(self, chat_id: str):
        """处理 /check 命令（强制立即连接 Netcup 检测最新状态）"""
        self.send_reply(chat_id, "🔄 正在连接 Netcup 立即检测最新服务器状态与流量，请稍候...")
        if self.force_check_callback:
            try:
                self.force_check_callback()
            except Exception as e:
                logger.error(f"[Telegram] 强制检测执行异常: {e}")
        # 检测完毕后返回最新状态卡片
        self._cmd_status(chat_id)

    def _cmd_daily(self, chat_id: str):
        """处理 /daily 命令（查看今日流量简报）"""
        if self.get_daily_report_callback:
            try:
                report = self.get_daily_report_callback()
                if report:
                    self.send_reply(chat_id, report)
                    return
            except Exception as e:
                logger.error(f"[Telegram] 获取每日流量报告失败: {e}")
        self.send_reply(chat_id, "📡 暂无今日流量报告数据，请先尝试 /status 查看状态")

    def _cmd_report(self, chat_id: str):
        """处理 /report 命令"""
        if not self.get_report_callback:
            self.send_reply(chat_id, "❌ 报告数据获取函数未注册")
            return

        try:
            data = self.get_report_callback()
            if not data or not data.get("success"):
                err_msg = data.get("error", "未知错误") if isinstance(data, dict) else "接口未返回数据"
                err_str = str(err_msg)
                if "401" in err_str or "Unauthorized" in err_str or "鉴权失效" in err_str:
                    reply_text = (
                        "❌ <b>获取 Vertex 报告失败: 鉴权失效 (401)</b>\n\n"
                        "🔑 原因: <code>config.json</code> 中的 Vertex Cookie 已过期失效。\n\n"
                        "💡 解决方式: 请重新登录 Vertex 控制台，获取最新 Cookie 并更新至 <code>config.json</code> 中的 <code>vertex.cookie</code>。"
                    )
                elif "配置缺失" in err_str:
                    reply_text = "⚠️ 未配置 Vertex 服务地址或 Cookie，无法生成 Vertex 运行报告。"
                else:
                    reply_text = f"❌ 获取 Vertex 报告失败:\n<code>{err_str}</code>"
                self.send_reply(chat_id, reply_text)
                return

            msg = self.format_vertex_report(data.get("data", {}))
            self.send_reply(chat_id, msg)

        except Exception as e:
            logger.error(f"[Telegram] 处理 /report 命令出错: {e}")
            self.send_reply(chat_id, f"❌ 获取报告失败: {e}")

    # ──────────────────────────────────────────
    # 报告格式化（保持原有逻辑）
    # ──────────────────────────────────────────

    @staticmethod
    def bytes_to_tib(bytes_value: int) -> float:
        return bytes_value / (1024 ** 4)

    @staticmethod
    def format_ratio(uploaded: int, downloaded: int) -> str:
        if downloaded == 0:
            return "∞"
        ratio = uploaded / downloaded
        return f"{ratio:.3f}"

    def format_vertex_report(self, data: dict) -> str:
        """格式化 Vertex 运行报告"""
        try:
            uploaded_today = data.get('uploadedToday', 0)
            downloaded_today = data.get('downloadedToday', 0)
            uploaded_today_tib = self.bytes_to_tib(uploaded_today)
            downloaded_today_tib = self.bytes_to_tib(downloaded_today)
            ratio_today = self.format_ratio(uploaded_today, downloaded_today)

            uploaded_total = data.get('uploaded', 0)
            downloaded_total = data.get('downloaded', 0)
            uploaded_total_tib = self.bytes_to_tib(uploaded_total)
            downloaded_total_tib = self.bytes_to_tib(downloaded_total)
            ratio_total = self.format_ratio(uploaded_total, downloaded_total)

            add_today = data.get('addCountToday', 0)
            reject_today = data.get('rejectCountToday', 0)
            delete_today = data.get('deleteCountToday', 0)

            message = f"""<b>📊 Vertex 今日运行报告</b>

<b>📈 今日流量统计</b>
• 上传: <code>{uploaded_today_tib:.3f} TiB</code>
• 下载: <code>{downloaded_today_tib:.3f} TiB</code>
• 分享率: <code>{ratio_today}</code>

<b>📦 今日任务统计</b>
• 新增: <code>{add_today}</code> 个
• 拒绝: <code>{reject_today}</code> 个
• 删除: <code>{delete_today}</code> 个

<b>💾 总计流量统计</b>
• 上传: <code>{uploaded_total_tib:.3f} TiB</code>
• 下载: <code>{downloaded_total_tib:.3f} TiB</code>
• 分享率: <code>{ratio_total}</code>"""

            per_tracker_today = data.get('perTrackerToday', [])
            if per_tracker_today:
                sorted_trackers = sorted(
                    per_tracker_today,
                    key=lambda x: x.get('uploaded', 0),
                    reverse=True
                )
                message += "\n\n<b>🎯 今日 Tracker Top 10 (按上传量排序)</b>"
                for idx, tracker in enumerate(sorted_trackers[:10], 1):
                    tracker_name = tracker.get('tracker', 'Unknown')
                    tracker_up = tracker.get('uploaded', 0)
                    tracker_down = tracker.get('downloaded', 0)
                    tracker_up_tib = self.bytes_to_tib(tracker_up)
                    tracker_down_tib = self.bytes_to_tib(tracker_down)
                    tracker_ratio = self.format_ratio(tracker_up, tracker_down)
                    message += f"\n\n<b>{idx}. {tracker_name}</b>"
                    message += f"\n   ↑ <code>{tracker_up_tib:.3f} TiB</code> | ↓ <code>{tracker_down_tib:.3f} TiB</code> | 比率 <code>{tracker_ratio}</code>"

            message += f"\n\n<i>⏰ 统计时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</i>"
            return message

        except Exception as e:
            logger.error(f"[Telegram] 格式化报告时发生错误: {e}")
            return f"<b>❌ 生成报告失败</b>\n\n错误: {str(e)}"

    def send_vertex_report(self, api_data: dict) -> bool:
        """发送 Vertex 运行报告"""
        if not api_data.get('success'):
            logger.error("[Telegram] API 数据返回失败,无法生成报告")
            return False
        data = api_data.get('data', {})
        message = self.format_vertex_report(data)
        return self.send_message(message)