import os
import json
import subprocess
import threading
import time
import requests
from datetime import datetime, timedelta
from flask import Flask, jsonify, request
from logger import logger, BEIJING_TZ, get_beijing_now, format_beijing_time, beijing_from_timestamp
from netcup_api import NetcupAPI
from qb_client import QBittorrentClient
from qb_rss import QBRSSClient
from telegram_notifier import TelegramNotifier
from apscheduler.schedulers.background import BackgroundScheduler


class NetcupTrafficMonitor:
    """增强版 Netcup 流量监控器"""

    def __init__(self):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.config_file = os.path.join(script_dir, 'config.json')
        self.history_file = os.path.join(script_dir, 'throttle_history.json')

        # 数据缓存
        self.cached_data = {}
        # 限速历史记录
        self.throttle_history = self.load_throttle_history()
        # 每日流量基准统计
        self.daily_traffic_file = os.path.join(script_dir, 'daily_traffic.json')
        self.daily_traffic = self.load_daily_traffic()

        # 加载配置
        config = self.load_config()
        self.webhook_path = config.get('webhook_path', '/webhook/secret-monitor')
        self.port = config.get('port', 56578)
        self.accounts = config.get('rest_accounts', [])
        # 检查频率配置 (秒，默认 300，最少 30 秒)
        self.check_interval = max(30, int(config.get('check_interval', 300)))
        logger.info(f"[配置] 流量检查间隔: {self.check_interval} 秒")

        # 限速处理策略配置
        throttle_config = config.get('throttle_action', {})
        self.throttle_strategy = throttle_config.get('strategy', 'pause')  # pause, delete, pause_resume
        self.delete_files = throttle_config.get('delete_files', False)  # 删除时是否删除文件
        
        # 新增：排除的分类列表
        exclude_categories_str = throttle_config.get('exclude_categories', '')
        self.exclude_categories = self._parse_exclude_categories(exclude_categories_str)
        
        # 服务控制策略配置（SSH/systemctl）
        self.service_config = throttle_config.get('service', {})
        self.service_name = self.service_config.get('name', '')
        self.ssh_config = {
            'host': self.service_config.get('host', ''),
            'port': self.service_config.get('port', 22),
            'username': self.service_config.get('username', ''),
            'password': self.service_config.get('password', ''),
        }
        
        logger.info(f"[配置] 限速处理策略: {self.throttle_strategy}")
        if self.throttle_strategy == 'service':
            logger.info(f"[配置] 服务控制: {self.service_name}")
            if self.ssh_config.get('host'):
                logger.info(f"[配置] SSH 远程控制: {self.ssh_config['host']}:{self.ssh_config['port']}")
            else:
                logger.info("[配置] 本地服务控制")
        if self.throttle_strategy in ('delete', 'delete_downloading', 'pause_resume'):
            logger.info(f"[配置] 删除文件: {self.delete_files}")
        if self.exclude_categories:
            logger.info(f"[配置] 排除分类: {self.exclude_categories}")

        # Vertex 配置
        vconf = config.get('vertex', {})
        self.vertex_base_url = vconf.get('base_url', '')
        self.vertex_cookie = vconf.get('cookie', '')
        # 控制是否在限速时联动启停 VT 里的下载器（默认 False，不操作 VT 下载器）
        self.vertex_manage_downloader = bool(vconf.get('manage_downloader', False))
        logger.info(f"[配置] Vertex 联动启停下载器: {'已启用' if self.vertex_manage_downloader else '已禁用(不操作VT下载器)'}")

        self.qb_rss = None
        if self.vertex_base_url:
            self.qb_rss = QBRSSClient(base=self.vertex_base_url, cookie=self.vertex_cookie)

        # Telegram 配置
        tg_config = config.get('telegram', {})
        self.telegram_bot_token = tg_config.get('bot_token', '')
        self.telegram_chat_id = tg_config.get('chat_id', '')
        self.telegram_enabled = bool(self.telegram_bot_token and self.telegram_chat_id)

        # 磁盘空间告警配置 (默认阈值: 5GB, 1GB, 0.1GB; 冷却时间 2 小时)
        disk_cfg = config.get('disk_alarm', {})
        self.disk_alarm_enabled = disk_cfg.get('enabled', True)
        self.disk_alarm_cooldown = int(disk_cfg.get('cooldown_hours', 2)) * 3600
        self.disk_alarm_state = {}  # {ip: {"last_notified_level": None, "last_notified_time": 0}}
        if self.disk_alarm_enabled:
            logger.info(f"[配置] 磁盘空间告警: 已启用 (阶梯阈值: 5GB / 1GB / 0GB，冷却: {self.disk_alarm_cooldown // 3600}小时)")
        else:
            logger.info("[配置] 磁盘空间告警: 已禁用")
        
        self.telegram_notifier = None
        if self.telegram_enabled:
            self.telegram_notifier = TelegramNotifier(
                bot_token=self.telegram_bot_token,
                chat_id=self.telegram_chat_id
            )
            # 注册 Bot 命令菜单
            self.telegram_notifier.set_commands()
            # 设置命令回调函数（全部使用 Netcup 官方真实数据，解耦 VT）
            self.telegram_notifier.get_status_callback = self._get_status_data
            self.telegram_notifier.get_report_callback = self.get_full_analytics_report_text
            self.telegram_notifier.force_check_callback = self.update_cached_data
            self.telegram_notifier.get_daily_report_callback = self.get_full_analytics_report_text
            self.telegram_notifier.get_history_callback = self.get_throttle_timeline_text
            # 启动命令轮询
            self.telegram_notifier.start_polling()
            logger.info("[Telegram] 通知功能已启用，Bot 命令菜单已注册")
        else:
            logger.warning("[Telegram] 通知功能未配置或已禁用")

        # 创建 Flask 应用
        self.app = Flask(__name__)
        self.setup_routes()

        # 启动数据收集线程
        self.data_thread = threading.Thread(target=self.data_collection_loop, daemon=True)
        self.data_thread.start()

        # 启动定时任务调度器
        if self.telegram_enabled:
            self.scheduler = BackgroundScheduler(timezone='Asia/Shanghai')
            # 每日 23:55 自动发送 Netcup 今日流量与运行分析晚报（真实官方数据）
            self.scheduler.add_job(
                func=self.send_daily_traffic_report,
                trigger='cron',
                hour=23,
                minute=55,
                id='netcup_daily_traffic_report'
            )
            self.scheduler.start()
            logger.info("[调度器] 定时任务已启动 (每日23:55自动推送流量与限速分析晚报)")
        else:
            self.scheduler = None
            logger.warning("[调度器] Telegram 未配置,定时日报任务未启动")

        logger.info("=" * 60)
        logger.info("NetcupTrafficMonitor 初始化完成")
        logger.info(f"Webhook路径: {self.webhook_path}")
        logger.info(f"端口: {self.port}")
        logger.info(f"加载了 {len(self.accounts)} 个账户")
        logger.info(f"Telegram 通知: {'已启用' if self.telegram_enabled else '未启用'}")
        logger.info(f"排除分类: {self.exclude_categories if self.exclude_categories else '无'}")
        logger.info("=" * 60)


    def _get_status_data(self) -> dict:
        """
        获取服务器状态数据（供 Telegram Bot 命令回调使用）
        与 /api/status 接口返回相同格式
        """
        servers = []
        for ip, data in self.cached_data.items():
            availability = self.calculate_availability(ip)
            enhanced_data = {**data, **availability}
            servers.append(enhanced_data)

        return {
            "success": True,
            "data": {
                "last_update": format_beijing_time(),
                "total_servers": len(servers),
                "throttled_count": sum(1 for s in servers if s.get('trafficThrottled')),
                "normal_count": sum(1 for s in servers if not s.get('trafficThrottled')),
                "throttle_strategy": self.throttle_strategy,
                "exclude_categories": self.exclude_categories,
                "vertex_manage_downloader": self.vertex_manage_downloader,
                "servers": servers
            }
        }

    def _is_localhost(self, host: str) -> bool:
        """判断目标主机是否为本地"""
        if not host:
            return True
        local_ips = ['127.0.0.1', 'localhost', '::1']
        if host in local_ips or host.startswith('127.') or host == '0.0.0.0':
            return True
        # 检查是否为本机网卡 IP
        try:
            import socket
            local_ip = socket.gethostbyname(socket.gethostname())
            if host == local_ip:
                return True
        except Exception:
            pass
        # 通过 ip addr 获取所有本机 IP
        try:
            result = subprocess.run(
                ["ip", "-4", "addr", "show", "scope", "global"],
                capture_output=True, text=True, timeout=5
            )
            for line in result.stdout.split('\n'):
                if 'inet ' in line:
                    ip = line.strip().split()[1].split('/')[0]
                    if host == ip:
                        return True
        except Exception:
            pass
        return False

    def _service_control(self, action: str) -> bool:
        """
        通过 systemctl 控制服务（本地或远程 SSH）
        
        Args:
            action: 'start' 或 'stop'
            
        Returns:
            bool: 是否成功
        """
        if not self.service_name:
            logger.error("[服务控制] 未配置 service_name")
            return False

        cmd = f"systemctl {action} {self.service_name}"
        ssh_host = self.ssh_config.get('host', '')

        try:
            if self._is_localhost(ssh_host):
                # 本地直接执行
                result = subprocess.run(
                    cmd.split(),
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                if result.returncode == 0:
                    logger.info(f"[服务控制] 本地 systemctl {action} {self.service_name} 成功")
                    return True
                else:
                    logger.error(f"[服务控制] 本地 systemctl {action} 失败: {result.stderr.strip()}")
                    return False
            else:
                # 远程 SSH 执行
                ssh_cmd = [
                    'sshpass', '-p', self.ssh_config.get('password', ''),
                    'ssh', '-o', 'StrictHostKeyChecking=no',
                    '-o', 'ConnectTimeout=10',
                    '-p', str(self.ssh_config.get('port', 22)),
                    f"{self.ssh_config['username']}@{ssh_host}",
                    cmd
                ]
                result = subprocess.run(
                    ssh_cmd,
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                if result.returncode == 0:
                    logger.info(f"[服务控制] SSH {ssh_host} systemctl {action} {self.service_name} 成功")
                    return True
                else:
                    logger.error(f"[服务控制] SSH {ssh_host} systemctl {action} 失败: {result.stderr.strip()}")
                    return False
        except subprocess.TimeoutExpired:
            logger.error(f"[服务控制] systemctl {action} 超时 (30秒)")
            return False
        except FileNotFoundError:
            logger.error("[服务控制] sshpass 未安装，远程 SSH 控制需要 sshpass: apt install sshpass")
            return False
        except Exception as e:
            logger.error(f"[服务控制] systemctl {action} 异常: {e}")
            return False


    def _parse_exclude_categories(self, categories_str: str) -> list[str]:
        """
        解析排除分类字符串为列表。
        支持逗号分隔、分号分隔或单个分类。
        
        Args:
            categories_str: 分类字符串，如 "ssd-返还" 或 "ssd-返还,免费种子"
            
        Returns:
            分类列表
        """
        if not categories_str or not isinstance(categories_str, str):
            return []
        
        # 尝试多种分隔符
        if ',' in categories_str:
            categories = [c.strip() for c in categories_str.split(',')]
        elif ';' in categories_str:
            categories = [c.strip() for c in categories_str.split(';')]
        elif '|' in categories_str:
            categories = [c.strip() for c in categories_str.split('|')]
        else:
            categories = [categories_str.strip()]
        
        # 过滤空字符串
        return [c for c in categories if c]


    def load_config(self):
        """加载配置文件"""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"加载配置文件时发生错误: {e}")
            return {}

    def load_throttle_history(self):
        """加载限速历史记录"""
        try:
            if os.path.exists(self.history_file):
                with open(self.history_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    logger.info(f"成功加载限速历史记录: {len(data)} 台服务器")
                    return data
            logger.info("限速历史文件不存在,将创建新的")
            return {}
        except Exception as e:
            logger.error(f"加载限速历史失败: {e}")
            return {}

    def save_throttle_history(self):
        """保存限速历史记录"""
        try:
            with open(self.history_file, 'w', encoding='utf-8') as f:
                json.dump(self.throttle_history, f, indent=2, ensure_ascii=False)
            logger.debug(f"限速历史已保存: {len(self.throttle_history)} 台服务器")
        except Exception as e:
            logger.error(f"保存限速历史失败: {e}")

    def load_daily_traffic(self) -> dict:
        """加载每日流量与月度基准统计"""
        today = format_beijing_time(fmt='%Y-%m-%d')
        current_month = format_beijing_time(fmt='%Y-%m')
        default_data = {
            "date": today,
            "month": current_month,
            "servers": {},
            "history": {},
            "months": {}
        }
        if not os.path.exists(self.daily_traffic_file):
            return default_data
        try:
            with open(self.daily_traffic_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if not isinstance(data, dict):
                return default_data
            data.setdefault("servers", {})
            data.setdefault("history", {})
            data.setdefault("months", {})
            data.setdefault("date", today)
            data.setdefault("month", current_month)
            return data
        except Exception as e:
            logger.error(f"加载每日流量记录出错: {e}")
            return default_data

    def save_daily_traffic(self):
        """保存每日与月度流量记录（直接写入并 fsync 确保容器与宿主机一致）"""
        try:
            with open(self.daily_traffic_file, 'w', encoding='utf-8') as f:
                json.dump(self.daily_traffic, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
        except Exception as e:
            logger.error(f"保存每日流量记录出错: {e}")

    def update_daily_traffic(self, ip: str, rx_mib: float, tx_mib: float, server_name: str = "") -> dict:
        """
        更新并计算某台服务器今天的上传、下载与总流量 (TiB)，并维护日历史与永久月度归档
        """
        today = format_beijing_time(fmt='%Y-%m-%d')
        current_month = format_beijing_time(fmt='%Y-%m')
        
        # 1. 跨天归档与基准重置
        current_recorded_date = self.daily_traffic.get("date")
        if current_recorded_date != today:
            logger.info(f"[流量统计] 跨日变更: {current_recorded_date} -> {today}，归档前一日流量并重置基准")
            old_servers = self.daily_traffic.get("servers", {})
            if old_servers and current_recorded_date:
                daily_summary = {}
                for s_ip, s_info in old_servers.items():
                    d_rx = max(0.0, s_info.get("last_rx_mib", 0) - s_info.get("base_rx_mib", 0))
                    d_tx = max(0.0, s_info.get("last_tx_mib", 0) - s_info.get("base_tx_mib", 0))
                    daily_summary[s_ip] = {
                        "name": s_info.get("server_name", s_ip),
                        "rx_tib": round(self.bytes_to_tib(d_rx), 3),
                        "tx_tib": round(self.bytes_to_tib(d_tx), 3),
                        "total_tib": round(self.bytes_to_tib(d_rx + d_tx), 3)
                    }
                self.daily_traffic.setdefault("history", {})[current_recorded_date] = daily_summary
                
                # 历史日数据保留 180 天 (半年)
                if len(self.daily_traffic["history"]) > 180:
                    oldest_keys = sorted(self.daily_traffic["history"].keys())[:-180]
                    for k in oldest_keys:
                        self.daily_traffic["history"].pop(k, None)

            # 更新日期并将现有机器基准置为上次记录的值
            self.daily_traffic["date"] = today
            for s_ip in list(self.daily_traffic.get("servers", {}).keys()):
                last_rx = self.daily_traffic["servers"][s_ip].get("last_rx_mib", rx_mib)
                last_tx = self.daily_traffic["servers"][s_ip].get("last_tx_mib", tx_mib)
                self.daily_traffic["servers"][s_ip]["base_rx_mib"] = last_rx
                self.daily_traffic["servers"][s_ip]["base_tx_mib"] = last_tx

        # 2. 跨月检测与月度永久归档
        current_recorded_month = self.daily_traffic.get("month", current_month)
        if current_recorded_month != current_month:
            logger.info(f"[流量统计] 跨月变更: {current_recorded_month} -> {current_month}，结转上月流量记录")
            self._sync_month_summary(current_recorded_month)
            self.daily_traffic["month"] = current_month

        # 3. 记录/更新当前服务器基准
        servers = self.daily_traffic.setdefault("servers", {})
        if ip not in servers:
            # 首次记录以当前值为初始基准
            servers[ip] = {
                "server_name": server_name,
                "base_rx_mib": rx_mib,
                "base_tx_mib": tx_mib,
                "last_rx_mib": rx_mib,
                "last_tx_mib": tx_mib
            }
        else:
            s_data = servers[ip]
            if server_name:
                s_data["server_name"] = server_name
            # 如果机房重启或月初网卡计数器清零，自动校准基准
            if tx_mib < s_data.get("base_tx_mib", 0):
                logger.info(f"[{ip}] 检测到网卡发送计数清零({s_data.get('base_tx_mib')} -> {tx_mib})，自动重置日基准")
                s_data["base_tx_mib"] = tx_mib
            if rx_mib < s_data.get("base_rx_mib", 0):
                logger.info(f"[{ip}] 检测到网卡接收计数清零({s_data.get('base_rx_mib')} -> {rx_mib})，自动重置日基准")
                s_data["base_rx_mib"] = rx_mib
            s_data["last_rx_mib"] = rx_mib
            s_data["last_tx_mib"] = tx_mib

        s_data = servers[ip]
        delta_rx_mib = max(0.0, rx_mib - s_data.get("base_rx_mib", 0))
        delta_tx_mib = max(0.0, tx_mib - s_data.get("base_tx_mib", 0))
        today_rx_tib = self.bytes_to_tib(delta_rx_mib)
        today_tx_tib = self.bytes_to_tib(delta_tx_mib)
        today_total_tib = today_rx_tib + today_tx_tib

        # 同步更新当月最新累计到 months[current_month]
        self._sync_month_summary(current_month, current_day_data={
            ip: {"rx_tib": today_rx_tib, "tx_tib": today_tx_tib, "total_tib": today_total_tib, "name": server_name}
        })

        self.save_daily_traffic()

        return {
            "today_rx_tib": round(today_rx_tib, 3),
            "today_tx_tib": round(today_tx_tib, 3),
            "today_traffic_tib": round(today_total_tib, 3)
        }

    def _sync_month_summary(self, target_month: str, current_day_data: dict = None):
        """汇总指定月份的所有天数流量并持久化存档在 months 中，永久不丢"""
        months = self.daily_traffic.setdefault("months", {})
        month_summary = {}

        # 统计该月已归档的历史日
        for date_str, s_map in self.daily_traffic.get("history", {}).items():
            if date_str.startswith(target_month) and isinstance(s_map, dict):
                for s_ip, stat in s_map.items():
                    m_item = month_summary.setdefault(s_ip, {
                        "name": stat.get("name", s_ip),
                        "rx_tib": 0.0,
                        "tx_tib": 0.0,
                        "total_tib": 0.0,
                        "days_count": 0
                    })
                    m_item["rx_tib"] += stat.get("rx_tib", 0.0)
                    m_item["tx_tib"] += stat.get("tx_tib", 0.0)
                    m_item["total_tib"] += stat.get("total_tib", 0.0)
                    m_item["days_count"] += 1

        # 若统计的是当月，还要加上今日实时的流量
        today = format_beijing_time(fmt='%Y-%m-%d')
        if target_month == today[:7] and current_day_data:
            for s_ip, stat in current_day_data.items():
                m_item = month_summary.setdefault(s_ip, {
                    "name": stat.get("name", s_ip),
                    "rx_tib": 0.0,
                    "tx_tib": 0.0,
                    "total_tib": 0.0,
                    "days_count": 0
                })
                m_item["rx_tib"] += stat.get("rx_tib", 0.0)
                m_item["tx_tib"] += stat.get("tx_tib", 0.0)
                m_item["total_tib"] += stat.get("total_tib", 0.0)

        # 保留三位小数
        for s_ip, m_item in month_summary.items():
            m_item["rx_tib"] = round(m_item["rx_tib"], 3)
            m_item["tx_tib"] = round(m_item["tx_tib"], 3)
            m_item["total_tib"] = round(m_item["total_tib"], 3)

        if month_summary:
            months[target_month] = month_summary

    def calculate_daily_uptime_stats(self, days: int = 14) -> list:
        """
        精确计算最近 N 天（默认 14 天）中，每天被限速的具体时长与正常运行时间对比
        支持按天切分跨天限速区间，计算每日限速百分比与可用率百分比
        """
        now_dt = get_beijing_now()
        now_ts = now_dt.timestamp()

        # 收集每台服务器的所有限速区间: [(t_start, t_end)]
        server_intervals = {}
        for ip, s_info in self.throttle_history.items():
            events = s_info.get("history", [])
            intervals = []
            curr_start = None
            for ev in events:
                if ev.get("event") == "throttled":
                    curr_start = ev.get("timestamp")
                elif ev.get("event") == "unthrottled":
                    if curr_start is not None:
                        intervals.append((curr_start, ev.get("timestamp")))
                        curr_start = None
                    elif ev.get("duration_seconds") and ev.get("timestamp"):
                        intervals.append((ev.get("timestamp") - ev.get("duration_seconds"), ev.get("timestamp")))

            if s_info.get("current_throttled") and s_info.get("last_throttle_time"):
                intervals.append((s_info.get("last_throttle_time"), now_ts))

            server_intervals[ip] = {
                "name": s_info.get("server_name", ip),
                "intervals": intervals
            }

        daily_stats = []
        for i in range(days - 1, -1, -1):
            d_dt = now_dt - timedelta(days=i)
            d_str = d_dt.strftime('%Y-%m-%d')
            weekday_cn = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][d_dt.weekday()]
            
            day_start = datetime.strptime(d_str, "%Y-%m-%d").replace(tzinfo=BEIJING_TZ).timestamp()
            day_end = day_start + 86400
            effective_end = min(day_end, now_ts)
            day_span_seconds = max(1, int(effective_end - day_start))

            # 汇总当天所有服务器的最大/合并限速时长
            # 针对单机或多机分别计算
            servers_day_detail = {}
            day_max_thr_sec = 0.0
            day_total_thr_count = 0

            for ip, s_data in server_intervals.items():
                s_thr_sec = 0.0
                s_thr_count = 0
                for t_start, t_end in s_data["intervals"]:
                    overlap = max(0.0, min(t_end, effective_end) - max(t_start, day_start))
                    if overlap > 0:
                        s_thr_sec += overlap
                        if day_start <= t_start < effective_end:
                            s_thr_count += 1

                s_thr_sec = min(float(day_span_seconds), s_thr_sec)
                s_norm_sec = day_span_seconds - s_thr_sec
                s_thr_hours = s_thr_sec / 3600
                s_norm_hours = s_norm_sec / 3600

                servers_day_detail[ip] = {
                    "name": s_data["name"],
                    "throttled_seconds": int(s_thr_sec),
                    "normal_seconds": int(s_norm_sec),
                    "throttled_str": f"{s_thr_hours:.1f}小时" if s_thr_hours >= 1 else f"{int(s_thr_sec // 60)}分钟",
                    "normal_str": f"{s_norm_hours:.1f}小时" if s_norm_hours >= 1 else f"{int(s_norm_sec // 60)}分钟",
                    "throttled_pct": round((s_thr_sec / day_span_seconds) * 100, 1),
                    "normal_pct": round((s_norm_sec / day_span_seconds) * 100, 1),
                    "throttle_count": s_thr_count
                }

                if s_thr_sec > day_max_thr_sec:
                    day_max_thr_sec = s_thr_sec
                day_total_thr_count += s_thr_count

            # 若未配置服务器则默认 0
            main_thr_sec = day_max_thr_sec if server_intervals else 0.0
            main_norm_sec = day_span_seconds - main_thr_sec
            thr_hours = main_thr_sec / 3600
            norm_hours = main_norm_sec / 3600

            daily_stats.append({
                "date": d_str,
                "weekday": weekday_cn,
                "is_today": (d_str == now_dt.strftime('%Y-%m-%d')),
                "total_seconds": day_span_seconds,
                "throttled_seconds": int(main_thr_sec),
                "normal_seconds": int(main_norm_sec),
                "throttled_str": f"{thr_hours:.1f}小时" if thr_hours >= 1 else f"{int(main_thr_sec // 60)}分钟",
                "normal_str": f"{norm_hours:.1f}小时" if norm_hours >= 1 else f"{int(main_norm_sec // 60)}分钟",
                "throttled_pct": round((main_thr_sec / day_span_seconds) * 100, 1),
                "normal_pct": round((main_norm_sec / day_span_seconds) * 100, 1),
                "throttle_count": day_total_thr_count,
                "servers": servers_day_detail
            })

        return daily_stats

    def get_analytics_summary(self) -> dict:
        """
        计算全维度的流量与限速分析报表数据：
        - 每日波动（今日 vs 昨日涨跌）
        - 最近 14 天每日明细
        - 本周累计 vs 上周对比
        - 本月累计与历史月度账单（永久保存，不怕月度重置）
        - 限速时间明细与事件时间轴
        """
        now_dt = get_beijing_now()
        today_str = now_dt.strftime('%Y-%m-%d')
        yesterday_str = (now_dt - timedelta(days=1)).strftime('%Y-%m-%d')
        current_month_str = now_dt.strftime('%Y-%m')

        # 1. 今日实时数据汇总 (优先从实时缓存获取，未生成时从持久化基准计算)
        today_tx = 0.0
        today_rx = 0.0
        servers_today = {}
        if self.cached_data:
            for ip, s in self.cached_data.items():
                tx = s.get("today_tx_tib", 0.0)
                rx = s.get("today_rx_tib", 0.0)
                tot = s.get("today_traffic_tib", 0.0)
                today_tx += tx
                today_rx += rx
                servers_today[ip] = {
                    "name": s.get("server_name", ip),
                    "tx_tib": round(tx, 3),
                    "rx_tib": round(rx, 3),
                    "total_tib": round(tot, 3),
                    "disk_free_gib": s.get("free_disk_gib"),
                    "throttled": s.get("trafficThrottled", False)
                }
        else:
            for ip, s_info in self.daily_traffic.get("servers", {}).items():
                d_rx = max(0.0, s_info.get("last_rx_mib", 0) - s_info.get("base_rx_mib", 0))
                d_tx = max(0.0, s_info.get("last_tx_mib", 0) - s_info.get("base_tx_mib", 0))
                rx = self.bytes_to_tib(d_rx)
                tx = self.bytes_to_tib(d_tx)
                tot = rx + tx
                today_tx += tx
                today_rx += rx
                servers_today[ip] = {
                    "name": s_info.get("server_name", ip),
                    "tx_tib": round(tx, 3),
                    "rx_tib": round(rx, 3),
                    "total_tib": round(tot, 3),
                    "disk_free_gib": None,
                    "throttled": False
                }
        today_total = today_tx + today_rx

        # 2. 昨日数据汇总
        yesterday_map = self.daily_traffic.get("history", {}).get(yesterday_str, {})
        yesterday_tx = sum(v.get("tx_tib", 0.0) for v in yesterday_map.values()) if isinstance(yesterday_map, dict) else 0.0
        yesterday_rx = sum(v.get("rx_tib", 0.0) for v in yesterday_map.values()) if isinstance(yesterday_map, dict) else 0.0
        yesterday_total = yesterday_tx + yesterday_rx

        # 每日波动计算
        diff_total = today_total - yesterday_total
        pct_change = round((diff_total / yesterday_total * 100), 1) if yesterday_total > 0 else 0.0

        # 3. 最近 14 天每日明细数据 (包含今日)
        recent_days = []
        for i in range(13, -1, -1):
            d_dt = now_dt - timedelta(days=i)
            d_str = d_dt.strftime('%Y-%m-%d')
            weekday_cn = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][d_dt.weekday()]
            if d_str == today_str:
                d_tx, d_rx, d_tot = today_tx, today_rx, today_total
            else:
                d_map = self.daily_traffic.get("history", {}).get(d_str, {})
                d_tx = sum(v.get("tx_tib", 0.0) for v in d_map.values()) if isinstance(d_map, dict) else 0.0
                d_rx = sum(v.get("rx_tib", 0.0) for v in d_map.values()) if isinstance(d_map, dict) else 0.0
                d_tot = d_tx + d_rx

            recent_days.append({
                "date": d_str,
                "weekday": weekday_cn,
                "tx_tib": round(d_tx, 3),
                "rx_tib": round(d_rx, 3),
                "total_tib": round(d_tot, 3)
            })

        # 4. 本周与上周累计
        # 周一为一周开始
        current_weekday = now_dt.weekday()
        start_of_this_week = (now_dt - timedelta(days=current_weekday)).strftime('%Y-%m-%d')
        start_of_last_week = (now_dt - timedelta(days=current_weekday + 7)).strftime('%Y-%m-%d')
        end_of_last_week = (now_dt - timedelta(days=current_weekday + 1)).strftime('%Y-%m-%d')

        this_week_tx, this_week_rx = 0.0, 0.0
        last_week_tx, last_week_rx = 0.0, 0.0

        # 历史记录中累加
        for h_date, h_map in self.daily_traffic.get("history", {}).items():
            if not isinstance(h_map, dict):
                continue
            h_tx = sum(v.get("tx_tib", 0.0) for v in h_map.values())
            h_rx = sum(v.get("rx_tib", 0.0) for v in h_map.values())

            if start_of_this_week <= h_date <= today_str:
                this_week_tx += h_tx
                this_week_rx += h_rx
            elif start_of_last_week <= h_date <= end_of_last_week:
                last_week_tx += h_tx
                last_week_rx += h_rx

        # 加上今日实时
        this_week_tx += today_tx
        this_week_rx += today_rx
        this_week_total = this_week_tx + this_week_rx
        last_week_total = last_week_tx + last_week_rx

        # 5. 本月与历史各月份（永久归档，机房重置也不丢失）
        months_dict = dict(self.daily_traffic.get("months", {}))
        # 确保当月包含最新今日实时
        this_month_data = months_dict.get(current_month_str, {})
        this_month_tx = sum(v.get("tx_tib", 0.0) for v in this_month_data.values()) if isinstance(this_month_data, dict) else today_tx
        this_month_rx = sum(v.get("rx_tib", 0.0) for v in this_month_data.values()) if isinstance(this_month_data, dict) else today_rx
        this_month_total = this_month_tx + this_month_rx

        monthly_history = []
        for m_key in sorted(months_dict.keys(), reverse=True):
            m_servers = months_dict[m_key]
            m_tx = sum(v.get("tx_tib", 0.0) for v in m_servers.values()) if isinstance(m_servers, dict) else 0.0
            m_rx = sum(v.get("rx_tib", 0.0) for v in m_servers.values()) if isinstance(m_servers, dict) else 0.0
            monthly_history.append({
                "month": m_key,
                "tx_tib": round(m_tx, 3),
                "rx_tib": round(m_rx, 3),
                "total_tib": round(m_tx + m_rx, 3),
                "is_current": (m_key == current_month_str)
            })

        # 6. 限速时间与事件时间轴
        throttle_summary = {
            "is_any_throttled": False,
            "servers": {}
        }
        all_throttle_events = []

        now_ts = now_dt.timestamp()
        for ip, t_info in self.throttle_history.items():
            is_thr = t_info.get("current_throttled", False)
            if is_thr:
                throttle_summary["is_any_throttled"] = True

            curr_dur = 0
            if is_thr and t_info.get("last_throttle_time"):
                curr_dur = int(now_ts - t_info["last_throttle_time"])

            s_name = t_info.get("server_name", ip)
            throttle_summary["servers"][ip] = {
                "name": s_name,
                "current_throttled": is_thr,
                "current_duration": self.format_duration(curr_dur) if curr_dur > 0 else "0秒",
                "throttle_count": t_info.get("throttle_count", 0),
                "total_throttled_time": self.format_duration(t_info.get("total_throttled_seconds", 0))
            }

            # 收集事件时间线
            events = t_info.get("history", [])
            for ev in events:
                all_throttle_events.append({
                    "ip": ip,
                    "server_name": s_name,
                    "event": ev.get("event"),
                    "datetime": ev.get("datetime"),
                    "timestamp": ev.get("timestamp", 0),
                    "duration_seconds": ev.get("duration_seconds", 0),
                    "duration_str": self.format_duration(ev.get("duration_seconds", 0)) if ev.get("duration_seconds") else None
                })

        # 按时间倒序排序限速事件
        all_throttle_events.sort(key=lambda x: x.get("timestamp", 0), reverse=True)

        # 计算最近 14 天每日限速与正常运行时间对比数据
        daily_uptime = self.calculate_daily_uptime_stats(days=14)

        return {
            "success": True,
            "generated_at": format_beijing_time(),
            "today": {
                "date": today_str,
                "tx_tib": round(today_tx, 3),
                "rx_tib": round(today_rx, 3),
                "total_tib": round(today_total, 3),
                "diff_vs_yesterday": round(diff_total, 3),
                "pct_vs_yesterday": pct_change,
                "servers": servers_today
            },
            "yesterday": {
                "date": yesterday_str,
                "tx_tib": round(yesterday_tx, 3),
                "rx_tib": round(yesterday_rx, 3),
                "total_tib": round(yesterday_total, 3)
            },
            "this_week": {
                "tx_tib": round(this_week_tx, 3),
                "rx_tib": round(this_week_rx, 3),
                "total_tib": round(this_week_total, 3)
            },
            "last_week": {
                "tx_tib": round(last_week_tx, 3),
                "rx_tib": round(last_week_rx, 3),
                "total_tib": round(last_week_total, 3)
            },
            "this_month": {
                "month": current_month_str,
                "tx_tib": round(this_month_tx, 3),
                "rx_tib": round(this_month_rx, 3),
                "total_tib": round(this_month_total, 3)
            },
            "recent_days": recent_days,
            "monthly_history": monthly_history,
            "throttle_summary": throttle_summary,
            "throttle_timeline": all_throttle_events[:30],  # 最新30条限速事件
            "daily_uptime": daily_uptime  # 每日限速时长与正常时间对比
        }

    def get_full_analytics_report_text(self) -> str:
        """
        生成权威完整的 Netcup 官方流量与限速多维报表文本
        支持 Telegram Bot (/report 或 /daily) 直接发送
        """
        data = self.get_analytics_summary()
        today = data["today"]
        yesterday = data["yesterday"]
        week = data["this_week"]
        month = data["this_month"]
        thr_sum = data["throttle_summary"]
        timeline = data["throttle_timeline"]

        # 波动符号与描述
        diff_tot = today["diff_vs_yesterday"]
        pct = today["pct_vs_yesterday"]
        if diff_tot > 0:
            fluctuation_str = f"+{diff_tot:.3f} TiB (+{pct}%) 📈"
        elif diff_tot < 0:
            fluctuation_str = f"{diff_tot:.3f} TiB ({pct}%) 📉"
        else:
            fluctuation_str = "持平 ➖"

        lines = [
            f"<b>📊 Netcup 官方流量与运行分析报表</b>\n",
            f"📅 <b>今日流量 ({today['date']})</b>",
            f"• 今日总计: <b><code>{today['total_tib']:.3f} TiB</code></b>",
            f"• 较昨日波动: <code>{fluctuation_str}</code>",
            f"• 今日上传: <code>{today['tx_tib']:.3f} TiB</code>",
            f"• 今日下载: <code>{today['rx_tib']:.3f} TiB</code>\n",
            f"📆 <b>多周期流量统计</b>",
            f"• 昨日流量: <code>{yesterday['total_tib']:.3f} TiB</code> (↑{yesterday['tx_tib']:.3f} / ↓{yesterday['rx_tib']:.3f})",
            f"• 本周累计: <b><code>{week['total_tib']:.3f} TiB</code></b> (↑{week['tx_tib']:.3f} / ↓{week['rx_tib']:.3f})",
            f"• <b>本月累计 ({month['month']})</b>: <b><code>{month['total_tib']:.3f} TiB</code></b> (↑{month['tx_tib']:.3f} / ↓{month['rx_tib']:.3f})\n"
        ]

        # 历史月份简览 (若有多月)
        hist_months = [m for m in data["monthly_history"] if not m.get("is_current")]
        if hist_months:
            lines.append("🗓️ <b>历史月份账单 (永久归档)</b>")
            for m in hist_months[:3]:
                lines.append(f"• {m['month']}: <code>{m['total_tib']:.3f} TiB</code> (↑{m['tx_tib']:.3f} / ↓{m['rx_tib']:.3f})")
            lines.append("")

        # 服务器单机明细与磁盘
        lines.append("🖥️ <b>服务器单机明细</b>")
        for ip, s in today["servers"].items():
            status_ico = "🔴" if s["throttled"] else "🟢"
            disk_str = f"{s['disk_free_gib']:.1f} GiB" if s["disk_free_gib"] is not None else "检测中"
            lines.append(f"{status_ico} <b>{s['name']}</b> (<code>{ip}</code>)")
            lines.append(f"   • 今日: <code>{s['total_tib']:.3f} TiB</code> (↑{s['tx_tib']:.3f} / ↓{s['rx_tib']:.3f})")
            lines.append(f"   • 磁盘可用: <code>{disk_str}</code>")
        lines.append("")

        # 限速时间与事件
        lines.append("🛑 <b>限速事件与时间记录</b>")
        for ip, ts in thr_sum["servers"].items():
            stat_desc = f"限速中 (已持续 {ts['current_duration']})" if ts["current_throttled"] else "正常"
            lines.append(f"• {ts['name']}: {stat_desc}，累计限速: <code>{ts['total_throttled_time']}</code> ({ts['throttle_count']}次)")

        # 每日限速时长简述 (今日 & 昨日)
        uptime_list = data.get("daily_uptime", [])
        if uptime_list:
            today_u = uptime_list[-1]
            lines.append(f"\n<b>⏱️ 每日限速时间统计:</b>")
            lines.append(f"• 今日被限速: <b><code>{today_u['throttled_str']}</code></b> (正常: {today_u['normal_str']}，正常率: {today_u['normal_pct']}%)")
            if len(uptime_list) >= 2:
                yest_u = uptime_list[-2]
                lines.append(f"• 昨日被限速: <code>{yest_u['throttled_str']}</code> (正常: {yest_u['normal_str']}，正常率: {yest_u['normal_pct']}%)")

        # 列出最近 3 次具体的被限速时间段
        recent_thrs = []
        for ev in timeline:
            if ev.get("event") == "unthrottled":
                dur = ev.get("duration_str", "未知")
                recent_thrs.append(f"• {ev.get('datetime')}: 解除限速 (持续 {dur})")
            elif ev.get("event") == "throttled":
                recent_thrs.append(f"• {ev.get('datetime')}: 触发限速 ⚠️")
            if len(recent_thrs) >= 3:
                break

        if recent_thrs:
            lines.append("\n<b>📋 最近限速事件:</b>")
            lines.extend(recent_thrs)

        lines.append(f"\n<i>⏰ 统计时间: {data['generated_at']}</i>")
        return "\n".join(lines)

    def get_throttle_timeline_text(self) -> str:
        """生成详细的被限速时间列表文本（供 Telegram /history 命令）"""
        data = self.get_analytics_summary()
        timeline = data["throttle_timeline"]
        thr_sum = data["throttle_summary"]

        lines = ["<b>🛑 服务器被限速时间记录明细</b>\n"]
        for ip, ts in thr_sum["servers"].items():
            cur_tag = f"🔴 <b>限速中 (已持续 {ts['current_duration']})</b>" if ts["current_throttled"] else "🟢 正常运行中"
            lines.append(f"🖥️ <b>{ts['name']}</b> (<code>{ip}</code>)")
            lines.append(f"• 当前状态: {cur_tag}")
            lines.append(f"• 累计限速时长: <code>{ts['total_throttled_time']}</code>")
            lines.append(f"• 累计限速次数: <b>{ts['throttle_count']}</b> 次\n")

        # 增加最近 7 天每日限速走势列表
        uptime_list = data.get("daily_uptime", [])
        if uptime_list:
            lines.append("<b>📊 最近 7 天每日被限速时长走势:</b>")
            for u in uptime_list[-7:]:
                tag = "(今天)" if u.get("is_today") else f"({u.get('weekday')})"
                lines.append(f"• <code>{u['date']}</code> {tag}: 🔴 限速 <b>{u['throttled_str']}</b> | 🟢 正常 {u['normal_str']} (正常率: {u['normal_pct']}%)")
            lines.append("")

        if not timeline:
            lines.append("✨ 暂无任何限速历史记录，服务运行良好！")
        else:
            lines.append("<b>📋 详细事件时间线 (最近 15 次):</b>")
            for idx, ev in enumerate(timeline[:15], 1):
                dt = ev.get("datetime", "未知")
                ev_type = ev.get("event")
                if ev_type == "throttled":
                    lines.append(f"{idx}. 🔴 <code>{dt}</code> 触发限速")
                elif ev_type == "unthrottled":
                    dur = ev.get("duration_str", "未知")
                    lines.append(f"{idx}. 🟢 <code>{dt}</code> 限速解除 (持续时长: <b>{dur}</b>)")

        lines.append(f"\n<i>⏰ 查询时间: {format_beijing_time()}</i>")
        return "\n".join(lines)

    def send_daily_traffic_report(self):
        """发送每日流量与运行分析报告到 Telegram"""
        if not self.telegram_notifier:
            return
        try:
            report_text = self.get_full_analytics_report_text()
            self.telegram_notifier.send_message(report_text)
            logger.info("[调度器] 每日流量统计与运行报告已推送至 Telegram")
        except Exception as e:
            logger.error(f"[调度器] 推送每日流量报告失败: {e}")

    def save_refreshed_tokens(self, account_id: str, access_token: str, refresh_token: str):
        """当 Token 刷新成功时，持久化写回 config.json 保证长期稳定运行"""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                cfg = json.load(f)

            updated = False
            for acc in cfg.get('rest_accounts', []):
                if str(acc.get('account_id')) == str(account_id):
                    acc['access_token'] = access_token
                    acc['refresh_token'] = refresh_token
                    updated = True
                    break

            if updated:
                # Docker bind mount 单文件不支持 os.replace，直接写回并执行 fsync 落盘
                with open(self.config_file, 'w', encoding='utf-8') as f:
                    json.dump(cfg, f, indent=2, ensure_ascii=False)
                    f.flush()
                    os.fsync(f.fileno())
                logger.info(f"[{account_id}] 新 Token 已持久化保存至 config.json")
        except Exception as e:
            logger.error(f"持久化 Token 出错: {e}")



    def get_server_disk_free(self, ip: str) -> float | None:
        """
        获取指定服务器的磁盘剩余空间 (GiB)
        优先通过 Linux 原生 df 命令获取物理真实剩余，彻底不依赖任何第三方
        """
        # 途径 1: 优先通过 SSH / 本地执行 Linux 原生 df 获取物理真实空间（最权威准确）
        try:
            ssh_host = self.ssh_config.get('host', '')
            if self._is_localhost(ssh_host) and (ip in ['127.0.0.1', 'localhost'] or not ssh_host):
                res = subprocess.run(["df", "-PB1", "/"], capture_output=True, text=True, timeout=10)
                if res.returncode == 0:
                    lines = res.stdout.strip().splitlines()
                    if len(lines) >= 2:
                        avail_bytes = int(lines[1].split()[3])
                        return round(avail_bytes / (1024 ** 3), 2)
            elif ssh_host and (ssh_host == ip or not self.qb_rss):
                ssh_cmd = [
                    'sshpass', '-p', self.ssh_config.get('password', ''),
                    'ssh', '-o', 'StrictHostKeyChecking=no',
                    '-o', 'ConnectTimeout=10',
                    '-p', str(self.ssh_config.get('port', 22)),
                    f"{self.ssh_config['username']}@{ssh_host}",
                    "df -PB1 /"
                ]
                res = subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=15)
                if res.returncode == 0:
                    lines = res.stdout.strip().splitlines()
                    if len(lines) >= 2:
                        avail_bytes = int(lines[1].split()[3])
                        return round(avail_bytes / (1024 ** 3), 2)
        except Exception as e:
            logger.debug(f"[磁盘检测] 通过 SSH 获取 {ip} 磁盘空间失败: {e}")

        # 途径 2: 备选通过 qBittorrent API 获取
        if self.qb_rss:
            try:
                url, username, password = self.qb_rss.get_user_info(ip)
                if url and username and password:
                    from qbittorrentapi import Client
                    client = Client(host=url, username=username, password=password, VERIFY_WEBUI_CERTIFICATE=False)
                    client.auth_log_in()
                    maindata = client.sync.maindata()
                    free_bytes = maindata.get("server_state", {}).get("free_space_on_disk")
                    if free_bytes is not None:
                        return round(free_bytes / (1024 ** 3), 2)
            except Exception as e:
                logger.debug(f"[磁盘检测] 通过 qB API 获取 {ip} 磁盘空间失败: {e}")

        return None

    def check_disk_space_alarms(self, ip: str, server_name: str, free_gib: float | None):
        """
        检查磁盘剩余空间并在低于 5GB / 1GB / 0GB 时发送 Telegram 智能阶梯告警
        具备：恶化立即报警、同级别冷却防刷屏、空间恢复自动提醒通知
        """
        if not self.disk_alarm_enabled or not self.telegram_notifier or free_gib is None:
            return

        now = time.time()
        state = self.disk_alarm_state.setdefault(ip, {
            "last_notified_level": None,
            "last_notified_time": 0
        })

        last_level = state["last_notified_level"]
        last_time = state["last_notified_time"]

        # 判定当前所处的危险级别
        current_level = None
        alarm_title = ""
        alarm_desc = ""
        level_severity = 0  # 0: 正常, 1: 5GB, 2: 1GB, 3: EMPTY

        if free_gib <= 0.1:
            current_level = "EMPTY"
            level_severity = 3
            alarm_title = "🛑 <b>【紧急告警】qBittorrent 磁盘已满 (0GB 剩余)！</b>"
            alarm_desc = "磁盘空间已完全耗尽！qB 所有下载将强制中断或产生 I/O 错误，请立即清理已完成种子！"
        elif free_gib <= 1.0:
            current_level = "1GB"
            level_severity = 2
            alarm_title = "🚨 <b>【严重告警】qBittorrent 磁盘空间不足 1GB！</b>"
            alarm_desc = "磁盘空间即将完全耗尽，处于极度危险状态，请尽快删种或腾出空间！"
        elif free_gib <= 5.0:
            current_level = "5GB"
            level_severity = 1
            alarm_title = "⚠️ <b>【空间告警】qBittorrent 磁盘剩余不足 5GB！</b>"
            alarm_desc = "磁盘剩余空间紧张，请关注任务进度，适时清理旧数据以防写满磁盘。"

        # 1. 恢复通知逻辑：若之前曾报警过，现在磁盘空间恢复到 5GB 以上
        if current_level is None:
            if last_level is not None:
                logger.info(f"[磁盘告警] {server_name}({ip}) 磁盘空间已恢复正常: {free_gib:.2f} GiB")
                try:
                    recovery_msg = (
                        f"🟢 <b>qBittorrent 磁盘空间已恢复正常</b>\n\n"
                        f"🖥️ 服务器: <code>{server_name or ip}</code>\n"
                        f"🌐 IP: <code>{ip}</code>\n"
                        f"💽 当前可用空间: <b><code>{free_gib:.2f} GiB</code></b>\n"
                        f"⏰ 时间: {format_beijing_time()}"
                    )
                    self.telegram_notifier.send_message(recovery_msg)
                except Exception as e:
                    logger.error(f"[Telegram] 发送磁盘恢复通知失败: {e}")
                state["last_notified_level"] = None
                state["last_notified_time"] = now
            return

        # 2. 告警发送判断：恶化无视冷却直接报警；同级别遵守冷却时间防刷屏
        level_order = {"5GB": 1, "1GB": 2, "EMPTY": 3}
        last_severity = level_order.get(last_level, 0)

        should_notify = False
        if last_level is None:
            # 首次进入危险区间
            should_notify = True
        elif level_severity > last_severity:
            # 危险级别恶化（如 5GB 恶化至 1GB 或 EMPTY），立即报警
            should_notify = True
        elif current_level == last_level:
            # 同级别，超过冷却时间才再次提醒
            if (now - last_time) >= self.disk_alarm_cooldown:
                should_notify = True

        if should_notify:
            logger.warning(f"[磁盘告警] {server_name}({ip}) 触发磁盘空间告警 [{current_level}]: 剩余 {free_gib:.2f} GiB")
            try:
                msg = (
                    f"{alarm_title}\n\n"
                    f"🖥️ 服务器: <code>{server_name or ip}</code>\n"
                    f"🌐 IP: <code>{ip}</code>\n"
                    f"💽 当前剩余空间: <b><code>{free_gib:.2f} GiB</code></b>\n\n"
                    f"💡 {alarm_desc}\n"
                    f"⏰ 时间: {format_beijing_time()}"
                )
                self.telegram_notifier.send_message(msg)
                state["last_notified_level"] = current_level
                state["last_notified_time"] = now
            except Exception as e:
                logger.error(f"[Telegram] 发送磁盘告警通知失败: {e}")

    def update_throttle_history(self, ip: str, is_throttled: bool, server_name: str = ""):
        """更新限速历史并发送 Telegram 通知"""
        now = get_beijing_now()
        timestamp = now.timestamp()
        
        if ip not in self.throttle_history:
            self.throttle_history[ip] = {
                "server_name": server_name,
                "current_throttled": is_throttled,
                "last_throttle_time": None,
                "last_unthrottle_time": None,
                "throttle_count": 0,
                "total_throttled_seconds": 0,
                "history": []
            }
            logger.info(f"[历史记录] 为 {ip} ({server_name}) 创建新的历史记录")
        
        history = self.throttle_history[ip]
        old_throttled = history.get("current_throttled")
        
        # 更新服务器名称
        if server_name and history.get("server_name") != server_name:
            history["server_name"] = server_name
        
        # 状态变化时更新历史
        if old_throttled != is_throttled:
            logger.warning(f"[历史记录] {ip} 状态变化: {old_throttled} -> {is_throttled}")
            
            service_desc = f" ({self.service_name})" if (self.throttle_strategy == 'service' and self.service_name) else ""
            delete_file_desc = '(含文件)' if self.delete_files else '(保留文件)'
            strategy_name_map = {
                'pause': '暂停所有种子',
                'delete': f'删除下载中种子{delete_file_desc}',
                'pause_resume': f'删除下载中种子{delete_file_desc}，暂停已完成种子',
                'pause_downloading': '暂停下载中种子(做种保持)',
                'delete_downloading': f'删除下载中种子{delete_file_desc}(做种保持)',
                'service': f'停止系统服务{service_desc}'
            }
            strategy_name = strategy_name_map.get(self.throttle_strategy, self.throttle_strategy or '未知策略')
            
            # 添加排除分类信息
            exclude_info = f" (排除: {', '.join(self.exclude_categories)})" if self.exclude_categories else ""
            
            if is_throttled:
                # 被限速
                history["last_throttle_time"] = timestamp
                history["throttle_count"] += 1
                history["history"].append({
                    "event": "throttled",
                    "timestamp": timestamp,
                    "datetime": now.strftime('%Y-%m-%d %H:%M:%S')
                })
                
                logger.warning(f"[限速] {server_name or ip} 已被限速 (第 {history['throttle_count']} 次)")
                
                # 发送 Telegram 通知
                if self.telegram_notifier:
                    try:
                        message = (
                            f"🔴 <b>限速警告</b>\n\n"
                            f"服务器: <code>{server_name or ip}</code>\n"
                            f"IP: <code>{ip}</code>\n\n"
                            f"已被限速,执行策略: <b>{strategy_name}</b>{exclude_info}\n"
                            f"限速次数: 第 {history['throttle_count']} 次\n"
                            f"时间: {now.strftime('%Y-%m-%d %H:%M:%S')}"
                        )
                        self.telegram_notifier.send_message(message)
                        logger.info(f"[Telegram] 限速通知已发送: {ip}")
                    except Exception as e:
                        logger.error(f"[Telegram] 发送限速通知失败: {e}")
                    
            else:
                # 解除限速
                history["last_unthrottle_time"] = timestamp
                
                # 计算本次限速持续时间
                duration_str = "未知"
                if history["last_throttle_time"]:
                    throttled_duration = int(timestamp - history["last_throttle_time"])
                    history["total_throttled_seconds"] += throttled_duration
                    duration_str = self.format_duration(throttled_duration)
                    
                    history["history"].append({
                        "event": "unthrottled",
                        "timestamp": timestamp,
                        "datetime": now.strftime('%Y-%m-%d %H:%M:%S'),
                        "duration_seconds": throttled_duration
                    })
                    
                    logger.info(f"[解除限速] {server_name or ip} 限速已解除,持续了 {duration_str}")
                    
                    # 恢复时的策略动作描述
                    recovery_strategy_name = {
                        'pause': '恢复所有种子下载',
                        'delete': '无需恢复(已删除下载中)',
                        'pause_resume': '恢复已完成种子做种',
                        'pause_downloading': '恢复下载中种子',
                        'delete_downloading': '无需恢复(做种保持未停)',
                        'service': f'启动系统服务{service_desc}'
                    }.get(self.throttle_strategy, strategy_name)

                    # 发送 Telegram 通知
                    if self.telegram_notifier:
                        try:
                            message = (
                                f"🟢 <b>限速解除</b>\n\n"
                                f"服务器: <code>{server_name or ip}</code>\n"
                                f"IP: <code>{ip}</code>\n\n"
                                f"限速已解除,执行动作: <b>{recovery_strategy_name}</b>{exclude_info}\n"
                                f"本次限速时长: <code>{duration_str}</code>\n"
                                f"累计限速: <code>{self.format_duration(history['total_throttled_seconds'])}</code>\n"
                                f"时间: {now.strftime('%Y-%m-%d %H:%M:%S')}"
                            )
                            self.telegram_notifier.send_message(message)
                            logger.info(f"[Telegram] 解除限速通知已发送: {ip}")
                        except Exception as e:
                            logger.error(f"[Telegram] 发送解除限速通知失败: {e}")
            
            history["current_throttled"] = is_throttled
            
            # 只保留最近100条历史记录
            if len(history["history"]) > 100:
                history["history"] = history["history"][-100:]
            
            self.save_throttle_history()


    @staticmethod
    def format_duration(seconds: int) -> str:
        """格式化时长"""
        if seconds < 60:
            return f"{seconds}秒"
        elif seconds < 3600:
            minutes = seconds // 60
            secs = seconds % 60
            return f"{minutes}分{secs}秒"
        else:
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            return f"{hours}小时{minutes}分"


    def calculate_availability(self, ip: str) -> dict:
        """计算可用率统计"""
        if ip not in self.throttle_history:
            return {
                "throttle_count": 0,
                "total_throttled_time": "0秒",
                "last_throttle_time": None,
                "last_unthrottle_time": None,
                "current_throttle_duration": "0秒",
                "history": []
            }
        
        history = self.throttle_history[ip]
        now = time.time()
        
        # 计算当前限速持续时间
        current_throttle_duration = 0
        if history.get("current_throttled") and history.get("last_throttle_time"):
            current_throttle_duration = int(now - history["last_throttle_time"])
        
        # 计算总限速时间(不包括当前正在进行的限速)
        total_throttled = history.get("total_throttled_seconds", 0)
        
        # 格式化时长为小时
        def format_hours(seconds):
            hours = seconds / 3600
            if hours >= 1:
                return f"{hours:.1f}小时"
            else:
                minutes = seconds / 60
                return f"{minutes:.0f}分钟"
        
        return {
            "throttle_count": history.get("throttle_count", 0),
            "total_throttled_time": format_hours(total_throttled),
            "last_throttle_time": beijing_from_timestamp(history.get("last_throttle_time")),
            "last_unthrottle_time": beijing_from_timestamp(history.get("last_unthrottle_time")),
            "current_throttle_duration": format_hours(current_throttle_duration) if current_throttle_duration > 0 else "0分钟",
            "history": history.get("history", [])
        }

    def bytes_to_tib(self, mib_value: float) -> float:
        """将 MiB 转换为 TiB"""
        return mib_value / (1024 * 1024)

    def setup_routes(self):
        """设置Flask路由"""

        @self.app.route(self.webhook_path, methods=['GET', 'POST'])
        def webhook():
            try:
                ipv4_ip = request.args.get('ipv4IP') or (request.json.get('ipv4IP') if request.is_json else None)
                logger.info(f"[Webhook] 收到外部超限通知触发, 目标IP: {ipv4_ip or '全部'}")

                # 异步立即执行一次全局数据刷新与限速检测（秒级响应）
                threading.Thread(target=self.update_cached_data, daemon=True).start()

                if ipv4_ip and ipv4_ip in self.cached_data:
                    return jsonify({
                        "success": True,
                        "message": "Webhook 已触发即时检查与状态刷新",
                        "server": self.cached_data[ipv4_ip]
                    })
                return jsonify({
                    "success": True,
                    "message": "Webhook 已触发全局即时检查与状态刷新"
                })

            except Exception as e:
                logger.error(f"处理webhook请求时发生错误: {e}")
                return jsonify({"error": "内部服务器错误"}), 500

        @self.app.route('/api/status', methods=['GET'])
        def api_status():
            """返回所有服务器状态"""
            try:
                return jsonify(self._get_status_data())
            except Exception as e:
                logger.error(f"获取状态时发生错误: {e}")
                return jsonify({"success": False, "error": str(e)}), 500

        @self.app.route('/api/analytics', methods=['GET'])
        def api_analytics():
            """返回多维流量统计、每日波动、每月历史与限速时间线数据"""
            try:
                return jsonify(self.get_analytics_summary())
            except Exception as e:
                logger.error(f"获取分析报表失败: {e}")
                return jsonify({"success": False, "error": str(e)}), 500

        @self.app.route('/api/vertex/report', methods=['GET'])
        def vertex_report():
            """获取 Vertex 运行信息报告并发送到 Telegram"""
            try:
                # 检查是否启用 Telegram
                if not self.telegram_notifier:
                    return jsonify({
                        "success": False,
                        "error": "Telegram 通知未启用"
                    }), 400
                
                # 获取 Vertex 运行信息
                run_info = self.fetch_vertex_run_info()
                
                if not run_info.get('success'):
                    return jsonify({
                        "success": False,
                        "error": run_info.get('error', '获取运行信息失败')
                    }), 500
                
                # 发送到 Telegram
                send_success = self.telegram_notifier.send_vertex_report(run_info)
                
                if send_success:
                    return jsonify({
                        "success": True,
                        "message": "报告已发送到 Telegram",
                        "data": run_info.get('data', {})
                    })
                else:
                    return jsonify({
                        "success": False,
                        "error": "发送 Telegram 消息失败",
                        "data": run_info.get('data', {})
                    }), 500
                
            except Exception as e:
                logger.error(f"[/api/vertex/report] 获取报告时发生错误: {e}")
                return jsonify({
                    "success": False,
                    "error": str(e)
                }), 500

        @self.app.route('/health', methods=['GET'])
        def health():
            return jsonify({
                "status": "ok",
                "timestamp": datetime.now().isoformat(),
                "total_servers": len(self.cached_data),
                "telegram_enabled": self.telegram_enabled,
                "throttle_strategy": self.throttle_strategy,
                "exclude_categories": self.exclude_categories
            })

        @self.app.route('/', methods=['GET'])
        def dashboard():
            """Web监控面板"""
            return self.render_dashboard()

    def render_dashboard(self):
        """渲染 Web 监控面板"""
        try:
            html_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dashboard.html')

            if os.path.exists(html_file):
                with open(html_file, 'r', encoding='utf-8') as f:
                    html_content = f.read()
                logger.info(f"成功加载外部 HTML 文件: {html_file}")
                return html_content
            else:
                logger.warning(f"未找到外部 HTML 文件: {html_file}, 使用内置版本")
                return self._get_builtin_html()

        except Exception as e:
            logger.error(f"读取 HTML 文件时出错: {e}, 使用内置版本")
            return self._get_builtin_html()

    def _get_builtin_html(self):
        """内置的简化版 HTML"""
        return """
<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>监控面板</title></head>
<body><h1>监控面板</h1><p>请检查 dashboard.html 文件是否存在</p></body>
</html>
        """

    def get_server_info_from_account(self, account_config: dict) -> dict:
        """从单个账户获取所有服务器信息"""
        server_data = {}

        try:
            api = NetcupAPI(
                account_id=account_config['account_id'],
                access_token=account_config['access_token'],
                refresh_token=account_config['refresh_token']
            )

            servers_list = api.get_servers()

            if not servers_list:
                logger.warning(f"[{account_config['account_id']}] 未找到任何服务器")
                return server_data

            logger.info(f"[{account_config['account_id']}] 发现 {len(servers_list)} 台服务器")

            for server in servers_list:
                server_id = server.get('vserverId') or server.get('id')
                server_name = server.get('hostname') or server.get('name') or server_id

                if not server_id:
                    continue

                try:
                    # 单次请求获取详细信息（包含 IPv4、状态、流量接口和限速标志，避免重复发 3 次请求）
                    server_details = api.get_server_details(str(server_id), load_live_info=True)
                    if not server_details:
                        continue

                    ipv4, status, is_throttled, traffic_info = api.parse_server_details(server_details)
                    if not ipv4 or is_throttled is None:
                        continue

                    # 转换流量为 TiB (从 MiB)
                    rx_mib = traffic_info.get('total_rx_mib', 0)
                    tx_mib = traffic_info.get('total_tx_mib', 0)
                    rx_tib = self.bytes_to_tib(rx_mib)
                    tx_tib = self.bytes_to_tib(tx_mib)
                    total_tib = rx_tib + tx_tib

                    # 每日流量统计（计算今日上传、今日下载及今日总计）
                    daily_stat = self.update_daily_traffic(ipv4, rx_mib, tx_mib, server_name)

                    # 检测磁盘剩余空间与智能阶梯告警
                    free_disk_gib = self.get_server_disk_free(ipv4)
                    self.check_disk_space_alarms(ipv4, server_name, free_disk_gib)

                    server_data[ipv4] = {
                        "ipv4IP": ipv4,
                        "trafficThrottled": is_throttled,
                        "status": status or "UNKNOWN",
                        "traffic_tib": round(total_tib, 3),
                        "rx_tib": round(rx_tib, 3),
                        "tx_tib": round(tx_tib, 3),
                        "today_traffic_tib": daily_stat.get("today_traffic_tib", 0.0),
                        "today_rx_tib": daily_stat.get("today_rx_tib", 0.0),
                        "today_tx_tib": daily_stat.get("today_tx_tib", 0.0),
                        "free_disk_gib": free_disk_gib,
                        "account_id": account_config['account_id'],
                        "server_id": server_id,
                        "server_name": server_name,
                        "last_check_time": format_beijing_time()
                    }

                    disk_log_str = f", 磁盘剩余: {free_disk_gib} GiB" if free_disk_gib is not None else ""
                    logger.info(
                        f"[{account_config['account_id']}] {server_name}({ipv4}) - "
                        f"限速: {is_throttled}, 总量: {round(total_tib, 3)} TiB, "
                        f"今日: {daily_stat.get('today_traffic_tib', 0.0)} TiB "
                        f"(↑{daily_stat.get('today_tx_tib', 0.0)} / ↓{daily_stat.get('today_rx_tib', 0.0)})"
                        f"{disk_log_str}"
                    )

                except Exception as e:
                    logger.error(f"获取服务器 {server_name} 信息失败: {e}")
                    continue

            # 检测 Token 刷新并在变动时持久化保存回 config.json
            if api.access_token != account_config.get('access_token') or api.refresh_token != account_config.get('refresh_token'):
                account_config['access_token'] = api.access_token
                account_config['refresh_token'] = api.refresh_token
                self.save_refreshed_tokens(account_config['account_id'], api.access_token, api.refresh_token)

        except Exception as e:
            logger.error(f"从账户获取服务器信息失败: {e}")

        return server_data

    def enable_downloader(self, ip: str):
        """启用下载器并根据策略恢复种子状态"""
        # 1. 启用 Vertex 下载器（受 vertex_manage_downloader 开关控制，默认不操作 VT）
        if self.qb_rss and self.vertex_manage_downloader:
            try:
                r = self.qb_rss.enable_downloader(ip)
                logger.info(f"[Vertex] 启用下载器({ip}): {r}")
            except Exception as e:
                logger.error(f"[Vertex] 启用下载器({ip})失败: {e}")

        # 2. 如果是 service 策略，直接启动服务
        if self.throttle_strategy == 'service':
            logger.info(f"[服务控制] 启用 {ip} 的下载器，启动服务 {self.service_name}")
            self._service_control('start')
            return

        # 3. 根据策略处理 qBittorrent 中的种子
        if self.throttle_strategy == 'pause_resume':
            # pause_resume 策略: 恢复所有暂停的种子(排除指定分类)
            if self.qb_rss:
                try:
                    url, username, password = self.qb_rss.get_user_info(ip)
                    if url and username and password:
                        qb = QBittorrentClient(url, username, password)
                        qb.resume_all(exclude_categories=self.exclude_categories)
                        exclude_msg = f" (排除分类: {', '.join(self.exclude_categories)})" if self.exclude_categories else ""
                        logger.info(f"[qBittorrent] 已恢复 {ip} 的种子下载 (pause_resume策略){exclude_msg}")
                    else:
                        logger.warning(f"[qBittorrent] 无法获取 {ip} 的连接信息")
                except Exception as e:
                    logger.error(f"[qBittorrent] 恢复 {ip} 种子失败: {e}")
        elif self.throttle_strategy == 'pause':
            # pause 策略: 也恢复种子(因为之前只是暂停)
            if self.qb_rss:
                try:
                    url, username, password = self.qb_rss.get_user_info(ip)
                    if url and username and password:
                        qb = QBittorrentClient(url, username, password)
                        qb.resume_all(exclude_categories=self.exclude_categories)
                        exclude_msg = f" (排除分类: {', '.join(self.exclude_categories)})" if self.exclude_categories else ""
                        logger.info(f"[qBittorrent] 已恢复 {ip} 的种子下载 (pause策略){exclude_msg}")
                    else:
                        logger.warning(f"[qBittorrent] 无法获取 {ip} 的连接信息")
                except Exception as e:
                    logger.error(f"[qBittorrent] 恢复 {ip} 种子失败: {e}")
        elif self.throttle_strategy == 'delete':
            # delete 策略: 不需要恢复(种子已被删除)
            logger.info(f"[qBittorrent] {ip} 使用delete策略,无需恢复种子")
        elif self.throttle_strategy == 'pause_downloading':
            # pause_downloading 策略: 恢复之前暂停的下载中种子
            if self.qb_rss:
                try:
                    url, username, password = self.qb_rss.get_user_info(ip)
                    if url and username and password:
                        qb = QBittorrentClient(url, username, password)
                        qb.resume_all(exclude_categories=self.exclude_categories)
                        exclude_msg = f" (排除分类: {', '.join(self.exclude_categories)})" if self.exclude_categories else ""
                        logger.info(f"[qBittorrent] 已恢复 {ip} 的种子下载 (pause_downloading策略){exclude_msg}")
                    else:
                        logger.warning(f"[qBittorrent] 无法获取 {ip} 的连接信息")
                except Exception as e:
                    logger.error(f"[qBittorrent] 恢复 {ip} 种子失败: {e}")
        elif self.throttle_strategy == 'delete_downloading':
            # delete_downloading 策略: 不需要恢复(下载中种子已被删除,已完成种子从未暂停)
            logger.info(f"[qBittorrent] {ip} 使用delete_downloading策略,无需恢复种子")


    def disable_downloader(self, ip: str, url: str = None, username: str = None, password: str = None):
        """禁用下载器并根据策略处理种子"""
        # 1. 禁用 Vertex 下载器（受 vertex_manage_downloader 开关控制，默认不操作 VT）
        if self.qb_rss and self.vertex_manage_downloader:
            try:
                r = self.qb_rss.pause_downloader(ip)
                logger.info(f"[Vertex] 暂停下载器({ip}): {r}")
            except Exception as e:
                logger.error(f"[Vertex] 暂停下载器({ip})失败: {e}")

        # 2. 如果是 service 策略，直接停止服务，无需 qB WebUI 凭据
        if self.throttle_strategy == 'service':
            exclude_msg = f" (排除分类: {', '.join(self.exclude_categories)})" if self.exclude_categories else ""
            logger.info(f"[服务控制] 禁用 {ip} 的下载器，停止服务 {self.service_name}{exclude_msg}")
            self._service_control('stop')
            return

        # 3. 获取 qBittorrent 连接信息（仅针对需要操作种子的策略）
        if not (url and username and password) and self.qb_rss:
            try:
                url, username, password = self.qb_rss.get_user_info(ip)
            except Exception as e:
                logger.error(f"[qBittorrent] 获取 {ip} 连接信息失败: {e}")
                return

        if not (url and username and password):
            logger.warning(f"[qBittorrent] 无法获取 {ip} 的连接信息,跳过种子处理")
            return

        # 4. 根据策略处理种子
        try:
            qb = QBittorrentClient(url, username, password)
            
            exclude_msg = f" (排除分类: {', '.join(self.exclude_categories)})" if self.exclude_categories else ""
            
            if self.throttle_strategy == 'pause':
                # 策略1: 汇报后暂停(不删除)
                qb.pause_all_with_reannounce(exclude_categories=self.exclude_categories)
                logger.info(f"[qBittorrent] 已暂停 {ip} 的种子(保留文件) - pause策略{exclude_msg}")
                
            elif self.throttle_strategy == 'delete':
                # 策略2: 删除下载中的种子，保留已完成的种子
                qb.smart_throttle_action(
                    strategy='delete',
                    delete_files=self.delete_files,
                    exclude_categories=self.exclude_categories
                )
                action = "删除下载中种子(含文件)" if self.delete_files else "删除下载中种子(保留文件)"
                logger.info(f"[qBittorrent] {action}，保留已完成种子 - {ip} - delete策略{exclude_msg}")
                
            elif self.throttle_strategy == 'pause_resume':
                # 策略3: 删除下载中的种子，暂停已完成的种子
                qb.smart_throttle_action(
                    strategy='pause_resume',
                    delete_files=self.delete_files,
                    exclude_categories=self.exclude_categories
                )
                logger.info(f"[qBittorrent] 删除下载中种子，暂停已完成种子 - {ip} - pause_resume策略{exclude_msg}")
                
            elif self.throttle_strategy == 'pause_downloading':
                # 策略4: 暂停下载中的种子，已完成的不动（继续做种）
                qb.smart_throttle_action(
                    strategy='pause_downloading',
                    delete_files=self.delete_files,
                    exclude_categories=self.exclude_categories
                )
                logger.info(f"[qBittorrent] 暂停下载中种子，已完成继续做种 - {ip} - pause_downloading策略{exclude_msg}")
                
            elif self.throttle_strategy == 'delete_downloading':
                # 策略5: 删除下载中的种子，已完成的不动（继续做种）
                qb.smart_throttle_action(
                    strategy='delete_downloading',
                    delete_files=self.delete_files,
                    exclude_categories=self.exclude_categories
                )
                action = "删除下载中种子(含文件)" if self.delete_files else "删除下载中种子(保留文件)"
                logger.info(f"[qBittorrent] {action}，已完成继续做种 - {ip} - delete_downloading策略{exclude_msg}")
                
            else:
                logger.warning(f"[qBittorrent] 未知策略: {self.throttle_strategy},默认执行暂停")
                qb.pause_all_with_reannounce(exclude_categories=self.exclude_categories)
                
        except Exception as e:
            logger.error(f"[qBittorrent] 处理 {ip} 种子失败: {e}")

        
    def update_cached_data(self):
        """更新缓存的数据"""
        try:
            new_data = {}

            for account in self.accounts:
                if not all(k in account for k in ['account_id', 'access_token', 'refresh_token']):
                    continue

                account_data = self.get_server_info_from_account(account)
                new_data.update(account_data)

            # 核心数据先更新至缓存，确保 API 和面板展示不受后续联动影响
            old_data = self.cached_data
            self.cached_data = new_data
            logger.info(f"数据更新成功,共 {len(self.cached_data)} 台服务器")

            # 对比新旧状态并执行联动
            for ip, payload in new_data.items():
                try:
                    new_throttled = payload.get("trafficThrottled")
                    old_throttled = old_data.get(ip, {}).get("trafficThrottled")
                    server_name = payload.get("server_name", "")

                    if old_throttled is None:
                        # 首次发现
                        logger.info(f"[首次] {ip} ({server_name}), 限速={new_throttled}")
                        self.update_throttle_history(ip, new_throttled, server_name)

                        try:
                            if new_throttled is False:
                                self.enable_downloader(ip)
                            elif new_throttled is True:
                                self.disable_downloader(ip)
                        except Exception as e:
                            logger.error(f"[首次联动] {ip} 出错: {e}")

                    elif old_throttled != new_throttled:
                        # 状态变化
                        logger.warning(f"[状态变化] {ip}: {old_throttled} -> {new_throttled}")
                        self.update_throttle_history(ip, new_throttled, server_name)

                        try:
                            if old_throttled is True and new_throttled is False:
                                self.enable_downloader(ip)
                            elif old_throttled is False and new_throttled is True:
                                self.disable_downloader(ip)
                        except Exception as e:
                            logger.error(f"[联动] {ip} 出错: {e}")
                except Exception as e:
                    logger.error(f"处理服务器 {ip} 状态异常: {e}")

        except Exception as e:
            logger.error(f"更新缓存数据失败: {e}")

    def data_collection_loop(self):
        """数据收集循环"""
        logger.info(f"数据收集线程已启动, 检查间隔: {self.check_interval} 秒")
        self.update_cached_data()

        while True:
            try:
                time.sleep(self.check_interval)
                self.update_cached_data()
            except Exception as e:
                logger.error(f"数据收集出错: {e}")
                time.sleep(60)

    def fetch_vertex_run_info(self) -> dict:
        """获取 Vertex 运行信息"""
        if not self.vertex_base_url or not self.vertex_cookie:
            return {"success": False, "error": "配置缺失"}

        try:
            api_url = f"{self.vertex_base_url}/api/setting/getRunInfo"
            headers = {"Cookie": self.vertex_cookie, "User-Agent": "Mozilla/5.0"}
            
            response = requests.get(api_url, headers=headers, timeout=15)
            response.raise_for_status()
            
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 401:
                logger.warning("[Vertex] 获取运行信息失败: 鉴权失效 (401 Unauthorized)，Cookie 已过期")
                return {"success": False, "error": "401 Unauthorized (Cookie已过期失效，请重新登录Vertex并更新cookie)"}
            logger.error(f"[Vertex] 获取运行信息HTTP错误: {e}")
            return {"success": False, "error": str(e)}
        except Exception as e:
            logger.error(f"[Vertex] 获取运行信息失败: {e}")
            return {"success": False, "error": str(e)}

    def send_vertex_daily_report(self):
        """发送 Vertex 日报"""
        if not self.telegram_notifier:
            return

        try:
            run_info = self.fetch_vertex_run_info()
            
            if not run_info.get('success'):
                return

            self.telegram_notifier.send_vertex_report(run_info)
        except Exception as e:
            logger.error(f"发送日报失败: {e}")

    def run(self):
        """启动Flask应用"""
        logger.info(f"启动Web服务,端口: {self.port}")
        logger.info(f"监控面板: http://localhost:{self.port}/")
        self.app.run(host='0.0.0.0', port=self.port, debug=False)


def main():
    monitor = NetcupTrafficMonitor()
    monitor.run()


if __name__ == '__main__':
    main()
