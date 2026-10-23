import logging
import sys
from datetime import datetime, timezone, timedelta
from logging.handlers import RotatingFileHandler

# 北京时间时区定义 (UTC+8)
BEIJING_TZ = timezone(timedelta(hours=8))


def get_beijing_now() -> datetime:
    """获取当前的北京时间 (UTC+8)"""
    return datetime.now(BEIJING_TZ)


def format_beijing_time(dt: datetime = None, fmt: str = '%Y-%m-%d %H:%M:%S') -> str:
    """格式化为北京时间字符串"""
    if dt is None:
        dt = get_beijing_now()
    elif dt.tzinfo is None:
        dt = dt.replace(tzinfo=BEIJING_TZ)
    else:
        dt = dt.astimezone(BEIJING_TZ)
    return dt.strftime(fmt)


def beijing_from_timestamp(ts: float, fmt: str = '%Y-%m-%d %H:%M:%S') -> str:
    """从时间戳格式化为北京时间字符串"""
    if not ts:
        return ""
    return datetime.fromtimestamp(ts, tz=BEIJING_TZ).strftime(fmt)


def beijing_time_converter(*args):
    """日志格式化器转换器，确保日志时间强制为北京时间"""
    return datetime.now(BEIJING_TZ).timetuple()


def setup_logger(name, log_file=None, level=logging.INFO, encoding='utf-8', max_bytes=10*1024*1024, backup_count=3):
    """
    设置日志记录，同时输出到文件（带大小轮转）和标准输出（强制北京时间戳）
    """
    logger = logging.getLogger(name)

    # 防止重复添加处理器
    if not logger.handlers:
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        formatter.converter = beijing_time_converter

        # 添加 StreamHandler（输出到 Docker logs / 控制台）
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        # 如果指定了日志文件，使用 RotatingFileHandler 防止磁盘被日志占满
        if log_file:
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding=encoding
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        logger.setLevel(level)

    return logger


# 创建主日志
logger = setup_logger('main_logger', 'rss.log')