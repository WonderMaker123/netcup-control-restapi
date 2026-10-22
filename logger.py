import logging
import sys
from logging.handlers import RotatingFileHandler


def setup_logger(name, log_file=None, level=logging.INFO, encoding='utf-8', max_bytes=10*1024*1024, backup_count=3):
    """
    设置日志记录，同时输出到文件（带大小轮转）和标准输出
    """
    logger = logging.getLogger(name)

    # 防止重复添加处理器
    if not logger.handlers:
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

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