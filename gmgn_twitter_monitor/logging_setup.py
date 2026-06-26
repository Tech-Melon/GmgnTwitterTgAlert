import sys

from loguru import logger

from . import config

# loguru 对文件路径 sink 会忽略 colorize 参数（认为文件不需要颜色）
# 要强制写入 ANSI 颜色码，需要用 file-like 对象 + colorize=True
# 但这会丧失 rotation/retention 自动轮转能力
# 折中方案：文件 sink 不带颜色（保证 grep 等工具兼容），
# 控制台 sink 带颜色（tail -f 和 journalctl 通过控制台颜色渲染）

LOG_FORMAT = (
    "<level>{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {message}</level>"
)
FILE_FORMAT = "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {message}"


def setup_logging():
    logger.remove()

    # 文件日志 — 纯文本，保留 rotation/retention/compression
    logger.add(
        config.LOG_FILE,
        rotation="10 MB",
        retention="7 days",
        compression="zip",
        level="INFO",
        format=FILE_FORMAT,
        colorize=False,
    )

    # 控制台日志 — 带颜色标记，systemd journal + tail 管道可渲染
    logger.add(
        sys.stderr,
        level="INFO",
        format=LOG_FORMAT,
        colorize=True,
    )

    return logger
