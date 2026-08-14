"""日志初始化：loguru + 文件轮转。

使用：
    from rquant.logging import setup_logging
    setup_logging()

    from loguru import logger
    logger.info("hello")
"""

import sys

from loguru import logger

from rquant.config import settings

_initialized = False


def setup_logging(*, enqueue: bool = True) -> None:
    global _initialized
    if _initialized:
        return

    logger.remove()

    logger.add(
        sys.stderr,
        level=settings.log_level,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
            "<level>{message}</level>"
        ),
    )

    logger.add(
        settings.log_dir / "rquant_{time:YYYY-MM-DD}.log",
        level=settings.log_level,
        rotation="00:00",
        retention="30 days",
        compression="zip",
        enqueue=enqueue,
        format=(
            "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}"
        ),
    )

    _initialized = True
