"""日志初始化：loguru + 文件轮转。

使用：
    from rquant.logging import setup_logging
    setup_logging()

    from loguru import logger
    logger.info("hello")
"""

import sys
from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from rquant.config import Settings

_initialized = False


def _settings() -> "Settings":
    """Read the process settings at call time, never at import time.

    `rquant.cli` imports this module, so a module-level `Settings` made five environment
    variables a precondition of the console script's own import: `rquant
    runtime-authority-stage` died before `main()` could dispatch it, and the bootstrap
    worktree it has to run in (acceptance A22) has no `.env`. A `settings` that a test has
    bound onto this module still wins, exactly as the old module-level name did.
    """

    bound = globals().get("settings")
    if bound is not None:
        return bound  # type: ignore[no-any-return]
    from rquant.config import get_settings

    return get_settings()


def __getattr__(name: str) -> object:
    """`rquant.logging.settings` stays readable — built on first use, like the source."""

    if name == "settings":
        from rquant.config import get_settings

        return get_settings()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def setup_logging(*, enqueue: bool = True) -> None:
    global _initialized
    if _initialized:
        return

    settings = _settings()

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
