from .config import get_logger, get_settings


settings = get_settings()
logger = get_logger()

__all__ = ["settings", "logger"]
