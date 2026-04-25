"""Repository-local logging helpers."""

from __future__ import annotations

import logging


def get_logger(
    name: str,
    debug: bool = True,
    log_name: str = "app",
    log_level: int = logging.INFO,
) -> logging.Logger:
    """Return a configured logger with a stable, reusable stream handler."""

    _ = (debug, log_name)
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    if not any(getattr(handler, "_dag_pipeline_handler", False) for handler in logger.handlers):
        handler = logging.StreamHandler()
        handler._dag_pipeline_handler = True  # type: ignore[attr-defined]
        handler.setLevel(log_level)
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
        )
        logger.addHandler(handler)

    return logger
