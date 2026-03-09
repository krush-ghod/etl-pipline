"""Structured JSON logging for all pipeline stages."""

from __future__ import annotations

import logging
import sys

from pythonjsonlogger import jsonlogger


_CONFIGURED = False


def get_logger(name: str = "etl", stage: str | None = None) -> logging.Logger:
    """Return a logger configured with JSON formatting.

    Parameters
    ----------
    name:
        Logger name (usually the module name).
    stage:
        Pipeline stage tag attached to every log line (``ingest``, ``transform``, ``load``).
    """
    global _CONFIGURED  # noqa: PLW0603

    logger = logging.getLogger(name)

    if not _CONFIGURED:
        _configure_root_logger()
        _CONFIGURED = True

    if stage:
        logger = logging.LoggerAdapter(logger, {"stage": stage})  # type: ignore[assignment]

    return logger


def _configure_root_logger() -> None:
    """Set up the root logger with a JSON handler writing to *stdout*."""
    from src.utils.config import LOG_LEVEL

    root = logging.getLogger()
    root.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))

    handler = logging.StreamHandler(sys.stdout)
    formatter = jsonlogger.JsonFormatter(
        fmt="%(timestamp)s %(level)s %(name)s %(stage)s %(message)s",
        rename_fields={
            "levelname": "level",
            "asctime": "timestamp",
        },
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    handler.setFormatter(formatter)

    # Avoid duplicate handlers on re-import
    if not root.handlers:
        root.addHandler(handler)
