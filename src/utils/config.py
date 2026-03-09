"""Centralized configuration from environment variables."""

from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()


def get(key: str, default: str | None = None, required: bool = False) -> str | None:
    """Return an environment variable, raising if *required* and missing."""
    value = os.getenv(key, default)
    if required and not value:
        raise EnvironmentError(f"Required environment variable '{key}' is not set.")
    return value


# ── Source ────────────────────────────────────────────────────────────────────
SOURCE_URL: str = get("SOURCE_URL", required=True)  # type: ignore[assignment]
SOURCE_API_KEY: str | None = get("SOURCE_API_KEY")

# ── API behaviour ─────────────────────────────────────────────────────────────
PAGINATION_STYLE: str = get("PAGINATION_STYLE", "auto")  # type: ignore[assignment]
AUTH_METHOD: str = get("AUTH_METHOD", "bearer")  # type: ignore[assignment]
AUTH_PARAM_NAME: str | None = get("AUTH_PARAM_NAME")  # e.g. api_key, X-API-Key
DATA_KEY: str | None = get("DATA_KEY")  # dot-path to records array in JSON response
CURSOR_KEY: str | None = get("CURSOR_KEY")  # dot-path to next-page cursor
TOTAL_KEY: str | None = get("TOTAL_KEY")  # dot-path to total count
RATE_LIMIT_DELAY: float = float(get("RATE_LIMIT_DELAY", "0"))  # type: ignore[arg-type]

# ── MongoDB ───────────────────────────────────────────────────────────────────
MONGO_URI: str = get("MONGO_URI", required=True)  # type: ignore[assignment]
MONGO_DB: str = get("MONGO_DB", required=True)  # type: ignore[assignment]
MONGO_COLLECTION: str = get("MONGO_COLLECTION", required=True)  # type: ignore[assignment]

# ── Pipeline ──────────────────────────────────────────────────────────────────
BATCH_SIZE: int = int(get("BATCH_SIZE", "500"))  # type: ignore[arg-type]
LOG_LEVEL: str = get("LOG_LEVEL", "INFO")  # type: ignore[assignment]
MAX_RETRIES: int = int(get("MAX_RETRIES", "3"))  # type: ignore[arg-type]
RETRY_DELAY: int = int(get("RETRY_DELAY", "2"))  # type: ignore[arg-type]
