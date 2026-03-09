"""Centralized configuration from environment variables."""

from __future__ import annotations

import os
import re
from urllib.parse import urlparse

from dotenv import load_dotenv

load_dotenv()


def _db_name_from_url(url: str) -> str:
    """Derive a database name from the API domain.

    e.g. https://api.nasa.gov/planetary/apod -> nasa_etl
         https://api.openweathermap.org/data/2.5 -> openweathermap_etl
    """
    host = urlparse(url).hostname or "default"
    parts = host.replace("-", ".").split(".")
    # drop generic prefixes/suffixes like api, www, com, org, net, io, gov
    skip = {"api", "www", "com", "org", "net", "io", "gov", "co", "uk", "dev"}
    meaningful = [p for p in parts if p and p not in skip]
    name = "_".join(meaningful) if meaningful else "default"
    return f"{name}_etl"


def _collection_from_url(url: str) -> str:
    """Derive a collection name from the last segment of the URL path.

    e.g. /planetary/apod -> apod
         /data/2.5/weather -> weather
    """
    path = urlparse(url).path.rstrip("/")
    segments = [s for s in path.split("/") if s]
    if not segments:
        return "records"
    last = segments[-1]
    # clean non-alphanumeric chars
    cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", last).strip("_")
    return cleaned or "records"


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
MONGO_DB: str = get("MONGO_DB") or _db_name_from_url(SOURCE_URL)  # type: ignore[assignment]
MONGO_COLLECTION: str = get("MONGO_COLLECTION") or _collection_from_url(SOURCE_URL)  # type: ignore[assignment]
UPSERT_KEY: str = get("UPSERT_KEY", "_id")  # type: ignore[assignment]

# ── Pipeline ──────────────────────────────────────────────────────────────────
BATCH_SIZE: int = int(get("BATCH_SIZE", "500"))  # type: ignore[arg-type]
LOG_LEVEL: str = get("LOG_LEVEL", "INFO")  # type: ignore[assignment]
MAX_RETRIES: int = int(get("MAX_RETRIES", "3"))  # type: ignore[arg-type]
RETRY_DELAY: int = int(get("RETRY_DELAY", "2"))  # type: ignore[arg-type]
