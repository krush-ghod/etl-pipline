"""Ingestor — pulls raw data from APIs or local files in streaming batches."""

from __future__ import annotations

import csv
import io
import json
import pathlib
import time
from typing import Any, Generator
from urllib.parse import urljoin

import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from src.utils import config
from src.utils.logger import get_logger

logger = get_logger(__name__, stage="ingest")

PAGINATION_STYLES = {"auto", "page_number", "offset", "cursor", "link_header", "none"}

_CURSOR_FIELD_NAMES = (
    "next_cursor", "nextCursor", "cursor", "next_page_token",
    "nextPageToken", "continuation", "continuationToken",
    "next_token", "nextToken", "after", "scroll_id",
)
_TOTAL_FIELD_NAMES = ("total", "count", "total_count", "totalResults", "total_results")
_PAGE_FIELD_NAMES = ("page", "current_page", "currentPage", "pageNumber")
_OFFSET_FIELD_NAMES = ("offset", "skip", "start")

AUTH_METHODS = {"bearer", "query_param", "header", "basic", "none"}


class Ingestor:
    """Pull raw records from the configured source in batches."""

    def __init__(
        self,
        source_url: str | None = None,
        api_key: str | None = None,
        batch_size: int | None = None,
        pagination: str | None = None,
        auth_method: str | None = None,
        auth_param_name: str | None = None,
        data_key: str | None = None,
        cursor_key: str | None = None,
        total_key: str | None = None,
        rate_limit_delay: float | None = None,
        extra_params: dict[str, str] | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        self.source_url = source_url or config.SOURCE_URL
        self.api_key = api_key or config.SOURCE_API_KEY
        self.batch_size = batch_size or config.BATCH_SIZE

        self.pagination = (pagination or config.PAGINATION_STYLE).lower()
        if self.pagination not in PAGINATION_STYLES:
            raise ValueError(
                f"Unknown pagination style '{self.pagination}'. "
                f"Choose from {PAGINATION_STYLES}"
            )

        self.auth_method = (auth_method or config.AUTH_METHOD).lower()
        if self.auth_method not in AUTH_METHODS:
            raise ValueError(
                f"Unknown auth method '{self.auth_method}'. "
                f"Choose from {AUTH_METHODS}"
            )

        self.auth_param_name = auth_param_name or config.AUTH_PARAM_NAME
        self.data_key = data_key or config.DATA_KEY
        self.cursor_key = cursor_key or config.CURSOR_KEY
        self.total_key = total_key or config.TOTAL_KEY
        self.rate_limit_delay = (
            rate_limit_delay
            if rate_limit_delay is not None
            else config.RATE_LIMIT_DELAY
        )
        self.extra_params = extra_params or {}
        self.extra_headers = extra_headers or {}


    def extract(self) -> Generator[list[dict[str, Any]], None, None]:
        """Yield batches of raw records from the data source."""
        logger.info(
            "Starting ingestion from %s (pagination=%s, auth=%s)",
            self.source_url,
            self.pagination,
            self.auth_method,
        )

        if self._is_local_file():
            yield from self._read_local_file()
        else:
            yield from self._fetch_api()

        logger.info("Ingestion complete")

    def _is_local_file(self) -> bool:
        return pathlib.Path(self.source_url).suffix in {".csv", ".json", ".ndjson", ".jsonl"}

    def _read_local_file(self) -> Generator[list[dict[str, Any]], None, None]:
        path = pathlib.Path(self.source_url)

        if path.suffix == ".json":
            with open(path, encoding="utf-8") as fh:
                data = json.load(fh)
                if isinstance(data, dict):
                    data = [data]
                yield from self._batch(data)

        elif path.suffix in {".ndjson", ".jsonl"}:
            with open(path, encoding="utf-8") as fh:
                yield from self._batch(
                    json.loads(line) for line in fh if line.strip()
                )

        elif path.suffix == ".csv":
            with open(path, newline="", encoding="utf-8") as fh:
                reader = csv.DictReader(fh)
                yield from self._batch(reader)

        else:
            raise ValueError(f"Unsupported file type: {path.suffix}")

    def _build_headers(self) -> dict[str, str]:
        """Build request headers including auth if applicable."""
        headers: dict[str, str] = {"Accept": "application/json"}
        headers.update(self.extra_headers)

        if self.auth_method == "bearer" and self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        elif self.auth_method == "header" and self.api_key:
            name = self.auth_param_name or "X-API-Key"
            headers[name] = self.api_key
        elif self.auth_method == "basic" and self.api_key:
            # Expect api_key in "user:password" format
            import base64
            token = base64.b64encode(self.api_key.encode()).decode()
            headers["Authorization"] = f"Basic {token}"

        return headers

    def _build_auth_params(self) -> dict[str, str]:
        """Return query-string params for query_param auth."""
        if self.auth_method == "query_param" and self.api_key:
            name = self.auth_param_name or "api_key"
            return {name: self.api_key}
        return {}

    @retry(
        retry=retry_if_exception_type(requests.RequestException),
        stop=stop_after_attempt(config.MAX_RETRIES),
        wait=wait_exponential(multiplier=config.RETRY_DELAY, min=1, max=30),
        reraise=True,
    )
    def _fetch_page(self, url: str, params: dict | None = None) -> requests.Response:
        """Execute a GET request with auth, retry, and rate-limit handling."""
        headers = self._build_headers()
        all_params = {**self._build_auth_params(), **(params or {}), **self.extra_params}

        response = requests.get(url, headers=headers, params=all_params, timeout=30)

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 5))
            logger.warning("Rate-limited — sleeping %d s", retry_after)
            time.sleep(retry_after)
            raise requests.RequestException("Rate-limited, retrying")

        response.raise_for_status()
        return response

    @staticmethod
    def _resolve_key(data: Any, dotted_key: str | None) -> Any:
        """Walk into a nested dict following a dot-separated key path."""
        if not dotted_key:
            return data
        for part in dotted_key.split("."):
            if isinstance(data, dict):
                data = data.get(part)
            else:
                return None
        return data

    def _extract_records(self, payload: Any) -> list[dict[str, Any]]:
        """Pull the records list out of an API response body."""
        if isinstance(payload, list):
            return payload

        if self.data_key:
            records = self._resolve_key(payload, self.data_key)
            if isinstance(records, list):
                return records

        for key in ("results", "data", "items", "records", "features", "entries"):
            if key in payload and isinstance(payload[key], list):
                return payload[key]

        if isinstance(payload, dict):
            return [payload]

        return []

    def _extract_cursor(self, payload: Any) -> str | None:
        """Pull the next-page cursor from the response body."""
        return self._resolve_key(payload, self.cursor_key) if self.cursor_key else None

    def _extract_total(self, payload: Any) -> int | None:
        """Pull a total-record count from the response (for logging only)."""
        val = self._resolve_key(payload, self.total_key) if self.total_key else None
        return int(val) if val is not None else None

    def _fetch_api(self) -> Generator[list[dict[str, Any]], None, None]:
        """Route to the correct pagination strategy."""
        if self.pagination == "auto":
            yield from self._paginate_auto()
        else:
            strategy = {
                "page_number": self._paginate_page_number,
                "offset": self._paginate_offset,
                "cursor": self._paginate_cursor,
                "link_header": self._paginate_link_header,
                "none": self._paginate_none,
            }
            yield from strategy[self.pagination]()

    def _paginate_auto(self) -> Generator[list[dict[str, Any]], None, None]:
        """Make one probe request, detect the pagination style, then delegate."""
        logger.info("Auto-detecting pagination style…")
        response = self._fetch_page(self.source_url, params={"per_page": self.batch_size})
        detected = self._detect_pagination(response)
        self.pagination = detected
        logger.info("Auto-detected pagination style: %s", detected)

        records = self._parse_response(response)

        if detected == "none":
            if records:
                yield from self._batch(records)
            return

        if detected == "link_header":
            if records:
                yield records
            next_url = self._next_link(response)
            if next_url:
                self._rate_limit_sleep()
                page = 1
                url = next_url
                while url:
                    page += 1
                    logger.info("Fetching link-header page %d", page)
                    resp = self._fetch_page(url)
                    recs = self._parse_response(resp)
                    if not recs:
                        break
                    logger.info("Fetched %d records (link page %d)", len(recs), page)
                    yield recs
                    url = self._next_link(resp)
                    if url:
                        self._rate_limit_sleep()
            return

        if detected == "cursor":
            payload = response.json()
            if records:
                yield records
            cursor = self._extract_cursor(payload)
            if not cursor:
                cursor = self._find_cursor_value(payload)
            page = 1
            while cursor:
                page += 1
                params: dict[str, Any] = {"limit": self.batch_size, "cursor": cursor}
                logger.info("Fetching cursor page %d", page)
                resp = self._fetch_page(self.source_url, params=params)
                p = resp.json()
                recs = self._extract_records(p)
                if not recs:
                    break
                logger.info("Fetched %d records (cursor page %d)", len(recs), page)
                yield recs
                cursor = self._extract_cursor(p) or self._find_cursor_value(p)
                self._rate_limit_sleep()
            return

        # page_number or offset — yield first page, then continue
        if records:
            yield records

        if len(records) < self.batch_size:
            return

        if detected == "offset":
            offset = len(records)
            while True:
                params2 = {"offset": offset, "limit": self.batch_size}
                logger.info("Fetching offset=%d limit=%d", offset, self.batch_size)
                resp = self._fetch_page(self.source_url, params=params2)
                recs = self._parse_response(resp)
                if not recs:
                    break
                yield recs
                if len(recs) < self.batch_size:
                    break
                offset += len(recs)
                self._rate_limit_sleep()
        else:  # page_number
            page = 2
            while True:
                params3 = {"page": page, "per_page": self.batch_size}
                logger.info("Fetching page %d", page)
                resp = self._fetch_page(self.source_url, params=params3)
                recs = self._parse_response(resp)
                if not recs:
                    break
                logger.info("Fetched %d records from page %d", len(recs), page)
                yield recs
                if len(recs) < self.batch_size:
                    break
                page += 1
                self._rate_limit_sleep()

    @classmethod
    def _detect_pagination(cls, response: requests.Response) -> str:
        """Inspect a response to decide which pagination style fits."""
        # 1. Link header?
        link = response.headers.get("Link", "")
        if 'rel="next"' in link:
            return "link_header"

        # Try to parse JSON body for metadata clues
        try:
            payload = response.json()
        except (ValueError, AttributeError):
            return "none"

        if not isinstance(payload, dict):
            return "none"

        flat = cls._flatten_meta(payload)

        for name in _CURSOR_FIELD_NAMES:
            if name in flat and flat[name] is not None:
                return "cursor"

        for name in _OFFSET_FIELD_NAMES:
            if name in flat:
                return "offset"

        for name in _PAGE_FIELD_NAMES:
            if name in flat:
                return "page_number"

        for name in _TOTAL_FIELD_NAMES:
            if name in flat:
                return "offset"

        return "none"

    @staticmethod
    def _flatten_meta(payload: dict) -> dict[str, Any]:
        """Collect scalar keys from top-level and common nested metadata wrappers."""
        flat: dict[str, Any] = {}
        meta_wrappers = ("meta", "pagination", "paging", "page_info", "pageInfo", "_links")
        for k, v in payload.items():
            if isinstance(v, dict) and k in meta_wrappers:
                flat.update(v)
            elif not isinstance(v, (dict, list)):
                flat[k] = v
        return flat

    @classmethod
    def _find_cursor_value(cls, payload: Any) -> str | None:
        """Hunt for a cursor value in the response when no cursor_key is configured."""
        if not isinstance(payload, dict):
            return None
        flat = cls._flatten_meta(payload)
        for name in _CURSOR_FIELD_NAMES:
            val = flat.get(name)
            if val:
                return str(val)
        return None

    def _rate_limit_sleep(self) -> None:
        if self.rate_limit_delay > 0:
            time.sleep(self.rate_limit_delay)


    def _paginate_page_number(self) -> Generator[list[dict[str, Any]], None, None]:
        page = 1
        while True:
            params = {"page": page, "per_page": self.batch_size}
            logger.info("Fetching page %d", page)

            response = self._fetch_page(self.source_url, params=params)
            records = self._parse_response(response)

            if not records:
                break

            logger.info("Fetched %d records from page %d", len(records), page)
            yield records

            if len(records) < self.batch_size:
                break

            page += 1
            self._rate_limit_sleep()


    def _paginate_offset(self) -> Generator[list[dict[str, Any]], None, None]:
        offset = 0
        total_fetched = 0
        while True:
            params = {"offset": offset, "limit": self.batch_size}
            logger.info("Fetching offset=%d limit=%d", offset, self.batch_size)

            response = self._fetch_page(self.source_url, params=params)
            records = self._parse_response(response)

            if not records:
                break

            total_fetched += len(records)
            logger.info("Fetched %d records (total so far: %d)", len(records), total_fetched)
            yield records

            if len(records) < self.batch_size:
                break

            offset += len(records)
            self._rate_limit_sleep()

    def _paginate_cursor(self) -> Generator[list[dict[str, Any]], None, None]:
        cursor: str | None = None
        page = 0
        while True:
            params: dict[str, Any] = {"limit": self.batch_size}
            if cursor:
                params["cursor"] = cursor
            page += 1
            logger.info("Fetching cursor page %d", page)

            response = self._fetch_page(self.source_url, params=params)
            payload = response.json()
            records = self._extract_records(payload)

            if not records:
                break

            logger.info("Fetched %d records (cursor page %d)", len(records), page)
            yield records

            cursor = self._extract_cursor(payload)
            if not cursor:
                break

            self._rate_limit_sleep()

    def _paginate_link_header(self) -> Generator[list[dict[str, Any]], None, None]:
        url: str | None = self.source_url
        params: dict[str, Any] = {"per_page": self.batch_size}
        page = 0

        while url:
            page += 1
            logger.info("Fetching link-header page %d", page)

            response = self._fetch_page(url, params=params)
            records = self._parse_response(response)

            if not records:
                break

            logger.info("Fetched %d records (link page %d)", len(records), page)
            yield records

            params = {}
            url = self._next_link(response)

            if url:
                self._rate_limit_sleep()

    @staticmethod
    def _next_link(response: requests.Response) -> str | None:
        """Parse the ``Link`` header for the ``rel="next"`` URL."""
        link_header = response.headers.get("Link", "")
        for part in link_header.split(","):
            if 'rel="next"' in part:
                url = part.split(";")[0].strip().strip("<>")
                return url
        return None


    def _paginate_none(self) -> Generator[list[dict[str, Any]], None, None]:
        logger.info("Fetching (single request, no pagination)")
        response = self._fetch_page(self.source_url)
        records = self._parse_response(response)

        if records:
            logger.info("Fetched %d records", len(records))
            yield from self._batch(records)

    def _parse_response(self, response: requests.Response) -> list[dict[str, Any]]:
        """Extract records from an HTTP response regardless of content type."""
        content_type = response.headers.get("Content-Type", "")

        if "text/csv" in content_type:
            reader = csv.DictReader(io.StringIO(response.text))
            return list(reader)

        payload = response.json()
        records = self._extract_records(payload)

        total = self._extract_total(payload)
        if total is not None:
            logger.info("API reports %d total records", total)

        return records

    def _batch(self, iterable) -> Generator[list[dict[str, Any]], None, None]:
        """Chunk an iterable into lists of *batch_size*."""
        batch: list[dict[str, Any]] = []
        for record in iterable:
            batch.append(dict(record))
            if len(batch) >= self.batch_size:
                logger.info("Fetched %d records", len(batch))
                yield batch
                batch = []
        if batch:
            logger.info("Fetched %d records (final batch)", len(batch))
            yield batch
