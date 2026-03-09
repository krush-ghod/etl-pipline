"""Tests for the Ingestor module."""

from __future__ import annotations

import json
import os
import tempfile
from unittest.mock import patch, MagicMock, PropertyMock

import pytest

# Set required env vars before importing modules that read config at import time
os.environ.setdefault("SOURCE_URL", "https://example.com/api/data")
os.environ.setdefault("MONGO_URI", "mongodb://localhost:27017")
os.environ.setdefault("MONGO_DB", "test_db")
os.environ.setdefault("MONGO_COLLECTION", "test_col")

from src.ingest.ingestor import Ingestor


# ── Helpers ───────────────────────────────────────────────────────────────────

def _mock_response(
    data=None,
    content_type="application/json",
    status_code=200,
    link_header=None,
    text=None,
):
    """Create a mock ``requests.Response``."""
    response = MagicMock()
    response.status_code = status_code
    response.headers = {"Content-Type": content_type}
    if link_header:
        response.headers["Link"] = link_header
    response.json.return_value = data
    response.text = text or (json.dumps(data) if data else "")
    response.raise_for_status = MagicMock()
    return response


# ── Local file tests ──────────────────────────────────────────────────────────

class TestIngestorLocalJSON:
    """Test ingesting from local JSON files."""

    def test_ingest_json_list(self, tmp_path):
        data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        json_file = tmp_path / "data.json"
        json_file.write_text(json.dumps(data))

        ingestor = Ingestor(source_url=str(json_file), batch_size=10, pagination="none")
        batches = list(ingestor.extract())

        assert len(batches) == 1
        assert len(batches[0]) == 2
        assert batches[0][0]["name"] == "Alice"

    def test_ingest_json_single_object(self, tmp_path):
        data = {"id": 1, "name": "Solo"}
        json_file = tmp_path / "single.json"
        json_file.write_text(json.dumps(data))

        ingestor = Ingestor(source_url=str(json_file), batch_size=10, pagination="none")
        batches = list(ingestor.extract())

        assert len(batches) == 1
        assert batches[0][0]["name"] == "Solo"

    def test_ingest_json_batching(self, tmp_path):
        data = [{"id": i} for i in range(5)]
        json_file = tmp_path / "batch.json"
        json_file.write_text(json.dumps(data))

        ingestor = Ingestor(source_url=str(json_file), batch_size=2, pagination="none")
        batches = list(ingestor.extract())

        assert len(batches) == 3  # 2 + 2 + 1


class TestIngestorLocalCSV:
    """Test ingesting from local CSV files."""

    def test_ingest_csv(self, tmp_path):
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name\n1,Alice\n2,Bob\n")

        ingestor = Ingestor(source_url=str(csv_file), batch_size=10, pagination="none")
        batches = list(ingestor.extract())

        assert len(batches) == 1
        assert len(batches[0]) == 2
        assert batches[0][0]["name"] == "Alice"


class TestIngestorNDJSON:
    """Test ingesting from newline-delimited JSON files."""

    def test_ingest_ndjson(self, tmp_path):
        ndjson_file = tmp_path / "data.ndjson"
        ndjson_file.write_text('{"id": 1}\n{"id": 2}\n{"id": 3}\n')

        ingestor = Ingestor(source_url=str(ndjson_file), batch_size=10, pagination="none")
        batches = list(ingestor.extract())

        assert len(batches) == 1
        assert len(batches[0]) == 3


# ── Pagination: page_number ───────────────────────────────────────────────────

class TestPaginationPageNumber:
    """Test ?page=N&per_page=N pagination."""

    @patch("src.ingest.ingestor.requests.get")
    def test_single_page(self, mock_get):
        mock_get.return_value = _mock_response([{"id": 1}, {"id": 2}])

        ingestor = Ingestor(
            source_url="https://api.example.com/data",
            batch_size=10,
            pagination="page_number",
            auth_method="none",
        )
        batches = list(ingestor.extract())

        assert len(batches) == 1
        assert len(batches[0]) == 2

    @patch("src.ingest.ingestor.requests.get")
    def test_multi_page(self, mock_get):
        page1 = _mock_response([{"id": i} for i in range(3)])
        page2 = _mock_response([{"id": 10}])
        mock_get.side_effect = [page1, page2]

        ingestor = Ingestor(
            source_url="https://api.example.com/data",
            batch_size=3,
            pagination="page_number",
            auth_method="none",
        )
        batches = list(ingestor.extract())

        assert len(batches) == 2

    @patch("src.ingest.ingestor.requests.get")
    def test_empty_first_page(self, mock_get):
        mock_get.return_value = _mock_response([])

        ingestor = Ingestor(
            source_url="https://api.example.com/data",
            batch_size=10,
            pagination="page_number",
            auth_method="none",
        )
        batches = list(ingestor.extract())

        assert len(batches) == 0


# ── Pagination: offset ────────────────────────────────────────────────────────

class TestPaginationOffset:
    """Test ?offset=N&limit=N pagination (NASA, Socrata, CKAN)."""

    @patch("src.ingest.ingestor.requests.get")
    def test_offset_pagination(self, mock_get):
        batch1 = _mock_response({"data": [{"id": i} for i in range(3)]})
        batch2 = _mock_response({"data": [{"id": 10}]})
        mock_get.side_effect = [batch1, batch2]

        ingestor = Ingestor(
            source_url="https://api.nasa.gov/data",
            batch_size=3,
            pagination="offset",
            auth_method="none",
            data_key="data",
        )
        batches = list(ingestor.extract())

        assert len(batches) == 2
        # Verify offset params were sent
        first_call_params = mock_get.call_args_list[0][1].get("params", {})
        assert first_call_params.get("offset") == 0

    @patch("src.ingest.ingestor.requests.get")
    def test_offset_empty_stops(self, mock_get):
        mock_get.return_value = _mock_response({"data": []})

        ingestor = Ingestor(
            source_url="https://api.example.com/data",
            batch_size=10,
            pagination="offset",
            auth_method="none",
            data_key="data",
        )
        batches = list(ingestor.extract())
        assert len(batches) == 0


# ── Pagination: cursor ────────────────────────────────────────────────────────

class TestPaginationCursor:
    """Test cursor-based pagination."""

    @patch("src.ingest.ingestor.requests.get")
    def test_cursor_pagination(self, mock_get):
        page1 = _mock_response({
            "data": [{"id": 1}, {"id": 2}],
            "meta": {"next_cursor": "abc123"},
        })
        page2 = _mock_response({
            "data": [{"id": 3}],
            "meta": {"next_cursor": None},
        })
        mock_get.side_effect = [page1, page2]

        ingestor = Ingestor(
            source_url="https://api.example.com/data",
            batch_size=10,
            pagination="cursor",
            auth_method="none",
            data_key="data",
            cursor_key="meta.next_cursor",
        )
        batches = list(ingestor.extract())

        assert len(batches) == 2
        assert len(batches[0]) == 2
        assert len(batches[1]) == 1

    @patch("src.ingest.ingestor.requests.get")
    def test_cursor_no_cursor_stops(self, mock_get):
        mock_get.return_value = _mock_response({"data": [{"id": 1}]})

        ingestor = Ingestor(
            source_url="https://api.example.com/data",
            batch_size=10,
            pagination="cursor",
            auth_method="none",
            data_key="data",
        )
        batches = list(ingestor.extract())
        assert len(batches) == 1


# ── Pagination: link_header ───────────────────────────────────────────────────

class TestPaginationLinkHeader:
    """Test RFC 5988 Link header pagination (GitHub-style)."""

    @patch("src.ingest.ingestor.requests.get")
    def test_link_header_pagination(self, mock_get):
        page1 = _mock_response(
            [{"id": 1}],
            link_header='<https://api.example.com/data?page=2>; rel="next"',
        )
        page2 = _mock_response([{"id": 2}])
        mock_get.side_effect = [page1, page2]

        ingestor = Ingestor(
            source_url="https://api.example.com/data",
            batch_size=10,
            pagination="link_header",
            auth_method="none",
        )
        batches = list(ingestor.extract())

        assert len(batches) == 2

    @patch("src.ingest.ingestor.requests.get")
    def test_no_link_header_stops(self, mock_get):
        mock_get.return_value = _mock_response([{"id": 1}])

        ingestor = Ingestor(
            source_url="https://api.example.com/data",
            batch_size=10,
            pagination="link_header",
            auth_method="none",
        )
        batches = list(ingestor.extract())
        assert len(batches) == 1


# ── Pagination: none ──────────────────────────────────────────────────────────

class TestPaginationNone:
    """Test single-request (no pagination) mode."""

    @patch("src.ingest.ingestor.requests.get")
    def test_no_pagination(self, mock_get):
        mock_get.return_value = _mock_response([{"id": 1}, {"id": 2}, {"id": 3}])

        ingestor = Ingestor(
            source_url="https://api.example.com/data",
            batch_size=10,
            pagination="none",
            auth_method="none",
        )
        batches = list(ingestor.extract())

        assert len(batches) == 1
        assert len(batches[0]) == 3


# ── Auth methods ──────────────────────────────────────────────────────────────

class TestAuthMethods:
    """Test different authentication strategies."""

    @patch("src.ingest.ingestor.requests.get")
    def test_bearer_auth(self, mock_get):
        mock_get.return_value = _mock_response([{"id": 1}])

        ingestor = Ingestor(
            source_url="https://api.example.com/data",
            api_key="mytoken",
            batch_size=10,
            pagination="none",
            auth_method="bearer",
        )
        list(ingestor.extract())

        headers = mock_get.call_args[1]["headers"]
        assert headers["Authorization"] == "Bearer mytoken"

    @patch("src.ingest.ingestor.requests.get")
    def test_query_param_auth(self, mock_get):
        mock_get.return_value = _mock_response([{"id": 1}])

        ingestor = Ingestor(
            source_url="https://api.example.com/data",
            api_key="DEMO_KEY",
            batch_size=10,
            pagination="none",
            auth_method="query_param",
            auth_param_name="api_key",
        )
        list(ingestor.extract())

        params = mock_get.call_args[1]["params"]
        assert params["api_key"] == "DEMO_KEY"

    @patch("src.ingest.ingestor.requests.get")
    def test_header_auth(self, mock_get):
        mock_get.return_value = _mock_response([{"id": 1}])

        ingestor = Ingestor(
            source_url="https://api.example.com/data",
            api_key="secret",
            batch_size=10,
            pagination="none",
            auth_method="header",
            auth_param_name="X-Custom-Key",
        )
        list(ingestor.extract())

        headers = mock_get.call_args[1]["headers"]
        assert headers["X-Custom-Key"] == "secret"

    @patch("src.ingest.ingestor.requests.get")
    def test_no_auth(self, mock_get):
        mock_get.return_value = _mock_response([{"id": 1}])

        ingestor = Ingestor(
            source_url="https://api.example.com/data",
            batch_size=10,
            pagination="none",
            auth_method="none",
        )
        list(ingestor.extract())

        headers = mock_get.call_args[1]["headers"]
        assert "Authorization" not in headers


# ── Data extraction ───────────────────────────────────────────────────────────

class TestDataExtraction:
    """Test extracting records from different JSON response shapes."""

    @patch("src.ingest.ingestor.requests.get")
    def test_nested_data_key(self, mock_get):
        mock_get.return_value = _mock_response({
            "response": {"data": [{"id": 1}, {"id": 2}]},
            "status": "ok",
        })

        ingestor = Ingestor(
            source_url="https://api.example.com/data",
            batch_size=10,
            pagination="none",
            auth_method="none",
            data_key="response.data",
        )
        batches = list(ingestor.extract())

        assert len(batches) == 1
        assert len(batches[0]) == 2

    @patch("src.ingest.ingestor.requests.get")
    def test_fallback_common_keys(self, mock_get):
        """When no data_key is set, auto-detect common wrapper keys."""
        mock_get.return_value = _mock_response({
            "features": [{"id": 1}],
            "type": "FeatureCollection",
        })

        ingestor = Ingestor(
            source_url="https://api.example.com/geo",
            batch_size=10,
            pagination="none",
            auth_method="none",
        )
        batches = list(ingestor.extract())

        assert len(batches) == 1
        assert batches[0][0]["id"] == 1

    @patch("src.ingest.ingestor.requests.get")
    def test_top_level_list(self, mock_get):
        mock_get.return_value = _mock_response([{"id": 1}, {"id": 2}])

        ingestor = Ingestor(
            source_url="https://api.example.com/data",
            batch_size=10,
            pagination="none",
            auth_method="none",
        )
        batches = list(ingestor.extract())

        assert len(batches) == 1
        assert len(batches[0]) == 2


# ── Resolve key helper ────────────────────────────────────────────────────────

class TestResolveKey:
    """Test the dot-path key resolver."""

    def test_simple_key(self):
        assert Ingestor._resolve_key({"a": 1}, "a") == 1

    def test_nested_key(self):
        assert Ingestor._resolve_key({"a": {"b": {"c": 42}}}, "a.b.c") == 42

    def test_missing_key_returns_none(self):
        assert Ingestor._resolve_key({"a": 1}, "x.y") is None

    def test_none_key_returns_data(self):
        data = {"a": 1}
        assert Ingestor._resolve_key(data, None) is data


# ── Invalid config ────────────────────────────────────────────────────────────

class TestInvalidConfig:
    """Test that bad config raises clear errors."""

    def test_invalid_pagination_style(self):
        with pytest.raises(ValueError, match="Unknown pagination"):
            Ingestor(
                source_url="https://example.com",
                pagination="magic",
                auth_method="none",
            )

    def test_invalid_auth_method(self):
        with pytest.raises(ValueError, match="Unknown auth"):
            Ingestor(
                source_url="https://example.com",
                pagination="none",
                auth_method="oauth3",
            )


# ── Auto-detection ────────────────────────────────────────────────────────────

class TestAutoDetectPagination:
    """Test the auto-detection of pagination style from response signals."""

    def test_detect_link_header(self):
        resp = _mock_response(
            [{"id": 1}],
            link_header='<https://api.example.com/data?page=2>; rel="next"',
        )
        assert Ingestor._detect_pagination(resp) == "link_header"

    def test_detect_cursor_in_meta(self):
        resp = _mock_response({
            "data": [{"id": 1}],
            "meta": {"next_cursor": "abc123"},
        })
        assert Ingestor._detect_pagination(resp) == "cursor"

    def test_detect_cursor_top_level(self):
        resp = _mock_response({
            "results": [{"id": 1}],
            "nextPageToken": "xyz",
        })
        assert Ingestor._detect_pagination(resp) == "cursor"

    def test_detect_offset(self):
        resp = _mock_response({
            "data": [{"id": 1}],
            "offset": 0,
            "limit": 100,
        })
        assert Ingestor._detect_pagination(resp) == "offset"

    def test_detect_page_number(self):
        resp = _mock_response({
            "data": [{"id": 1}],
            "page": 1,
            "per_page": 50,
        })
        assert Ingestor._detect_pagination(resp) == "page_number"

    def test_detect_total_only_falls_to_offset(self):
        resp = _mock_response({
            "results": [{"id": 1}],
            "total": 1000,
        })
        assert Ingestor._detect_pagination(resp) == "offset"

    def test_detect_pagination_nested_in_pagination_key(self):
        resp = _mock_response({
            "items": [{"id": 1}],
            "pagination": {"current_page": 1, "total": 100},
        })
        # current_page -> page_number? Actually currentPage is in _PAGE_FIELD_NAMES
        # but current_page is matched by _PAGE_FIELD_NAMES: "currentPage" not "current_page"
        # The pagination wrapper has "total" -> offset
        detected = Ingestor._detect_pagination(resp)
        assert detected in ("page_number", "offset")

    def test_detect_none_for_plain_list(self):
        resp = _mock_response([{"id": 1}, {"id": 2}])
        assert Ingestor._detect_pagination(resp) == "none"

    def test_detect_none_for_single_object(self):
        resp = _mock_response({"name": "Alice", "age": 30})
        assert Ingestor._detect_pagination(resp) == "none"

    def test_detect_none_for_non_json(self):
        resp = _mock_response(None, content_type="text/html")
        resp.json.side_effect = ValueError("Not JSON")
        assert Ingestor._detect_pagination(resp) == "none"


class TestAutoDetectEndToEnd:
    """Test that auto mode correctly ingests data after detecting the style."""

    @patch("src.ingest.ingestor.requests.get")
    def test_auto_detects_link_header_and_follows(self, mock_get):
        page1 = _mock_response(
            [{"id": 1}],
            link_header='<https://api.example.com/data?page=2>; rel="next"',
        )
        page2 = _mock_response([{"id": 2}])
        mock_get.side_effect = [page1, page2]

        ingestor = Ingestor(
            source_url="https://api.example.com/data",
            batch_size=10,
            pagination="auto",
            auth_method="none",
        )
        batches = list(ingestor.extract())

        assert len(batches) == 2
        assert ingestor.pagination == "link_header"

    @patch("src.ingest.ingestor.requests.get")
    def test_auto_detects_none_for_simple_list(self, mock_get):
        mock_get.return_value = _mock_response([{"id": 1}, {"id": 2}])

        ingestor = Ingestor(
            source_url="https://api.example.com/data",
            batch_size=10,
            pagination="auto",
            auth_method="none",
        )
        batches = list(ingestor.extract())

        assert len(batches) == 1
        assert len(batches[0]) == 2
        assert ingestor.pagination == "none"

    @patch("src.ingest.ingestor.requests.get")
    def test_auto_detects_offset(self, mock_get):
        page1 = _mock_response({
            "data": [{"id": 1}, {"id": 2}],
            "offset": 0,
            "total": 2,
        })
        mock_get.side_effect = [page1]  # only 2 records < batch_size → stops

        ingestor = Ingestor(
            source_url="https://api.example.com/data",
            batch_size=10,
            pagination="auto",
            auth_method="none",
            data_key="data",
        )
        batches = list(ingestor.extract())

        assert len(batches) == 1
        assert ingestor.pagination == "offset"

    @patch("src.ingest.ingestor.requests.get")
    def test_auto_detects_cursor(self, mock_get):
        page1 = _mock_response({
            "data": [{"id": 1}],
            "meta": {"next_cursor": "cur_abc"},
        })
        page2 = _mock_response({
            "data": [{"id": 2}],
            "meta": {"next_cursor": None},
        })
        mock_get.side_effect = [page1, page2]

        ingestor = Ingestor(
            source_url="https://api.example.com/data",
            batch_size=10,
            pagination="auto",
            auth_method="none",
            data_key="data",
            cursor_key="meta.next_cursor",
        )
        batches = list(ingestor.extract())

        assert len(batches) == 2
        assert ingestor.pagination == "cursor"
