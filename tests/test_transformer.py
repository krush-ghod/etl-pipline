"""Tests for the Transformer module."""

from __future__ import annotations

import os

os.environ.setdefault("SOURCE_URL", "https://example.com/api/data")
os.environ.setdefault("MONGO_URI", "mongodb://localhost:27017")
os.environ.setdefault("MONGO_DB", "test_db")
os.environ.setdefault("MONGO_COLLECTION", "test_col")

import pytest

from src.transform.transformer import Transformer, BaseTransformer


class TestTransformerNulls:
    """Test null / whitespace handling."""

    def test_empty_strings_become_none(self):
        t = Transformer()
        result = t.transform_batch([{"name": "", "age": "30"}])
        assert result[0]["name"] is None
        assert result[0]["age"] == "30"

    def test_whitespace_only_becomes_none(self):
        t = Transformer()
        result = t.transform_batch([{"field": "   "}])
        assert result[0]["field"] is None


class TestTransformerDates:
    """Test date normalisation."""

    def test_iso_date_string(self):
        t = Transformer(date_fields={"created_at"})
        result = t.transform_batch([{"created_at": "2026-01-15"}])
        assert "2026-01-15" in result[0]["created_at"]

    def test_datetime_with_timezone(self):
        t = Transformer(date_fields={"timestamp"})
        result = t.transform_batch([{"timestamp": "2026-01-15T10:30:00Z"}])
        assert "2026-01-15" in result[0]["timestamp"]

    def test_non_date_field_untouched(self):
        t = Transformer(date_fields={"created_at"})
        result = t.transform_batch([{"name": "Alice", "created_at": "2026-01-01"}])
        assert result[0]["name"] == "Alice"


class TestTransformerFieldRenames:
    """Test field renaming."""

    def test_rename_field(self):
        t = Transformer(field_renames={"old_name": "new_name"})
        result = t.transform_batch([{"old_name": "value"}])
        assert "new_name" in result[0]
        assert "old_name" not in result[0]

    def test_rename_missing_field_is_noop(self):
        t = Transformer(field_renames={"nonexistent": "target"})
        result = t.transform_batch([{"keep": "value"}])
        assert result[0] == {"keep": "value"}


class TestTransformerErrorHandling:
    """Test per-record error handling."""

    def test_bad_record_skipped(self):
        class FailingTransformer(BaseTransformer):
            def pre_transform(self, record):
                if record.get("bad"):
                    raise ValueError("bad record")
                return record

        t = Transformer(custom=FailingTransformer())
        records = [{"id": 1}, {"id": 2, "bad": True}, {"id": 3}]
        result = t.transform_batch(records)

        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[1]["id"] == 3


class TestCustomTransformer:
    """Test custom transformer hooks."""

    def test_pre_and_post_hooks(self):
        class Custom(BaseTransformer):
            def pre_transform(self, record):
                record["pre"] = True
                return record

            def post_transform(self, record):
                record["post"] = True
                return record

        t = Transformer(custom=Custom())
        result = t.transform_batch([{"id": 1}])

        assert result[0]["pre"] is True
        assert result[0]["post"] is True
