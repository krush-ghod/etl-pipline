"""Tests for the Loader module."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

os.environ.setdefault("SOURCE_URL", "https://example.com/api/data")
os.environ.setdefault("MONGO_URI", "mongodb://localhost:27017")
os.environ.setdefault("MONGO_DB", "test_db")
os.environ.setdefault("MONGO_COLLECTION", "test_col")

import pytest

from src.load.loader import Loader


class TestLoaderLoadBatch:
    """Test the Loader.load_batch method."""

    def _make_loader(self) -> Loader:
        """Create a Loader with a mocked MongoClient."""
        mock_client = MagicMock()
        mock_collection = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=MagicMock())
        mock_client.__getitem__.return_value.__getitem__ = MagicMock(
            return_value=mock_collection
        )

        loader = Loader(
            mongo_uri="mongodb://localhost:27017",
            db_name="test_db",
            collection_name="test_col",
            client=mock_client,
        )
        # Wire up the mocked collection
        loader._collection = mock_collection
        return loader

    def test_empty_batch_returns_zero(self):
        loader = self._make_loader()
        assert loader.load_batch([]) == 0

    def test_load_batch_calls_bulk_write(self):
        loader = self._make_loader()

        result_mock = MagicMock()
        result_mock.upserted_count = 2
        result_mock.modified_count = 0
        loader._collection.bulk_write.return_value = result_mock

        docs = [{"_id": "a", "value": 1}, {"_id": "b", "value": 2}]
        affected = loader.load_batch(docs)

        assert affected == 2
        loader._collection.bulk_write.assert_called_once()

    def test_close_calls_client_close(self):
        loader = self._make_loader()
        loader.close()
        loader._client.close.assert_called_once()


class TestLoaderUpsertKey:
    """Test custom upsert key configuration."""

    def test_custom_upsert_key(self):
        mock_client = MagicMock()
        mock_collection = MagicMock()
        result_mock = MagicMock()
        result_mock.upserted_count = 1
        result_mock.modified_count = 0
        mock_collection.bulk_write.return_value = result_mock

        loader = Loader(
            mongo_uri="mongodb://localhost:27017",
            db_name="test_db",
            collection_name="test_col",
            upsert_key="record_id",
            client=mock_client,
        )
        loader._collection = mock_collection

        loader.load_batch([{"record_id": "x", "data": "hello"}])

        # Verify the filter used the custom key
        call_args = mock_collection.bulk_write.call_args
        operations = call_args[0][0]
        # UpdateOne stores the filter in _filter
        assert operations[0]._filter == {"record_id": "x"}
