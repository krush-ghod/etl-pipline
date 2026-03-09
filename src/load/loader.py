"""Loader — bulk-upserts cleaned documents into MongoDB."""

from __future__ import annotations

from typing import Any

from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from src.utils import config
from src.utils.logger import get_logger

logger = get_logger(__name__, stage="load")


class Loader:
    """Write transformed documents into a MongoDB collection."""

    def __init__(
        self,
        mongo_uri: str | None = None,
        db_name: str | None = None,
        collection_name: str | None = None,
        upsert_key: str | None = None,
        ordered: bool = False,
        client: MongoClient | None = None,
    ) -> None:
        self.mongo_uri = mongo_uri or config.MONGO_URI
        self.db_name = db_name or config.MONGO_DB
        self.collection_name = collection_name or config.MONGO_COLLECTION
        self.upsert_key = upsert_key or config.UPSERT_KEY
        self.ordered = ordered

        self._client = client or MongoClient(self.mongo_uri)
        self._db = self._client[self.db_name]
        self._collection = self._db[self.collection_name]


    @retry(
        retry=retry_if_exception_type(Exception),
        stop=stop_after_attempt(config.MAX_RETRIES),
        wait=wait_exponential(multiplier=config.RETRY_DELAY, min=1, max=30),
        reraise=True,
    )
    def load_batch(self, documents: list[dict[str, Any]]) -> int:
        """Bulk-upsert *documents* into MongoDB.

        Returns the number of documents upserted / modified.
        """
        if not documents:
            logger.info("Empty batch — nothing to load")
            return 0

        operations = [
            UpdateOne(
                {self.upsert_key: doc.get(self.upsert_key)},
                {"$set": doc},
                upsert=True,
            )
            for doc in documents
        ]

        try:
            result = self._collection.bulk_write(operations, ordered=self.ordered)
            affected = result.upserted_count + result.modified_count
            logger.info(
                "Upserted %d documents into %s.%s",
                affected,
                self.db_name,
                self.collection_name,
            )
            return affected
        except BulkWriteError as exc:
            logger.error(
                "Bulk write error: %d errors",
                len(exc.details.get("writeErrors", [])),
                extra={"write_errors": exc.details.get("writeErrors", [])},
            )
            raise

    def close(self) -> None:
        """Close the underlying MongoDB connection."""
        self._client.close()
        logger.info("MongoDB connection closed")
