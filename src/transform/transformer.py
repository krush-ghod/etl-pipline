"""Transformer — cleans and reshapes raw records."""

from __future__ import annotations

import traceback
from datetime import datetime, timezone
from typing import Any

from src.utils.logger import get_logger

logger = get_logger(__name__, stage="transform")

_DATE_FIELDS: set[str] = {"created_at", "updated_at", "date", "timestamp"}

_FIELD_RENAMES: dict[str, str] = {}


class BaseTransformer:
    """Override hook methods to inject custom business logic."""

    def pre_transform(self, record: dict[str, Any]) -> dict[str, Any]:
        """Called before the standard transformations."""
        return record

    def post_transform(self, record: dict[str, Any]) -> dict[str, Any]:
        """Called after the standard transformations."""
        return record


class Transformer:
    """Apply cleaning and reshaping rules to raw records."""

    def __init__(
        self,
        date_fields: set[str] | None = None,
        field_renames: dict[str, str] | None = None,
        custom: BaseTransformer | None = None,
    ) -> None:
        self.date_fields = date_fields or _DATE_FIELDS
        self.field_renames = field_renames or _FIELD_RENAMES
        self.custom = custom or BaseTransformer()


    def transform_batch(
        self, records: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Transform a batch of records, skipping any that fail."""
        transformed: list[dict[str, Any]] = []
        errors = 0

        for record in records:
            try:
                clean = self._transform_record(record)
                transformed.append(clean)
            except Exception:
                errors += 1
                logger.error(
                    "Failed to transform record: %s",
                    record.get("_id", record.get("id", "unknown")),
                    extra={
                        "record_id": record.get("_id", record.get("id")),
                        "error": traceback.format_exc(),
                    },
                )

        logger.info(
            "Transformed %d records (%d errors)",
            len(transformed),
            errors,
        )
        return transformed


    def _transform_record(self, record: dict[str, Any]) -> dict[str, Any]:
        record = self.custom.pre_transform(dict(record))  # shallow copy
        record = self._rename_fields(record)
        record = self._handle_nulls(record)
        record = self._normalise_dates(record)
        record = self.custom.post_transform(record)
        return record

    def _rename_fields(self, record: dict[str, Any]) -> dict[str, Any]:
        for old_name, new_name in self.field_renames.items():
            if old_name in record:
                record[new_name] = record.pop(old_name)
        return record

    @staticmethod
    def _handle_nulls(record: dict[str, Any]) -> dict[str, Any]:
        """Replace empty-string and whitespace-only values with None."""
        for key, value in record.items():
            if isinstance(value, str) and not value.strip():
                record[key] = None
        return record

    def _normalise_dates(self, record: dict[str, Any]) -> dict[str, Any]:
        """Parse known date fields into ISO-8601 UTC strings."""
        for field in self.date_fields:
            if field in record and record[field] is not None:
                record[field] = self._to_iso(record[field])
        return record

    @staticmethod
    def _to_iso(value: Any) -> str | None:
        """Best-effort conversion of a value to an ISO-8601 UTC string."""
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc).isoformat()
        if isinstance(value, str):
            for fmt in (
                "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d",
                "%d/%m/%Y",
                "%m/%d/%Y",
            ):
                try:
                    dt = datetime.strptime(value, fmt)
                    return dt.replace(tzinfo=timezone.utc).isoformat()
                except ValueError:
                    continue
            # Return as-is if no format matched
            return value
        return value
