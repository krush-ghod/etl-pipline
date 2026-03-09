#!/usr/bin/env python3
"""ETL Pipeline entry point."""

from __future__ import annotations

import argparse
import sys
import traceback
from typing import Any

from src.utils.logger import get_logger
from src.ingest.ingestor import Ingestor
from src.transform.transformer import Transformer
from src.load.loader import Loader

logger = get_logger("etl.main")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ETL Pipeline — MongoDB")
    parser.add_argument(
        "--stage",
        choices=["ingest", "transform", "load"],
        default=None,
        help="Run a single stage instead of the full pipeline",
    )
    return parser.parse_args()


def run_pipeline(stage: str | None = None) -> None:
    """Orchestrate the Extract Transform Load pipeline."""
    logger.info("Pipeline started", extra={"stage_filter": stage or "all"})

    ingestor = Ingestor()
    transformer = Transformer()
    loader = Loader()

    total_ingested = 0
    total_transformed = 0
    total_loaded = 0

    try:
        for batch in ingestor.extract():
            total_ingested += len(batch)

            if stage == "ingest":
                continue

            clean_batch = transformer.transform_batch(batch)
            total_transformed += len(clean_batch)

            if stage == "transform":
                continue

            affected = loader.load_batch(clean_batch)
            total_loaded += affected

    finally:
        loader.close()

    logger.info(
        "Pipeline finished — ingested=%d  transformed=%d  loaded=%d",
        total_ingested,
        total_transformed,
        total_loaded,
    )


def main() -> None:
    args = parse_args()
    try:
        run_pipeline(stage=args.stage)
    except Exception:
        logger.error("Pipeline failed with an unrecoverable error")
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
