from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import boto3
from dotenv import load_dotenv


# =========================================================
# Project paths
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]

RAW_DATA_DIR = PROJECT_ROOT / "Data" / "raw"


# =========================================================
# Configuration
# =========================================================

load_dotenv()

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")


if not S3_BUCKET_NAME:
    raise ValueError(
        "S3_BUCKET_NAME is not set in .env"
    )


# =========================================================
# Tables
# =========================================================

TABLES = [
    "customers",
    "accounts",
    "merchants",
    "exchange_rates",
    "transactions",
    "transaction_events",
]


# =========================================================
# Logging
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# =========================================================
# S3 Client
# =========================================================

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
)


# =========================================================
# Upload function
# =========================================================

def upload_table(table_name: str) -> None:
    """
    Upload one Parquet table from local raw storage
    to the S3 raw layer.
    """

    local_file = RAW_DATA_DIR / f"{table_name}.parquet"

    if not local_file.exists():
        raise FileNotFoundError(
            f"Local file not found: {local_file}"
        )
    s3_key = (
        f"raw/{table_name}/"
        f"{table_name}.parquet"
    )

    logger.info(
        "Uploading %s...",
        local_file.name,
    )

    s3.upload_file(
        str(local_file),
        S3_BUCKET_NAME,
        s3_key,
    )

    logger.info(
        "Uploaded successfully: "
        "s3://%s/%s",
        S3_BUCKET_NAME,
        s3_key,
    )


# =========================================================
# Main
# =========================================================

def main() -> None:

    parser = argparse.ArgumentParser(
        description=(
            "Batch ingest FinStream raw Parquet "
            "datasets into Amazon S3."
        )
    )

    parser.add_argument(
        "--table",
        choices=TABLES,
        help=(
            "Upload only one table. "
            "If omitted, all tables are uploaded."
        ),
    )

    args = parser.parse_args()

    if args.table:
        tables_to_upload = [args.table]
    else:
        tables_to_upload = TABLES

    logger.info(
        "Starting batch ingestion..."
    )

    logger.info(
        "Tables selected: %s",
        ", ".join(tables_to_upload),
    )

    successful = 0
    failed = 0

    for table in tables_to_upload:

        try:
            upload_table(table)
            successful += 1

        except Exception as error:
            failed += 1

            logger.error(
                "Failed to upload %s: %s",
                table,
                error,
            )

    logger.info(
        "Batch ingestion completed."
    )

    logger.info(
        "Successful: %d | Failed: %d",
        successful,
        failed,
    )

    if failed > 0:
        raise RuntimeError(
            f"{failed} table(s) failed during ingestion."
        )


if __name__ == "__main__":
    main()
