from __future__ import annotations

import argparse
import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from faker import Faker
from kafka import KafkaProducer


# ---------------------------------------------------------
# Project paths
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[3]

RAW_DATA_DIR = PROJECT_ROOT / "Data" / "raw"

ACCOUNTS_FILE = RAW_DATA_DIR / "accounts.parquet"
MERCHANTS_FILE = RAW_DATA_DIR / "merchants.parquet"


# ---------------------------------------------------------
# Faker
# ---------------------------------------------------------

fake = Faker("en_IN")


# ---------------------------------------------------------
# Kafka configuration
# ---------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

KAFKA_TOPIC = "finstream.transactions"


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

TRANSACTION_TYPES = [
    "purchase",
    "withdrawal",
    "deposit",
    "transfer",
    "payment",
]

PAYMENT_METHODS = [
    "card",
    "upi",
    "bank_transfer",
    "cash",
    "online",
]

TRANSACTION_STATUSES = [
    "completed",
    "pending",
    "failed",
    "reversed",
]

CURRENCIES = [
    "INR",
    "USD",
    "EUR",
    "GBP",
    "AED",
    "SGD",
]


# ---------------------------------------------------------
# Load source data
# ---------------------------------------------------------

def load_source_data() -> tuple[pd.DataFrame, pd.DataFrame]:

    if not ACCOUNTS_FILE.exists():
        raise FileNotFoundError(
            f"Accounts file not found: {ACCOUNTS_FILE}"
        )

    if not MERCHANTS_FILE.exists():
        raise FileNotFoundError(
            f"Merchants file not found: {MERCHANTS_FILE}"
        )

    accounts = pd.read_parquet(ACCOUNTS_FILE)
    merchants = pd.read_parquet(MERCHANTS_FILE)

    if accounts.empty:
        raise ValueError("Accounts dataset is empty.")

    if merchants.empty:
        raise ValueError("Merchants dataset is empty.")

    return accounts, merchants


# ---------------------------------------------------------
# Transaction generation
# ---------------------------------------------------------

def generate_transaction(
    account_ids: list[str],
    merchant_ids: list[str],
    transaction_number: int,
) -> dict:

    transaction_type = random.choice(
        TRANSACTION_TYPES
    )

    # Merchant is primarily relevant for
    # purchase/payment transactions.
    if transaction_type in {
        "purchase",
        "payment",
    }:
        merchant_id = random.choice(merchant_ids)
    else:
        merchant_id = None

    # Generate realistic amounts based
    # on transaction type.
    if transaction_type == "purchase":

        amount = round(
            random.uniform(50, 50_000),
            2,
        )

    elif transaction_type == "withdrawal":

        amount = round(
            random.uniform(500, 50_000),
            2,
        )

    elif transaction_type == "deposit":

        amount = round(
            random.uniform(1_000, 200_000),
            2,
        )

    elif transaction_type == "transfer":

        amount = round(
            random.uniform(500, 100_000),
            2,
        )

    else:

        amount = round(
            random.uniform(100, 50_000),
            2,
        )

    transaction = {
        "transaction_id": (
            f"stream_txn_{transaction_number:012d}"
        ),

        "account_id": random.choice(
            account_ids
        ),

        "merchant_id": merchant_id,

        # Unlike the historical generator,
        # streaming events use the current timestamp.
        "transaction_timestamp": (
            datetime.now(timezone.utc)
            .isoformat()
        ),

        "transaction_type": transaction_type,

        "amount": amount,

        "currency": random.choice(
            CURRENCIES
        ),

        "transaction_status": random.choices(
            TRANSACTION_STATUSES,
            weights=[
                88,
                5,
                5,
                2,
            ],
            k=1,
        )[0],

        "payment_method": random.choice(
            PAYMENT_METHODS
        ),

        "description": fake.sentence(
            nb_words=6
        ),
    }

    return transaction


# ---------------------------------------------------------
# Kafka producer
# ---------------------------------------------------------

def create_kafka_producer() -> KafkaProducer:

    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

        value_serializer=lambda value: json.dumps(
            value
        ).encode("utf-8"),

        key_serializer=lambda key: key.encode(
            "utf-8"
        ) if key else None,

        acks="all",

        retries=5,
    )


# ---------------------------------------------------------
# Start streaming
# ---------------------------------------------------------

def stream_transactions(
    producer: KafkaProducer,
    account_ids: list[str],
    merchant_ids: list[str],
    interval: float,
) -> None:

    transaction_number = 1

    print("=" * 60)
    print("FinStream Transaction Producer")
    print("=" * 60)
    print(
        f"Kafka server : {KAFKA_BOOTSTRAP_SERVERS}"
    )
    print(
        f"Kafka topic  : {KAFKA_TOPIC}"
    )
    print(
        f"Interval     : {interval} seconds"
    )
    print("=" * 60)
    print("Streaming transactions...")
    print("Press CTRL+C to stop.")
    print()

    try:

        while True:

            transaction = generate_transaction(
                account_ids=account_ids,
                merchant_ids=merchant_ids,
                transaction_number=transaction_number,
            )

            transaction_id = transaction[
                "transaction_id"
            ]

            producer.send(
                KAFKA_TOPIC,
                key=transaction_id,
                value=transaction,
            )

            producer.flush()

            print(
                f"[SENT] "
                f"{transaction_id} | "
                f"{transaction['transaction_type']} | "
                f"{transaction['amount']} "
                f"{transaction['currency']} | "
                f"{transaction['transaction_status']}"
            )

            transaction_number += 1

            time.sleep(interval)

    except KeyboardInterrupt:

        print(
            "\nStopping transaction producer..."
        )

    finally:

        producer.flush()
        producer.close()

        print(
            "Kafka producer closed."
        )


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main() -> None:

    parser = argparse.ArgumentParser(
        description=(
            "FinStream real-time transaction "
            "producer using Kafka."
        )
    )

    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help=(
            "Seconds between transactions. "
            "Default: 1 second."
        ),
    )

    args = parser.parse_args()

    if args.interval <= 0:
        raise ValueError(
            "Interval must be greater than zero."
        )

    # Load existing FinStream datasets.
    accounts, merchants = load_source_data()

    account_ids = accounts[
        "account_id"
    ].astype(str).tolist()

    merchant_ids = merchants[
        "merchant_id"
    ].astype(str).tolist()

    # Create Kafka producer.
    producer = create_kafka_producer()

    # Start continuous stream.
    stream_transactions(
        producer=producer,
        account_ids=account_ids,
        merchant_ids=merchant_ids,
        interval=args.interval,
    )


if __name__ == "__main__":
    main()