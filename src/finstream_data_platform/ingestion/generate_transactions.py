from __future__ import annotations

import argparse
import random
from datetime import timezone
from pathlib import Path

import pandas as pd
from faker import Faker


# ---------------------------------------------------------
# Project paths
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[3]

RAW_DATA_DIR = PROJECT_ROOT / "Data" / "raw"

ACCOUNTS_FILE = RAW_DATA_DIR / "accounts.parquet"
MERCHANTS_FILE = RAW_DATA_DIR / "merchants.parquet"
OUTPUT_FILE = RAW_DATA_DIR / "transactions.parquet"


# ---------------------------------------------------------
# Faker
# ---------------------------------------------------------

fake = Faker("en_IN")


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

    accounts = pd.read_parquet(
        ACCOUNTS_FILE
    )

    merchants = pd.read_parquet(
        MERCHANTS_FILE
    )

    if accounts.empty:
        raise ValueError(
            "Accounts dataset is empty."
        )

    if merchants.empty:
        raise ValueError(
            "Merchants dataset is empty."
        )

    return accounts, merchants


# ---------------------------------------------------------
# Generate transactions
# ---------------------------------------------------------

def generate_transactions(
    accounts: pd.DataFrame,
    merchants: pd.DataFrame,
    count: int,
) -> pd.DataFrame:

    account_ids = accounts["account_id"].tolist()

    merchant_ids = merchants["merchant_id"].tolist()

    transactions = []

    for i in range(1, count + 1):

        transaction_type = random.choice(
            TRANSACTION_TYPES
        )

        # Deposits/withdrawals don't necessarily need
        # a merchant, but we'll keep merchant references
        # for purchase/payment transactions.
        if transaction_type in {
            "purchase",
            "payment",
        }:
            merchant_id = random.choice(
                merchant_ids
            )
        else:
            merchant_id = None

        # Generate some realistic transaction amounts.
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

        transactions.append(
            {
                "transaction_id": (
                    f"txn_{i:010d}"
                ),

                "account_id": random.choice(
                    account_ids
                ),

                "merchant_id": merchant_id,

                "transaction_timestamp": (
                    fake.date_time_between(
                        start_date="-1y",
                        end_date="now",
                        tzinfo=timezone.utc,
                    )
                ),

                "transaction_type": (
                    transaction_type
                ),

                "amount": amount,

                "currency": random.choice(
                    [
                        "INR",
                        "USD",
                        "EUR",
                        "GBP",
                        "AED",
                        "SGD",
                    ]
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
        )

    return pd.DataFrame(
        transactions
    )


# ---------------------------------------------------------
# Validate relationships
# ---------------------------------------------------------

def validate_transactions(
    transactions: pd.DataFrame,
    accounts: pd.DataFrame,
    merchants: pd.DataFrame,
) -> None:

    # Transaction IDs must be unique
    duplicate_transactions = (
        transactions["transaction_id"]
        .duplicated()
        .sum()
    )

    if duplicate_transactions > 0:
        raise ValueError(
            f"Found {duplicate_transactions} "
            "duplicate transaction IDs."
        )

    # Account IDs must exist
    invalid_accounts = (
        ~transactions["account_id"]
        .isin(accounts["account_id"])
    ).sum()

    if invalid_accounts > 0:
        raise ValueError(
            f"Found {invalid_accounts} "
            "transactions with invalid account IDs."
        )

    # Merchant IDs must exist when present
    merchant_transactions = transactions[
        transactions["merchant_id"].notna()
    ]

    invalid_merchants = (
        ~merchant_transactions["merchant_id"]
        .isin(merchants["merchant_id"])
    ).sum()

    if invalid_merchants > 0:
        raise ValueError(
            f"Found {invalid_merchants} "
            "transactions with invalid merchant IDs."
        )


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main() -> None:

    parser = argparse.ArgumentParser(
        description=(
            "Generate synthetic FinStream "
            "transaction data."
        )
    )

    parser.add_argument(
        "--count",
        type=int,
        default=100_000,
        help=(
            "Number of transactions "
            "to generate."
        ),
    )

    args = parser.parse_args()

    if args.count <= 0:
        raise ValueError(
            "Transaction count must be "
            "greater than zero."
        )

    # Load existing datasets
    accounts, merchants = (
        load_source_data()
    )

    # Generate transactions
    transactions = generate_transactions(
        accounts,
        merchants,
        args.count,
    )

    # Validate foreign-key relationships
    validate_transactions(
        transactions,
        accounts,
        merchants,
    )

    # Create output directory
    RAW_DATA_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Save Parquet
    transactions.to_parquet(
        OUTPUT_FILE,
        index=False,
    )

    print(
        f"Generated "
        f"{len(transactions):,} transactions."
    )

    print(
        f"Output: {OUTPUT_FILE}"
    )

    print(
        "Unique transaction IDs:",
        transactions["transaction_id"].nunique(),
    )

    print(
        "Invalid account IDs: 0"
    )

    print(
        "Invalid merchant IDs: 0"
    )


if __name__ == "__main__":
    main()