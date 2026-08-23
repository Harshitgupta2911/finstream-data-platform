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

CUSTOMERS_FILE = RAW_DATA_DIR / "customers.parquet"
ACCOUNTS_FILE = RAW_DATA_DIR / "accounts.parquet"


# ---------------------------------------------------------
# Faker
# ---------------------------------------------------------

fake = Faker("en_IN")


# ---------------------------------------------------------
# Account generator
# ---------------------------------------------------------

def generate_accounts(
    customers: pd.DataFrame,
    count: int,
) -> pd.DataFrame:
    """Generate synthetic account records."""

    customer_ids = customers["customer_id"].tolist()

    accounts = []

    for i in range(1, count + 1):

        customer_id = random.choice(customer_ids)
        opened_at = fake.date_time_between(
        start_date="-5y",
        end_date="now",
        tzinfo=timezone.utc,
    )

        updated_at = fake.date_time_between(
        start_date=opened_at,
        end_date="now",
        tzinfo=timezone.utc,
    )

        accounts.append(
            {
                "account_id": f"acc_{i:08d}",

                "customer_id": customer_id,

                "account_type": random.choice(
                    [
                        "checking",
                        "savings",
                        "investment",
                    ]
                ),

                "currency": random.choice(
                    [
                        "INR",
                        "USD",
                        "EUR",
                        "GBP",
                    ]
                ),

                "balance": round(
                    random.uniform(500, 500_000),
                    2,
                ),

                "account_status": random.choices(
                    [
                        "active",
                        "inactive",
                        "frozen",
                    ],
                    weights=[
                        85,
                        10,
                        5,
                    ],
                    k=1,
                )[0],

                "opened_at": opened_at,
                "updated_at":updated_at,
            }
        )

    return pd.DataFrame(accounts)


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main() -> None:

    parser = argparse.ArgumentParser(
        description="Generate synthetic FinStream account data."
    )

    parser.add_argument(
        "--count",
        type=int,
        default=15_000,
        help="Number of accounts to generate.",
    )

    args = parser.parse_args()

    if args.count <= 0:
        raise ValueError(
            "Account count must be greater than zero."
        )

    # -----------------------------------------------------
    # Check customer data
    # -----------------------------------------------------

    if not CUSTOMERS_FILE.exists():
        raise FileNotFoundError(
            f"Customer data not found: {CUSTOMERS_FILE}"
        )

    customers = pd.read_parquet(
        CUSTOMERS_FILE
    )

    if customers.empty:
        raise ValueError(
            "Customer dataset is empty."
        )

    # -----------------------------------------------------
    # Generate accounts
    # -----------------------------------------------------

    accounts = generate_accounts(
        customers,
        args.count,
    )

    # -----------------------------------------------------
    # Create output directory
    # -----------------------------------------------------

    RAW_DATA_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # -----------------------------------------------------
    # Save Parquet
    # -----------------------------------------------------

    accounts.to_parquet(
        ACCOUNTS_FILE,
        index=False,
    )

    print(
        f"Generated {len(accounts):,} accounts."
    )

    print(
        f"Output: {ACCOUNTS_FILE}"
    )

    print(
        f"Columns: {', '.join(accounts.columns)}"
    )


if __name__ == "__main__":
    main()