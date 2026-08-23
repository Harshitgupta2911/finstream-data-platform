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
MERCHANTS_FILE = RAW_DATA_DIR / "merchants.parquet"


# ---------------------------------------------------------
# Faker
# ---------------------------------------------------------

fake = Faker("en_IN")


# ---------------------------------------------------------
# Merchant configuration
# ---------------------------------------------------------

MERCHANT_CATEGORIES = [
    "grocery",
    "restaurant",
    "ecommerce",
    "electronics",
    "travel",
    "healthcare",
    "fuel",
    "entertainment",
    "utilities",
    "education",
]

COUNTRIES = [
    "IN",
    "US",
    "GB",
    "DE",
    "AE",
    "SG",
]

CURRENCIES = {
    "IN": "INR",
    "US": "USD",
    "GB": "GBP",
    "DE": "EUR",
    "AE": "AED",
    "SG": "SGD",
}


# ---------------------------------------------------------
# Merchant generator
# ---------------------------------------------------------

def generate_merchants(count: int) -> pd.DataFrame:
    """Generate synthetic merchant records."""

    merchants = []

    for i in range(1, count + 1):

        country = random.choice(COUNTRIES)
        created_at = fake.date_time_between(
            start_date="-5y",
            end_date="now",
            tzinfo=timezone.utc,
        )
        updated_at = fake.date_time_between(
            start_date=created_at,
            end_date="now"
        )

        merchants.append(
            {
                "merchant_id": f"mer_{i:06d}",

                "merchant_name": fake.company(),

                "merchant_category": random.choice(
                    MERCHANT_CATEGORIES
                ),

                "country": country,

                "city": fake.city(),

                "currency": CURRENCIES[country],

                "merchant_status": random.choices(
                    [
                        "active",
                        "inactive",
                    ],
                    weights=[
                        90,
                        10,
                    ],
                    k=1,
                )[0],

                "created_at": created_at,
                "updated_at": updated_at,
            }
        )

    return pd.DataFrame(merchants)


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main() -> None:

    parser = argparse.ArgumentParser(
        description="Generate synthetic FinStream merchant data."
    )

    parser.add_argument(
        "--count",
        type=int,
        default=2_000,
        help="Number of merchants to generate.",
    )

    args = parser.parse_args()

    if args.count <= 0:
        raise ValueError(
            "Merchant count must be greater than zero."
        )

    # Create raw directory if necessary
    RAW_DATA_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Generate merchants
    merchants = generate_merchants(
        args.count
    )

    # Save as Parquet
    merchants.to_parquet(
        MERCHANTS_FILE,
        index=False,
    )

    print(
        f"Generated {len(merchants):,} merchants."
    )

    print(
        f"Output: {MERCHANTS_FILE}"
    )

    print(
        f"Columns: {', '.join(merchants.columns)}"
    )


if __name__ == "__main__":
    main()
