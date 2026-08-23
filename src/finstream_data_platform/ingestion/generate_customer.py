from __future__ import annotations

import argparse
import random
from datetime import timezone
from pathlib import Path

import pandas as pd
from faker import Faker

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RAW_DATA_DIR = PROJECT_ROOT / "Data" / "raw"

fake = Faker("en_IN")
def generate_customers(count: int) -> pd.DataFrame:
    """Generate synthetic customer records."""

    customers = []

    for i in range(1, count + 1):
        created_at= fake.date_time_between(
                            start_date="-5y",
                            end_date="now",
                            tzinfo=timezone.utc,
                        )
        updated_at = fake.date_time_between(
        start_date=created_at,
        end_date="now"
                      )
        customers.append(
            {
                "customer_id": f"cust_{i:06d}",
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": fake.unique.email(),
                "phone": fake.phone_number(),
                "date_of_birth": fake.date_of_birth(
                    minimum_age=18,
                    maximum_age=75,
                ),
                "country": "IN",
                "created_at": created_at,
                "updated_at": updated_at,
                "customer_status": random.choice(
                    [
                        "active",
                        "active",
                        "active",
                        "inactive",
                    ]
                ),
            }
        )

    return pd.DataFrame(customers)
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate synthetic FinStream customer data."
    )

    parser.add_argument(
        "--count",
        type=int,
        default=10_000,
        help="Number of customers to generate.",
    )

    args = parser.parse_args()

    if args.count <= 0:
        raise ValueError("Customer count must be greater than zero.")

    # Create Data/raw if it doesn't exist
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Generate data
    df = generate_customers(args.count)

    # Output file
    output_file = RAW_DATA_DIR / "customers.parquet"

    # Save as Parquet
    df.to_parquet(
        output_file,
        index=False,
    )

    print(f"Generated {len(df):,} customers.")
    print(f"Output: {output_file}")
    print(f"Columns: {', '.join(df.columns)}")


if __name__ == "__main__":
    main()