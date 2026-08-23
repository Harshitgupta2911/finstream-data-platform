from __future__ import annotations

import argparse
import random
from datetime import date, timedelta
from pathlib import Path

import pandas as pd


# ---------------------------------------------------------
# Project paths
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[3]

RAW_DATA_DIR = PROJECT_ROOT / "Data" / "raw"

OUTPUT_FILE = RAW_DATA_DIR / "exchange_rates.parquet"


# ---------------------------------------------------------
# Currency configuration
# ---------------------------------------------------------

CURRENCIES = [
    "INR",
    "USD",
    "EUR",
    "GBP",
    "AED",
    "SGD",
]


# Approximate synthetic base rates.
# These are NOT real market rates.
BASE_RATES = {
    ("INR", "USD"): 0.0120,
    ("INR", "EUR"): 0.0110,
    ("INR", "GBP"): 0.0095,
    ("INR", "AED"): 0.0440,
    ("INR", "SGD"): 0.0160,

    ("USD", "INR"): 83.0,
    ("USD", "EUR"): 0.92,
    ("USD", "GBP"): 0.79,
    ("USD", "AED"): 3.67,
    ("USD", "SGD"): 1.34,

    ("EUR", "INR"): 90.0,
    ("EUR", "USD"): 1.09,
    ("EUR", "GBP"): 0.86,
    ("EUR", "AED"): 4.00,
    ("EUR", "SGD"): 1.46,

    ("GBP", "INR"): 105.0,
    ("GBP", "USD"): 1.27,
    ("GBP", "EUR"): 1.16,
    ("GBP", "AED"): 4.65,
    ("GBP", "SGD"): 1.70,

    ("AED", "INR"): 22.6,
    ("AED", "USD"): 0.272,
    ("AED", "EUR"): 0.250,
    ("AED", "GBP"): 0.215,
    ("AED", "SGD"): 0.365,

    ("SGD", "INR"): 62.0,
    ("SGD", "USD"): 0.746,
    ("SGD", "EUR"): 0.685,
    ("SGD", "GBP"): 0.588,
    ("SGD", "AED"): 2.74,
}


# ---------------------------------------------------------
# Generator
# ---------------------------------------------------------

def generate_exchange_rates(
    start_date: date,
    number_of_days: int,
) -> pd.DataFrame:
    """Generate synthetic daily exchange-rate records."""

    records = []

    rate_id = 1

    for day_offset in range(number_of_days):

        current_date = start_date + timedelta(
            days=day_offset
        )

        for base_currency in CURRENCIES:

            for target_currency in CURRENCIES:

                # Don't create INR -> INR, USD -> USD, etc.
                if base_currency == target_currency:
                    continue

                base_rate = BASE_RATES[
                    (base_currency, target_currency)
                ]

                # Small synthetic daily variation
                variation = random.uniform(
                    0.995,
                    1.005,
                )

                rate = round(
                    base_rate * variation,
                    6,
                )

                records.append(
                    {
                        "exchange_rate_id": f"fx_{rate_id:08d}",
                        "base_currency": base_currency,
                        "target_currency": target_currency,
                        "rate": rate,
                        "rate_date": current_date,
                        "source": "synthetic_provider",
                    }
                )

                rate_id += 1

    return pd.DataFrame(records)


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main() -> None:

    parser = argparse.ArgumentParser(
        description="Generate synthetic FinStream exchange-rate data."
    )

    parser.add_argument(
        "--days",
        type=int,
        default=365,
        help="Number of days of exchange-rate history.",
    )

    args = parser.parse_args()

    if args.days <= 0:
        raise ValueError(
            "Number of days must be greater than zero."
        )

    RAW_DATA_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Generate one year of historical rates
    start_date = date.today() - timedelta(
        days=args.days - 1
    )

    exchange_rates = generate_exchange_rates(
        start_date=start_date,
        number_of_days=args.days,
    )

    exchange_rates.to_parquet(
        OUTPUT_FILE,
        index=False,
    )

    print(
        f"Generated {len(exchange_rates):,} exchange-rate records."
    )

    print(
        f"Output: {OUTPUT_FILE}"
    )

    print(
        f"Date range: "
        f"{exchange_rates['rate_date'].min()} "
        f"to "
        f"{exchange_rates['rate_date'].max()}"
    )

    print(
        f"Columns: "
        f"{', '.join(exchange_rates.columns)}"
    )


if __name__ == "__main__":
    main()
