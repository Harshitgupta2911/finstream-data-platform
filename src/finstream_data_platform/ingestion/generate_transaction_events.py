from __future__ import annotations

import argparse
import random
from pathlib import Path

import pandas as pd


# ---------------------------------------------------------
# Project paths
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[3]

RAW_DATA_DIR = PROJECT_ROOT / "Data" / "raw"

TRANSACTIONS_FILE = RAW_DATA_DIR / "transactions.parquet"
OUTPUT_FILE = RAW_DATA_DIR / "transaction_events.parquet"


# ---------------------------------------------------------
# Event configuration
# ---------------------------------------------------------

EVENT_SEQUENCE = {
    "completed": [
        "initiated",
        "authorized",
        "completed",
    ],
    "pending": [
        "initiated",
        "authorized",
    ],
    "failed": [
        "initiated",
        "failed",
    ],
    "reversed": [
        "initiated",
        "authorized",
        "completed",
        "reversed",
    ],
}


FAILURE_REASONS = [
    "insufficient_funds",
    "merchant_declined",
    "fraud_detected",
    "network_error",
    "invalid_credentials",
]


# ---------------------------------------------------------
# Generate events
# ---------------------------------------------------------

def generate_transaction_events(
    transactions: pd.DataFrame,
) -> pd.DataFrame:

    events = []

    event_counter = 1

    for _, transaction in transactions.iterrows():

        transaction_id = transaction["transaction_id"]

        transaction_status = transaction[
            "transaction_status"
        ]

        transaction_timestamp = pd.Timestamp(
            transaction["transaction_timestamp"]
        )

        event_types = EVENT_SEQUENCE[
            transaction_status
        ]

        for event_index, event_type in enumerate(
            event_types
        ):

            # Events happen progressively after
            # the transaction timestamp.
            event_timestamp = (
                transaction_timestamp
                + pd.Timedelta(
                    seconds=random.randint(
                        1,
                        120,
                    ) * (event_index + 1)
                )
            )

            failure_reason = None

            if event_type == "failed":
                failure_reason = random.choice(
                    FAILURE_REASONS
                )

            events.append(
                {
                    "event_id": (
                        f"evt_{event_counter:010d}"
                    ),

                    "transaction_id": (
                        transaction_id
                    ),

                    "event_type": event_type,

                    "event_timestamp": (
                        event_timestamp
                    ),

                    "event_status": (
                        "success"
                        if event_type
                        != "failed"
                        else "failure"
                    ),

                    "failure_reason": (
                        failure_reason
                    ),
                }
            )

            event_counter += 1

    return pd.DataFrame(events)


# ---------------------------------------------------------
# Validation
# ---------------------------------------------------------

def validate_events(
    events: pd.DataFrame,
    transactions: pd.DataFrame,
) -> None:

    # Event IDs must be unique
    duplicate_event_ids = (
        events["event_id"]
        .duplicated()
        .sum()
    )

    if duplicate_event_ids > 0:
        raise ValueError(
            f"Found {duplicate_event_ids} "
            "duplicate event IDs."
        )

    # Every transaction_id must exist
    invalid_transactions = (
        ~events["transaction_id"]
        .isin(
            transactions["transaction_id"]
        )
    ).sum()

    if invalid_transactions > 0:
        raise ValueError(
            f"Found {invalid_transactions} "
            "events with invalid transaction IDs."
        )

    # Failed events must have a failure reason
    invalid_failures = (
        (
            events["event_type"] == "failed"
        )
        & events["failure_reason"].isna()
    ).sum()

    if invalid_failures > 0:
        raise ValueError(
            f"Found {invalid_failures} "
            "failed events without a reason."
        )


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main() -> None:

    parser = argparse.ArgumentParser(
        description=(
            "Generate synthetic FinStream "
            "transaction event data."
        )
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Optional number of transactions "
            "to generate events for."
        ),
    )

    args = parser.parse_args()

    if not TRANSACTIONS_FILE.exists():
        raise FileNotFoundError(
            f"Transactions file not found: "
            f"{TRANSACTIONS_FILE}"
        )

    transactions = pd.read_parquet(
        TRANSACTIONS_FILE
    )

    if transactions.empty:
        raise ValueError(
            "Transactions dataset is empty."
        )

    if args.limit is not None:

        if args.limit <= 0:
            raise ValueError(
                "Limit must be greater than zero."
            )

        transactions = transactions.head(
            args.limit
        )

    # Generate events
    events = generate_transaction_events(
        transactions
    )

    # Validate
    validate_events(
        events,
        transactions,
    )

    # Ensure output directory exists
    RAW_DATA_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Save
    events.to_parquet(
        OUTPUT_FILE,
        index=False,
    )

    print(
        f"Generated {len(events):,} "
        "transaction events."
    )

    print(
        f"Output: {OUTPUT_FILE}"
    )

    print(
        "Unique event IDs:",
        events["event_id"].nunique(),
    )

    print(
        "Unique transactions:",
        events["transaction_id"].nunique(),
    )

    print(
        "\nEvent types:"
    )

    print(
        events["event_type"].value_counts()
    )


if __name__ == "__main__":
    main()