import json
import time
from datetime import datetime, timezone

import boto3
from kafka import KafkaConsumer


# =========================================================
# Configuration
# =========================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "finstream.transactions"

# New consumer group for testing
KAFKA_GROUP_ID = "finstream-stream-transactions-s3-v2"

S3_BUCKET = "finstream-data-ingestion"
S3_PREFIX = "raw/stream_transactions"

BATCH_SIZE = 10
BATCH_TIMEOUT_SECONDS = 10


# =========================================================
# S3
# =========================================================

s3 = boto3.client("s3")


# =========================================================
# Kafka
# =========================================================

consumer = KafkaConsumer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=KAFKA_GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    ),
)

consumer.subscribe([KAFKA_TOPIC])


print("=" * 70)
print("Kafka → S3 Streaming Consumer")
print("=" * 70)
print(f"Kafka : {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Topic : {KAFKA_TOPIC}")
print(f"S3    : s3://{S3_BUCKET}/{S3_PREFIX}/")
print("=" * 70)

print("\nWaiting for Kafka messages...\n")


# =========================================================
# S3 Writer
# =========================================================

def write_batch_to_s3(records):

    if not records:
        return

    timestamp = datetime.now(timezone.utc).strftime(
        "%Y%m%d_%H%M%S_%f"
    )

    key = (
        f"{S3_PREFIX}/"
        f"stream_transactions_{timestamp}.jsonl"
    )

    body = "\n".join(
        json.dumps(record, separators=(",", ":"))
        for record in records
    )

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )

    print(
        f"\n[S3] Wrote {len(records)} records"
    )

    print(
        f"[S3] s3://{S3_BUCKET}/{key}\n"
    )


# =========================================================
# Streaming Loop
# =========================================================

batch = []
batch_start = time.time()

try:

    while True:

        # Poll Kafka every second
        records = consumer.poll(
            timeout_ms=1000
        )

        # -------------------------------------------------
        # Process received messages
        # -------------------------------------------------

        for topic_partition, messages in records.items():

            for message in messages:

                record = message.value

                batch.append(record)

                print(
                    f"[Kafka] "
                    f"{record.get('transaction_id')} | "
                    f"{record.get('transaction_type')} | "
                    f"{record.get('amount')} "
                    f"{record.get('currency')}"
                )

        # -------------------------------------------------
        # Write when batch reaches 10
        # -------------------------------------------------

        if len(batch) >= BATCH_SIZE:

            write_batch_to_s3(batch)

            batch.clear()

            batch_start = time.time()

        # -------------------------------------------------
        # Write after 10 seconds
        # -------------------------------------------------

        if batch and (
            time.time() - batch_start
            >= BATCH_TIMEOUT_SECONDS
        ):

            write_batch_to_s3(batch)

            batch.clear()

            batch_start = time.time()


except KeyboardInterrupt:

    print("\nStopping consumer...")

    if batch:
        write_batch_to_s3(batch)

finally:

    consumer.close()

    print("Kafka consumer closed.")