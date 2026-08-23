import os
import pendulum
from airflow.sdk import dag
from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
)
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from dotenv import load_dotenv
import os
load_dotenv()


def _required_env_int(var_name: str) -> int:
    """
    Read a required Databricks job ID from the Airflow environment.
    """

    value = os.getenv(var_name)

    if value is None:
        raise ValueError(
            f"{var_name} is not set in the Airflow environment."
        )

    return int(value)


@dag(
    dag_id="finstream_streaming_pipeline",
    schedule="*/10 * * * *",
    catchup=False,
    start_date=pendulum.datetime(
        2026,
        8,
        1,
        tz="Asia/Kolkata",
    ),
    tags=[
        "finstream",
        "streaming",
        "kafka",
        "s3",
        "databricks",
    ],
)
def finstream_streaming_pipeline():

    # ---------------------------------------------------------
    # 1. Check that Kafka → S3 has produced streaming files
    # ---------------------------------------------------------

    from airflow.decorators import task

    @task
    def check_streaming_s3():

        hook = S3Hook(
            aws_conn_id="aws_finstream"
        )

        keys = hook.list_keys(
            bucket_name="finstream-data-ingestion",
            prefix="raw/stream_transactions/",
        )

        if not keys:

            raise ValueError(
                "No streaming transaction files found in "
                "S3: raw/stream_transactions/"
            )

        print(
            f"Found {len(keys)} streaming objects in S3."
        )

        for key in keys[-10:]:
            print(f"  {key}")

    # ---------------------------------------------------------
    # 2. Databricks AvailableNow ingestion
    # ---------------------------------------------------------

    streaming_bronze_ingestion = DatabricksRunNowOperator(
        task_id="streaming_bronze_ingestion",

        databricks_conn_id="databricks_default",

        job_id=os.getenv(
            "STREAMING_BRONZE_JOB_ID"
        ),
    )

    # ---------------------------------------------------------
    # 3. Bronze quality checks
    # ---------------------------------------------------------

    bronze_quality_check = DatabricksRunNowOperator(
        task_id="streaming_bronze_quality_check",

        databricks_conn_id="databricks_default",

        job_id=_required_env_int(
            "BRONZE_QUALITY_JOB_ID"
        ),
    )

    # ---------------------------------------------------------
    # Dependency
    # ---------------------------------------------------------

    s3_check = check_streaming_s3()

    (
        s3_check
        >> streaming_bronze_ingestion
        >> bronze_quality_check
    )


finstream_streaming_dag = finstream_streaming_pipeline()
