from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.standard.operators.bash import BashOperator
from finstream_data_platform.ingestion.batch_ingestion import upload_table, TABLES, RAW_DATA_DIR

import pendulum
import os
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()


def _required_env_int(var_name: str) -> int:
    """
    Fail loudly and clearly at DAG-parse time if a required Databricks job ID
    env var is missing, instead of silently resolving to None/0 and producing
    a confusing 'Job 0 does not exist' error from the Databricks API later.
    """
    value = os.getenv(var_name)
    if value is None:
        raise ValueError(
            f"{var_name} is not set in the Airflow environment. "
            "Set it in your Airflow environment (e.g. docker-compose "
            "environment block), not just a local .env file."
        )
    return int(value)


@dag(
    dag_id="finstream_bronze_pipeline",
    schedule=None,
    catchup=False,
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Kolkata"),
    tags=["finstream", "bronze", "databricks"],
)
def finstream_pipeline():

    @task
    def generate_data():
        from finstream_data_platform.ingestion.generate_customer import generate_customers
        from finstream_data_platform.ingestion.generate_merchant import generate_merchants
        from finstream_data_platform.ingestion.generate_exchange_rate import generate_exchange_rates
        from finstream_data_platform.ingestion.generate_account import generate_accounts
        from finstream_data_platform.ingestion.generate_transactions import generate_transactions
        from finstream_data_platform.ingestion.generate_transaction_events import generate_transaction_events

        RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

        # Independent generators
        customers = generate_customers(10_000)
        customers.to_parquet(RAW_DATA_DIR / "customers.parquet", index=False)

        merchants = generate_merchants(2_000)
        merchants.to_parquet(RAW_DATA_DIR / "merchants.parquet", index=False)

        start_date = date.today() - timedelta(days=364)
        exchange_rates = generate_exchange_rates(
            start_date=start_date, number_of_days=365)
        exchange_rates.to_parquet(
            RAW_DATA_DIR / "exchange_rates.parquet", index=False)

        # Depends on customers
        accounts = generate_accounts(customers, 15_000)
        accounts.to_parquet(RAW_DATA_DIR / "accounts.parquet", index=False)

        # Depends on accounts + merchants
        transactions = generate_transactions(accounts, merchants, 100_000)
        transactions.to_parquet(
            RAW_DATA_DIR / "transactions.parquet", index=False)

        # Depends on transactions
        events = generate_transaction_events(transactions)
        events.to_parquet(
            RAW_DATA_DIR / "transaction_events.parquet", index=False)

        print("All 6 raw tables generated:")
        print(f"  customers:          {len(customers):,}")
        print(f"  merchants:          {len(merchants):,}")
        print(f"  exchange_rates:     {len(exchange_rates):,}")
        print(f"  accounts:           {len(accounts):,}")
        print(f"  transactions:       {len(transactions):,}")
        print(f"  transaction_events: {len(events):,}")

    @task
    def upload_to_s3():
        failed = []
        for table in TABLES:
            try:
                upload_table(table)
            except Exception as e:
                failed.append((table, str(e)))
        if failed:
            raise RuntimeError(f"{len(failed)} table(s) failed: {failed}")

    @task
    def check_s3_raw():
        hook = S3Hook(aws_conn_id="aws_finstream")

        keys = hook.list_keys(
            bucket_name="finstream-data-ingestion",
            prefix="raw/",
        )

        if not keys:
            raise ValueError("No raw data found in S3.")

        print(f"Found {len(keys)} objects in S3.")

    bronze_ingestion = DatabricksRunNowOperator(
        task_id="bronze_ingestion",
        databricks_conn_id="databricks_default",
        job_id=_required_env_int("BRONZE_JOB_ID"),
    )
    bronze_quality_check = DatabricksRunNowOperator(
        task_id="bronze_quality_check",
        databricks_conn_id="databricks_default",
        job_id=_required_env_int("BRONZE_QUALITY_JOB_ID"),
    )
    silver_transformation = DatabricksRunNowOperator(
        task_id="silver_transformation",
        databricks_conn_id="databricks_default",
        job_id=_required_env_int("SILVER_TRANSFORMATION_JOB_ID"),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        cwd="/opt/airflow/dbt/finstream",
        bash_command=(
            "dbt build "
            "--target {{ var.value.get('dbt_target', 'dev') }} "
            "--vars '{\"execution_date\": \"{{ ds }}\"}'"
        ),
        env={"DBT_PROFILES_DIR": "/opt/airflow/dbt/finstream"},
        append_env=True,
    )

    gen_data = generate_data()
    up_load_s3 = upload_to_s3()
    s3_check = check_s3_raw()

    gen_data >> up_load_s3 >> s3_check >> bronze_ingestion >> bronze_quality_check >> silver_transformation
    silver_transformation >> dbt_build


finstream_dag = finstream_pipeline()
