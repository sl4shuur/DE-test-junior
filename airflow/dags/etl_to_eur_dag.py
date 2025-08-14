import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Import our custom functions
import sys

sys.path.append("/opt/airflow")
from src.tasks.etl_converter import (
    extract_orders_from_source,
    transform_orders_to_eur,
    load_orders_to_target,
)
from src.services.exchange_rates import get_exchange_rates


def extract_orders(**context):
    """Extract orders from postgres-1 for the last hour"""

    source_hook = PostgresHook(postgres_conn_id="postgres_1")
    source_dsn = source_hook.get_uri()

    # Extract orders from last hour
    orders = extract_orders_from_source(source_dsn, hours_back=1)

    logging.info(f"Extracted {len(orders)} orders from source database")

    # Pass data to next task via XCom
    return orders


def transform_orders(**context):
    """Transform orders to EUR using current exchange rates"""

    # Get orders from previous task
    task_instance = context["task_instance"]
    orders = task_instance.xcom_pull(task_ids="extract_orders")

    if not orders:
        logging.info("No orders to transform")
        return []

    # Get current exchange rates
    rates, timestamp = get_exchange_rates()
    logging.info(f"Retrieved exchange rates with timestamp: {timestamp}")

    # Transform orders to EUR
    eur_orders = transform_orders_to_eur(orders, rates)

    logging.info(f"Transformed {len(eur_orders)} orders to EUR")

    # Pass transformed data to next task
    return eur_orders


def load_orders(**context):
    """Load EUR orders to postgres-2"""

    target_hook = PostgresHook(postgres_conn_id="postgres_2")
    target_dsn = target_hook.get_uri()

    # Get transformed orders from previous task
    task_instance = context["task_instance"]
    eur_orders = task_instance.xcom_pull(task_ids="transform_orders")

    if not eur_orders:
        logging.info("No orders to load")
        return 0

    # Load orders to target database
    loaded_count = load_orders_to_target(target_dsn, eur_orders)

    logging.info(f"Loaded {loaded_count} EUR orders to target database")
    return loaded_count


default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 13),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "etl_orders_to_eur_hourly",
    default_args=default_args,
    description="ETL pipeline: Extract orders → Transform to EUR → Load to target DB",
    schedule_interval="0 * * * *",  # Every hour at minute 0
    catchup=False,
    tags=["orders", "etl", "currency"],
)

# Task 1: Extract
extract_task = PythonOperator(
    task_id="extract_orders",
    python_callable=extract_orders,
    dag=dag,
)

# Task 2: Transform
transform_task = PythonOperator(
    task_id="transform_orders",
    python_callable=transform_orders,
    dag=dag,
)

# Task 3: Load
load_task = PythonOperator(
    task_id="load_orders",
    python_callable=load_orders,
    dag=dag,
)

# Define task dependencies
extract_task >> transform_task >> load_task
