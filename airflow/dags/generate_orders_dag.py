import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Import our custom functions
import sys
sys.path.append('/opt/airflow')
from src.tasks.generator import generate_orders
from src.repositories.postgres_repo import ensure_orders_table, insert_orders

def generate_and_insert_orders(**context):
    """Generate orders and insert them into postgres-1"""
    
    # Get connection via Airflow Hook
    hook = PostgresHook(postgres_conn_id="postgres_1")
    dsn = hook.get_uri()
    
    # Ensure table exists
    ensure_orders_table(dsn)
    
    # Generate orders
    now = datetime.now(timezone.utc)
    orders = generate_orders(n=5000, now=now, seed=None)  # Random seed each time
    
    # Insert orders
    inserted = insert_orders(dsn, orders)

    logging.info(f"Generated and inserted {inserted} orders at {now}")
    return inserted

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'generate_orders_every_10min',
    default_args=default_args,
    description='Generate 5000 orders every 10 minutes',
    schedule_interval='*/10 * * * *',  # Every 10 minutes
    catchup=False,
    tags=['orders', 'generator'],
)

generate_task = PythonOperator(
    task_id='generate_and_insert_5000_orders',
    python_callable=generate_and_insert_orders,
    dag=dag,
)