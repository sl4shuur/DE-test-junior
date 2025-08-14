import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Import our custom functions
import sys
sys.path.append('/opt/airflow')
from src.tasks.etl_converter import etl_hourly_conversion

def convert_orders_to_eur(**context):
    """Convert orders from postgres-1 to EUR in postgres-2"""
    
    # Get connections via Airflow Hooks
    source_hook = PostgresHook(postgres_conn_id="postgres_1")
    target_hook = PostgresHook(postgres_conn_id="postgres_2")
    
    source_dsn = source_hook.get_uri()
    target_dsn = target_hook.get_uri()
    
    # Run ETL process for last hour
    converted = etl_hourly_conversion(source_dsn, target_dsn, hours_back=1)
    
    logging.info(f"Converted {converted} orders to EUR at {datetime.now(timezone.utc)}")
    return converted

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'etl_orders_to_eur_hourly',
    default_args=default_args,
    description='Convert orders to EUR every hour',
    schedule_interval='0 * * * *',  # Every hour at minute 0
    catchup=False,
    tags=['orders', 'etl', 'currency'],
)

etl_task = PythonOperator(
    task_id='convert_orders_to_eur',
    python_callable=convert_orders_to_eur,
    dag=dag,
)