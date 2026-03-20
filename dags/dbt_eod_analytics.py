"""
DAG: Xử lý dữ liệu End-of-Day (EOD) bằng dbt
Lên lịch bởi Dataset("bigquery://bronze_historical_daily") từ DAG vn30_daily_incremental
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.datasets import Dataset
from datetime import datetime, timedelta
import pendulum

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

# Đường dẫn dbt project
DBT_PROJECT_DIR = '/opt/airflow/dbt'

default_args = {
    'owner': 'data_eng_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

with DAG(
    dag_id='dbt_eod_analytics',
    default_args=default_args,
    description='Xử lý dữ liệu daily & 1m: staging, fact tables, marts',
    # Dataset-triggered: được kích hoạt khi bronze_historical_daily được cập nhật
    schedule=[Dataset("bigquery://bronze_historical_daily")],
    start_date=datetime(2026, 3, 1, tzinfo=local_tz),
    tags=['dbt', 'eod', 'batch'],
    catchup=False,
) as dag:

    # Task dbt: Chạy các model staging, fact, và marts
    dbt_run_eod_analytics = BashOperator(
        task_id='dbt_run_eod_models',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select stg_historical_daily stg_historical_1m fact_stock_daily_base fact_stock_daily fact_stock_1m",
        do_xcom_push=False,
    )

    dbt_run_eod_analytics
