"""
DAG: Xử lý dữ liệu Intraday Ticks bằng dbt
Lên lịch bởi Dataset("bigquery://stock_raw_daily") từ DAG stock_microbatch_to_bq
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
    dag_id='dbt_tick_analytics',
    default_args=default_args,
    description='Xử lý dữ liệu ticks realtime: stg_stock_ticks, buy_sell_pressure, whale_transactions',
    # Dataset-triggered: được kích hoạt khi stock_raw_daily được cập nhật
    schedule=[Dataset("bigquery://stock_raw_daily")],
    start_date=datetime(2026, 3, 1, tzinfo=local_tz),
    tags=['dbt', 'intraday', 'realtime'],
    catchup=False,
) as dag:

    # Task dbt: Chạy các model liên quan đến ticks
    dbt_run_tick_analytics = BashOperator(
        task_id='dbt_run_tick_models',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select stg_stock_ticks fact_buy_sell_pressure fact_whale_transactions",
        do_xcom_push=False,
    )

    dbt_run_tick_analytics
