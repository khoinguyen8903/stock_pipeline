"""
DAG: Xử lý Dimension Master (VN30 Weekly) bằng dbt
Lên lịch bởi Dataset("bigquery://dim_company") từ DAG update_dim_company_weekly
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
    dag_id='dbt_weekly_master',
    default_args=default_args,
    description='Xử lý Dimension Master: stg_company & dim_company_gold',
    # Dataset-triggered: được kích hoạt khi dim_company được cập nhật
    schedule=[Dataset("bigquery://dim_company")],
    start_date=datetime(2026, 3, 1, tzinfo=local_tz),
    tags=['dbt', 'weekly', 'dimension'],
    catchup=False,
) as dag:

    # Task dbt: Chạy các model staging company & dimension gold
    dbt_run_weekly_master = BashOperator(
        task_id='dbt_run_dimension_models',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select stg_company dim_company_gold",
        do_xcom_push=False,
    )

    dbt_run_weekly_master
