from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'dangnhathuy',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Hàm đọc nội dung SQL từ đường dẫn tuyệt đối
def load_sql_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read()

with DAG(
    dag_id='retail_production_pipeline',
    default_args=default_args,
    description='Production ETL Pipeline: MySQL OLTP -> PySpark Transform -> Postgres DWH & Mart',
    schedule=None,
    # schedule_interval=None, # Chạy định kỳ mỗi ngày lúc 2h sáng
    start_date=datetime(2026, 1, 1),
    catchup=False,
    template_searchpath=['/opt/airflow/sql'],
    tags=['retail', 'production', 'pyspark', 'postgres'],
) as dag:

    # Task 1: PySpark Ingestion (Bốc dữ liệu incremental từ MySQL sang PostgreSQL Warehouse)
    task_spark_transform = BashOperator(
        task_id='spark_mysql_to_postgres',
        bash_command='python /opt/airflow/src/02_spark_mysql_to_postgres.py',
    )

    # Task 2: Refresh Data Mart (Dùng file DML chạy định kỳ từ thư mục transform)
    task_refresh_data_mart = PostgresOperator(
        task_id='refresh_postgres_data_mart',
        postgres_conn_id='postgres_dwh_default',
        sql=load_sql_file('/opt/airflow/sql/transform/refresh_postgres_mart.sql'),
    )

    # Dependency Flow chuẩn: Rót dữ liệu vào Warehouse trước, sau đó transform sang Mart
    task_spark_transform >> task_refresh_data_mart