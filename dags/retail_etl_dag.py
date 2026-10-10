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
    schedule_interval='0 2 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    template_searchpath=['/opt/airflow/sql'], # Khai báo thêm searchpath cho Jinja2
    tags=['retail', 'production', 'pyspark', 'postgres'],
) as dag:

    # Task 1: Khởi tạo Schema DWH trên Postgres
    task_init_postgres_dwh = PostgresOperator(
        task_id='init_postgres_dwh_schema',
        postgres_conn_id='postgres_dwh_default',
        sql=load_sql_file('/opt/airflow/sql/01_init_postgres_dwh.sql'),
    )

    # Task 2: PySpark Transform
    task_spark_transform = BashOperator(
        task_id='spark_mysql_to_postgres',
        bash_command='python /opt/airflow/src/02_spark_mysql_to_postgres.py',
    )

    # Task 3: Dựng tầng Data Mart
    task_build_data_mart = PostgresOperator(
        task_id='build_postgres_data_mart',
        postgres_conn_id='postgres_dwh_default',
        sql=load_sql_file('/opt/airflow/sql/02_build_postgres_mart.sql'),
    )

    # Dependency Flow
    task_init_postgres_dwh >> task_spark_transform >> task_build_data_mart