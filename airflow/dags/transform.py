from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import os
import subprocess

default_args = {"owner": "finbest", "retries": 1, "retry_delay": timedelta(minutes=1)}
dag = DAG(
    "transform",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["finbest"],
)

def run_dbt(**kwargs):
    """Запуск dbt с подробным выводом"""
    # Проверяем наличие исполняемого файла dbt
    dbt_path = "/home/airflow/.local/bin/dbt"
    print(f"Проверка существования dbt: {os.path.exists(dbt_path)}")
    # Логируем содержимое каталога проекта
    print("Содержимое каталога /dbt_project:")
    for root, dirs, files in os.walk('/dbt_project'):
        print(f"Директория: {root}")
        print(f"  Поддиректории: {dirs}")
        print(f"  Файлы: {files}")
    # Запускаем dbt
    result = subprocess.run(
        [
            dbt_path,
            "--debug", "run",
            "--project-dir", "/dbt_project",
            "--profiles-dir", "/dbt_project",
        ],
        capture_output=True,
        text=True,
    )
    print("STDOUT:")
    print(result.stdout)
    print("STDERR:")
    print(result.stderr)
    if result.returncode != 0:
        raise Exception(f"dbt command failed with return code {result.returncode}")
    return result.returncode

# Задача запуска dbt
dbt_run = PythonOperator(
    task_id="dbt_run",
    python_callable=run_dbt,
    dag=dag,
)

# Задача обнаружения подозрительных транзакций через Spark
spark = SparkSubmitOperator(
    task_id="detect_suspicious",
    conn_id="spark_default",
    application="/opt/airflow/spark/detect_suspicious.py",
    packages="org.postgresql:postgresql:42.6.0",
    application_args=[
        "--jdbc", "jdbc:postgresql://postgres:5432/finbest"
    ],
    driver_memory="1g",
    executor_memory="1g",
    executor_cores=1,
    dag=dag,
)

dbt_run >> spark