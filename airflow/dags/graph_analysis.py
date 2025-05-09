"""
DAG для построения и анализа транзакционного графа клиентов
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os

default_args = {
    "owner": "finbest",
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
    "start_date": days_ago(1),
}

dag = DAG(
    "graph_analysis",
    default_args=default_args,
    description="Построение и анализ транзакционного графа",
    schedule_interval=None,
    catchup=False,
    tags=["finbest", "graph"],
)

# Создаем схему и обновляем таблицы для графовых данных
create_schema = PostgresOperator(
    task_id="create_schema",
    postgres_conn_id="postgres_default",
    sql="""
    CREATE SCHEMA IF NOT EXISTS graph;
    
    CREATE TABLE IF NOT EXISTS graph.client_communities (
        client_id VARCHAR PRIMARY KEY,
        community_id BIGINT,
        influence_score FLOAT,
        community_size BIGINT,
        w_degree FLOAT,
        created_at TIMESTAMP DEFAULT NOW()
    );
    
    CREATE TABLE IF NOT EXISTS mart.fact_graph_analytics (
        transaction_id VARCHAR PRIMARY KEY,
        client_id VARCHAR,
        datetime TIMESTAMP,
        amount NUMERIC,
        currency VARCHAR(3),
        merchant VARCHAR,
        transaction_type VARCHAR,
        ip_network VARCHAR,
        category VARCHAR,
        community_id BIGINT,
        influence_score FLOAT,
        community_size BIGINT,
        w_degree FLOAT,
        created_at TIMESTAMP DEFAULT NOW()
    );
    
    -- Миграция: добавляем столбец w_degree, если он отсутствует
    ALTER TABLE mart.fact_graph_analytics
    ADD COLUMN IF NOT EXISTS w_degree FLOAT;
    """,
    dag=dag,
)

# Запускаем Spark-задачу для построения графа
build_graph = SparkSubmitOperator(
    task_id="build_graph",
    conn_id="spark_default",
    application="/opt/airflow/spark/build_graph.py",
    packages="org.postgresql:postgresql:42.6.0,graphframes:graphframes:0.8.2-spark3.2-s_2.12",
    application_args=[
        "--jdbc", "jdbc:postgresql://postgres:5432/finbest",
        "--user", "finbest",
        "--password", "finbest_password"
    ],
    driver_memory="1g",
    executor_memory="1g",
    executor_cores=1,
    conf={
        "spark.driver.maxResultSize": "512m",
        "spark.sql.shuffle.partitions": "10"
    },
    verbose=True,
    dag=dag,
)

# Определяем категории для транзакций
define_categories = PostgresOperator(
    task_id="define_categories",
    postgres_conn_id="postgres_default",
    sql="""
    ALTER TABLE raw.masked_transactions ADD COLUMN IF NOT EXISTS category VARCHAR;
    
    UPDATE raw.masked_transactions
    SET category = 
        CASE 
            WHEN transaction_type = 'p2p' THEN 'p2p'
            WHEN merchant IN ('Amazon', 'Ozon') THEN 'retail'
            WHEN merchant = 'Uber' THEN 'travel'
            ELSE 'other'
        END
    WHERE category IS NULL;
    """,
    dag=dag,
)

# Создаем витрину данных
create_mart = PostgresOperator(
    task_id="create_mart",
    postgres_conn_id="postgres_default",
    sql="""
    INSERT INTO mart.fact_graph_analytics (
        transaction_id, client_id, datetime, amount, currency, 
        merchant, transaction_type, ip_network, category,
        community_id, influence_score, community_size, w_degree
    )
    SELECT 
        t.transaction_id, 
        t.client_id, 
        t.datetime, 
        t.amount, 
        t.currency, 
        t.merchant, 
        t.transaction_type, 
        t.ip_network, 
        t.category,
        c.community_id, 
        c.influence_score, 
        c.community_size,
        c.w_degree
    FROM 
        raw.masked_transactions t
    LEFT JOIN 
        graph.client_communities c ON t.client_id = c.client_id
    ON CONFLICT (transaction_id) 
    DO UPDATE SET
        community_id = EXCLUDED.community_id,
        influence_score = EXCLUDED.influence_score,
        community_size = EXCLUDED.community_size,
        w_degree = EXCLUDED.w_degree,
        created_at = NOW();
    """,
    dag=dag,
)

# Запускаем Jupyter-ноутбук для визуализации
run_jupyter = PythonOperator(
    task_id="run_jupyter",
    python_callable=lambda: os.system(
        "jupyter nbconvert --execute --to html "
        "/notebooks/graph_visualization.ipynb "
        "--output /reports/graph_visualization_report.html"
    ),
    dag=dag,
)

# Порядок выполнения задач
create_schema >> define_categories >> build_graph >> create_mart >> run_jupyter