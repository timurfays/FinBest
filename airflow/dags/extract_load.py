from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import os

default_args = {"owner":"finbest","retries":1,"retry_delay":timedelta(minutes=2)}
dag = DAG("extract_load",
          start_date=datetime(2024,1,1),
          schedule_interval=None,  # запускаем из master
          catchup=False,
          default_args=default_args,
          tags=["finbest"])

# Пути к файлам данных
TRANSACTIONS_CSV = "/data/bank_transactions.csv"
CLIENTS_CSV = "/data/clients.csv"

def update_transactions_schema(**_):
    """Обновляет схему таблицы staging.transactions, добавляя недостающие столбцы"""
    hook = PostgresHook(postgres_conn_id="postgres_default")
    
    # Получаем список столбцов из таблицы staging.transactions
    columns_query = """
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_schema = 'staging' AND table_name = 'transactions'
    """
    db_columns = [row[0] for row in hook.get_records(columns_query)]
    print(f"Текущие столбцы в БД: {db_columns}")
    
    # Читаем заголовки CSV-файла для определения нужных столбцов
    df = pd.read_csv(TRANSACTIONS_CSV, nrows=0)
    csv_columns = df.columns.tolist()
    print(f"Столбцы в CSV: {csv_columns}")
    
    # Определяем, какие столбцы нужно добавить
    # Исключаем 'id', так как он не нужен в таблице
    missing_columns = [col for col in csv_columns if col.lower() not in [c.lower() for c in db_columns] and col.lower() != 'id']
    print(f"Недостающие столбцы: {missing_columns}")
    
    if missing_columns:
        # Определяем типы данных для новых столбцов
        column_types = {
            'category': 'VARCHAR',
            'country_code': 'VARCHAR(2)',
            'region': 'VARCHAR',
            'device_type': 'VARCHAR',
            'session_id': 'VARCHAR',
            'channel': 'VARCHAR',
            'recipient_id': 'VARCHAR',
            'transaction_purpose': 'VARCHAR'
        }
        
        # Создаем SQL-запрос для добавления столбцов
        alter_table_sql = "ALTER TABLE staging.transactions "
        
        for col in missing_columns:
            # Используем тип VARCHAR по умолчанию, если тип не определен
            col_type = column_types.get(col, 'VARCHAR')
            alter_table_sql += f'ADD COLUMN IF NOT EXISTS {col} {col_type}, '
        
        # Удаляем последнюю запятую и пробел
        alter_table_sql = alter_table_sql.rstrip(', ')
        
        print(f"Выполняем SQL: {alter_table_sql}")
        hook.run(alter_table_sql)
        print("Схема таблицы обновлена")

def load_transactions(path, **_):
    """Загружает данные о транзакциях"""
    df = pd.read_csv(path)
    print(f"Загружено {len(df)} строк из {path}")
    
    # Проверка на дубликаты transaction_id
    duplicates = df[df['transaction_id'].duplicated(keep=False)]
    if not duplicates.empty:
        print(f"Найдено {len(duplicates)} дубликатов по transaction_id: {duplicates['transaction_id'].tolist()}")
        raise ValueError("Обнаружены дубликаты в transaction_id в файле CSV")
    
    # Удаляем столбец 'id', если он есть
    if 'id' in df.columns:
        df = df.drop('id', axis=1)
        print("Удален столбец 'id'")
    
    # Загружаем данные
    hook = PostgresHook(postgres_conn_id="postgres_default")
    
    # Удаляем только записи с transaction_id, которые есть в CSV
    transaction_ids = df['transaction_id'].tolist()
    if transaction_ids:  # Проверяем, что список не пустой
        hook.run(
            "DELETE FROM staging.transactions WHERE transaction_id = ANY(%s)",
            parameters=[transaction_ids]
        )
    else:
        print("Список transaction_ids пуст, пропускаем DELETE")
    
    df.to_sql("transactions", hook.get_sqlalchemy_engine(), schema="staging",
              if_exists="append", index=False, method="multi")
    print(f"Данные загружены в staging.transactions")

def load_clients(path, **_):
    """Загружает данные о клиентах"""
    df = pd.read_csv(path)
    print(f"Загружено {len(df)} строк из {path}")
    
    # Получаем структуру таблицы в базе данных
    hook = PostgresHook(postgres_conn_id="postgres_default")
    
    # Получаем список столбцов из таблицы staging.clients
    columns_query = """
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_schema = 'staging' AND table_name = 'clients'
    """
    db_columns = [row[0] for row in hook.get_records(columns_query)]
    print(f"Столбцы в БД для clients: {db_columns}")
    
    # Выбираем только те столбцы из CSV, которые есть в таблице
    valid_columns = [col for col in df.columns if col.lower() in [c.lower() for c in db_columns]]
    print(f"Используем столбцы для clients: {valid_columns}")
    
    # Если есть лишние столбцы, удаляем их
    df = df[valid_columns]
    
    # Очищаем таблицу
    hook.run("TRUNCATE TABLE staging.clients")
    
    # Загружаем данные
    df.to_sql("clients", hook.get_sqlalchemy_engine(), schema="staging",
              if_exists="append", index=False, method="multi")
    print(f"Данные загружены в staging.clients")

def load_rates(**context):
    """Загружает курсы валют"""
    ds = context["ds"]
    df = pd.DataFrame([{"date":ds,"currency":c,"rate":r}
        for c,r in {"USD":90,"EUR":98,"GBP":110,"RUB":1}.items()])
    hook = PostgresHook(postgres_conn_id="postgres_default")
    hook.run(f"DELETE FROM staging.currency_rates WHERE date='{ds}'")
    df.to_sql("currency_rates", hook.get_sqlalchemy_engine(), schema="staging",
              if_exists="append", index=False)
    print(f"Курсы валют загружены на дату {ds}")

# Проверяем наличие файла с транзакциями
tx_sensor = FileSensor(
    task_id="wait_transactions_csv",
    filepath=TRANSACTIONS_CSV,
    fs_conn_id="fs_default",
    poke_interval=5,
    timeout=300, 
    mode="reschedule", 
    dag=dag
)

# Проверяем наличие файла с клиентами
clients_sensor = FileSensor(
    task_id="wait_clients_csv",
    filepath=CLIENTS_CSV,
    fs_conn_id="fs_default",
    poke_interval=5,
    timeout=300, 
    mode="reschedule", 
    dag=dag
)

# Обновляем схему таблицы transactions
update_schema = PythonOperator(
    task_id="update_transactions_schema",
    python_callable=update_transactions_schema,
    dag=dag
)

# Загружаем транзакции
load_transactions_task = PythonOperator(
    task_id="load_transactions",
    python_callable=load_transactions, 
    op_kwargs={"path":TRANSACTIONS_CSV}, 
    dag=dag
)

# Загружаем клиентов
load_clients_task = PythonOperator(
    task_id="load_clients",
    python_callable=load_clients, 
    op_kwargs={"path":CLIENTS_CSV}, 
    dag=dag
)

# Загружаем курсы валют
load_currency_rates = PythonOperator(
    task_id="load_rates",
    python_callable=load_rates,
    provide_context=True,
    dag=dag
)

# Определяем порядок выполнения задач
tx_sensor >> update_schema >> load_transactions_task
clients_sensor >> load_clients_task
[load_transactions_task, load_clients_task, load_currency_rates]
