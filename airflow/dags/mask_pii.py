from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from hashlib import sha256
from datetime import datetime
import pandas as pd
import ipaddress

dag = DAG("mask_pii",
          start_date=datetime(2024,1,1),
          schedule_interval=None,
          catchup=False,
          tags=["finbest"])

def mask():
    """
    Маскирует персональные данные:
    - для клиентов: email, passport_id
    - для транзакций: ip_address, recipient_id
    """
    hook = PostgresHook(postgres_conn_id="postgres_default")
    eng = hook.get_sqlalchemy_engine()

    # Маскируем данные клиентов
    clients = pd.read_sql("SELECT * FROM staging.clients", eng)
    print(f"Загружено {len(clients)} клиентов для маскирования")
    
    # Хэшируем email и passport_id
    clients["email_hash"] = clients.email.apply(lambda x: sha256(str(x).encode()).hexdigest() if pd.notna(x) else None)
    clients["passport_hash"] = clients.passport_id.apply(lambda x: sha256(str(x).encode()).hexdigest() if pd.notna(x) else None)
    
    # Удаляем оригинальные PII данные
    clients.drop(["email", "passport_id"], axis=1, inplace=True)
    
    # Сохраняем маскированные данные
    clients.to_sql("masked_clients", eng, schema="raw", if_exists="replace", index=False)
    print(f"Маскированные данные клиентов сохранены в raw.masked_clients")

    # Маскируем данные транзакций
    tx = pd.read_sql("SELECT * FROM staging.transactions", eng)
    print(f"Загружено {len(tx)} транзакций для маскирования")
    
    # Функция для маскирования IP-адреса (сеть /24)
    def mask_ip(ip):
        try:
            if pd.isna(ip):
                return None
            return str(ipaddress.ip_network(f"{ip}/24", strict=False))
        except Exception as e:
            print(f"Ошибка маскирования IP {ip}: {e}")
            return None
    
    # Функция для хэширования recipient_id
    def hash_recipient(recipient_id):
        try:
            if pd.isna(recipient_id):
                return None
            return sha256(str(recipient_id).encode()).hexdigest()
        except Exception as e:
            print(f"Ошибка хэширования recipient_id {recipient_id}: {e}")
            return None
    
    # Маскируем IP-адреса
    tx["ip_network"] = tx.ip_address.apply(mask_ip)
    
    # Проверяем наличие столбца recipient_id и маскируем его, если он есть
    if "recipient_id" in tx.columns:
        tx["recipient_id_hash"] = tx.recipient_id.apply(hash_recipient)
        tx.drop(["ip_address", "recipient_id"], axis=1, inplace=True)
    else:
        print("Столбец recipient_id не найден в данных")
        tx.drop(["ip_address"], axis=1, inplace=True)
    
    # Сохраняем маскированные транзакции
    tx.to_sql("masked_transactions", eng, schema="raw", if_exists="replace", index=False)
    print(f"Маскированные данные транзакций сохранены в raw.masked_transactions")

# Задача маскирования PII
mask_task = PythonOperator(
    task_id="mask",
    python_callable=mask,
    dag=dag
)
