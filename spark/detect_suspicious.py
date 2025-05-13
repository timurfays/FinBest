#!/usr/bin/env python3
# /opt/airflow/spark/detect_suspicious.py

import argparse
import logging

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.functions import col, lit, current_timestamp, date_format

# Настраиваем логирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Разбор аргументов
parser = argparse.ArgumentParser(description="Обнаружение подозрительных транзакций")
parser.add_argument("--jdbc", required=True,
                    help="jdbc:postgresql://postgres:5432/finbest")
args = parser.parse_args()

# 1. Создаём SparkSession
spark = SparkSession.builder.appName("FinBestDetect").getOrCreate()
logger.info("SparkSession создан")

# 2. Читаем маскированные транзакции из PostgreSQL
df = (
    spark.read
         .format("jdbc")
         .option("url", args.jdbc)
         .option("driver", "org.postgresql.Driver")
         .option("dbtable", "raw.masked_transactions")
         .option("user", "finbest")
         .option("password", "finbest_password")
         .load()
)
logger.info("Таблица raw.masked_transactions загружена, записей: %s", df.count())

# 3. Правило 1: крупные транзакции (amount > 600 000)
large = (
    df.filter(col("amount") > 600000)
      .withColumn("rule_triggered", lit("large_tx"))
      .withColumn("risk_score", lit(70))
)
logger.info("Правило large_tx отработано, записей: %s", large.count())

# 4. Правило 2: более 10 транзакций за последние 24 часа
# конвертируем datetime в секунды
df = df.withColumn("ts", col("datetime").cast("long"))

# окно за последние 24h по «ts»
window_24h = Window.partitionBy("client_id") \
                   .orderBy("ts") \
                   .rangeBetween(-24 * 3600, 0)

freq = (
    df.withColumn("cnt", F.count("transaction_id").over(window_24h))
      .filter(col("cnt") > 10)
      .withColumn("rule_triggered", lit("freq_tx"))
      .withColumn("risk_score", lit(50))
      .drop("cnt", "ts")  # удаляем вспомогательные столбцы
)

logger.info("Правило freq_tx отработано, записей: %s", freq.count())

# 5. Объединяем результаты двух правил
suspicious = large.unionByName(freq)
logger.info("Объединение результатов выполнено, итого записей: %s", suspicious.count())

# 6. Добавляем столбец времени обнаружения
suspicious = suspicious.withColumn("detection_time", current_timestamp())

# 7. Записываем результат в PostgreSQL (mart.suspicious_transactions)
pg_props = {
    "user": "finbest",
    "password": "finbest_password",
    "driver": "org.postgresql.Driver"
}
suspicious.write.jdbc(
    url=args.jdbc,
    table="mart.suspicious_transactions",
    mode="overwrite",
    properties=pg_props
)
logger.info("Запись в mart.suspicious_transactions завершена")

# Завершаем работу Spark
spark.stop()
logger.info("SparkSession остановлен")