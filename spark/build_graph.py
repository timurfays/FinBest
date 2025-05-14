#!/usr/bin/env python3
"""
Скрипт для построения и анализа транзакционного графа клиентов с использованием Spark и GraphFrames.
Строит граф на основе p2p-транзакций и общих атрибутов клиентов.
"""

import argparse
import logging
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, FloatType, LongType, ArrayType
from graphframes import GraphFrame
from graphframes.lib import AggregateMessages as AM  # Добавлен пропущенный импорт
import networkx as nx
import os
import json
from pyspark.sql.window import Window

# Настраиваем логгер
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def main():
    # Парсим аргументы командной строки
    parser = argparse.ArgumentParser(description="Построение транзакционного графа")
    parser.add_argument("--jdbc", required=True, help="JDBC URL для PostgreSQL")
    parser.add_argument("--user", required=True, help="Имя пользователя БД")
    parser.add_argument("--password", required=True, help="Пароль БД")
    parser.add_argument("--top_nodes", type=int, default=100, help="Количество топовых узлов для анализа")
    args = parser.parse_args()

    # Создаем SparkSession
    spark = SparkSession.builder \
        .appName("FinBestGraphAnalysis") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0,graphframes:graphframes:0.8.2-spark3.2-s_2.12") \
        .getOrCreate()
    
    logger.info("SparkSession создана")
    
    # Подключение к PostgreSQL
    jdbc_url = args.jdbc
    properties = {
        "user": args.user,
        "password": args.password,
        "driver": "org.postgresql.Driver"
    }
    
    try:
        # Загружаем данные
        logger.info("Загружаем клиентов и транзакции")
        clients = spark.read.jdbc(url=jdbc_url, table="staging.clients", properties=properties)
        transactions = spark.read.jdbc(url=jdbc_url, table="raw.masked_transactions", properties=properties)
        
        tx_count = transactions.count()
        client_count = clients.count()
        logger.info(f"Загружено {tx_count} транзакций и {client_count} клиентов")
        
        if tx_count == 0 or client_count == 0:
            raise Exception("Нет данных в таблицах raw.masked_transactions или staging.clients")
        
        # Подготовка данных
        transactions = transactions.withColumn("dt_30", 
            F.date_trunc("minute", F.col("datetime")).cast("timestamp"))
        transactions = transactions.fillna({"device_type": "unknown", "merchant": "no_merchant"})

        # 1. Создаем ребра P2P транзакций
        logger.info("Создаем P2P ребра")
        p2p_edges = transactions.filter(F.col("transaction_type") == "p2p") \
            .filter(F.col("recipient_id_hash").isNotNull()) \
            .select(
                F.col("client_id").alias("src"),
                F.col("recipient_id_hash").alias("dst"),
                F.lit("p2p").alias("relationship"),
                F.log1p(F.col("amount")).alias("weight")
            )
        
        # 2. Создаем ребра по общим атрибутам
        logger.info("Создаем ребра по общим атрибутам")
        
        # Общие мерчанты
        merchant_edges = transactions.filter(F.col("merchant").isNotNull()) \
            .groupBy("merchant") \
            .agg(F.collect_set("client_id").alias("clients")) \
            .filter(F.size("clients") <= 100) \
            .filter(F.size("clients") >= 2) \
            .withColumn("client_pairs", F.explode(
                F.expr("flatten(array_distinct(transform(sequence(0, size(clients)-2), " +
                      "i -> transform(sequence(i+1, size(clients)-1), j -> array(clients[i], clients[j])))))"))
            ) \
            .select(
                F.col("client_pairs")[0].alias("src"),
                F.col("client_pairs")[1].alias("dst"),
                F.lit("merchant").alias("relationship"),
                F.lit(0.5).alias("weight")
            )
            
        # Общие сессии
        session_edges = transactions.filter(F.col("session_id").isNotNull()) \
            .groupBy("session_id") \
            .agg(F.collect_set("client_id").alias("clients")) \
            .filter(F.size("clients") <= 50) \
            .filter(F.size("clients") >= 2) \
            .withColumn("client_pairs", F.explode(
                F.expr("flatten(array_distinct(transform(sequence(0, size(clients)-2), " +
                      "i -> transform(sequence(i+1, size(clients)-1), j -> array(clients[i], clients[j])))))"))
            ) \
            .select(
                F.col("client_pairs")[0].alias("src"),
                F.col("client_pairs")[1].alias("dst"),
                F.lit("session").alias("relationship"),
                F.lit(0.7).alias("weight")
            )
        
        # Общие IP-сети
        ip_edges = transactions.filter(F.col("ip_network").isNotNull()) \
            .groupBy("ip_network") \
            .agg(F.collect_set("client_id").alias("clients")) \
            .filter(F.size("clients") <= 50) \
            .filter(F.size("clients") >= 2) \
            .withColumn("client_pairs", F.explode(
                F.expr("flatten(array_distinct(transform(sequence(0, size(clients)-2), " +
                      "i -> transform(sequence(i+1, size(clients)-1), j -> array(clients[i], clients[j])))))"))
            ) \
            .select(
                F.col("client_pairs")[0].alias("src"),
                F.col("client_pairs")[1].alias("dst"),
                F.lit("ip").alias("relationship"),
                F.lit(0.6).alias("weight")
            )
        
        # Общие снятия в одном банкомате (регион + время)
        atm_edges = transactions.filter(F.col("transaction_type").isin("withdrawal")) \
            .withColumn("atm_group", 
                        F.concat(F.col("region"), F.lit("_"), F.date_format(F.col("dt_30"), "yyyy-MM-dd-HH-mm"))) \
            .filter(F.col("atm_group").isNotNull()) \
            .groupBy("atm_group") \
            .agg(F.collect_set("client_id").alias("clients")) \
            .filter(F.size("clients") <= 20) \
            .filter(F.size("clients") >= 2) \
            .withColumn("client_pairs", F.explode(
                F.expr("flatten(array_distinct(transform(sequence(0, size(clients)-2), " +
                      "i -> transform(sequence(i+1, size(clients)-1), j -> array(clients[i], clients[j])))))"))
            ) \
            .select(
                F.col("client_pairs")[0].alias("src"),
                F.col("client_pairs")[1].alias("dst"),
                F.lit("atm").alias("relationship"),
                F.lit(0.8).alias("weight")
            )
        
        # 3. Объединяем все ребра
        logger.info("Объединяем все ребра")
        all_edges = p2p_edges.union(merchant_edges).union(session_edges).union(ip_edges).union(atm_edges)
        
        # Группируем ребра между одними и теми же узлами и суммируем веса
        final_edges = all_edges.groupBy("src", "dst") \
            .agg(
                F.sum("weight").alias("weight"),
                F.collect_set("relationship").alias("relationships")
            ) \
            .withColumn("relationships_str", 
                       F.array_join(F.col("relationships"), ","))
        
        # Фильтруем слабые ребра
        final_edges = final_edges.filter(F.col("weight") >= 0.5)
        
        # 4. Создаем вершины
        logger.info("Создаем вершины графа")
        # Получаем уникальные ID клиентов из ребер
        src_nodes = final_edges.select("src").distinct()
        dst_nodes = final_edges.select("dst").distinct()
        all_nodes = src_nodes.union(dst_nodes).distinct()
        
        vertices = all_nodes.withColumnRenamed("src", "id")
        
        # 5. Создаем финальный граф
        logger.info("Создаем финальный граф")
        graph = GraphFrame(vertices, final_edges)
        
        # 6. Анализ графа
        logger.info("Вычисляем метрики графа")
        
        # Степени вершин
        degrees = graph.degrees
        
        # Взвешенные степени с использованием AggregateMessages
        weighted_degrees = graph.aggregateMessages(
            F.sum(AM.msg).alias("w_degree"),  # Используем AM.msg вместо AM.edge["weight"]
            sendToSrc=AM.edge["weight"],
            sendToDst=AM.edge["weight"]
        )

        
        # Label Propagation для сообществ
        communities = graph.labelPropagation(maxIter=10)
        
        # Размер сообществ
        community_sizes = communities.groupBy("label") \
            .count() \
            .withColumnRenamed("label", "community_id") \
            .withColumnRenamed("count", "community_size")
        
        # PageRank для влиятельности
        pagerank = graph.pageRank(resetProbability=0.15, maxIter=20)
        
        # 7. Объединяем результаты
        logger.info("Объединяем результаты")
        result = communities.withColumnRenamed("label", "community_id") \
            .join(pagerank.vertices.select("id", F.col("pagerank").alias("influence_score")), "id") \
            .join(weighted_degrees, "id") \
            .join(degrees, "id")  # Убираем переименование, оставляем оригинальное имя "degree"

        # Добавляем размеры сообществ
        result = result.join(
            community_sizes,
            result.community_id == community_sizes.community_id,
            "left"
        ).drop(community_sizes.community_id)

        
        # 8. Выбираем топ-N крупнейших сообществ и их узлы
        logger.info(f"Выбираем топ-{args.top_nodes} крупнейших сообществ")
        # Сначала получаем топ-N сообществ по размеру
        top_communities = community_sizes \
            .orderBy(F.desc("community_size")) \
            .limit(args.top_nodes) \
            .select("community_id")
        
        # Затем получаем все узлы из этих сообществ
        top_nodes = result.join(
            top_communities,
            result.community_id == top_communities.community_id,
            "inner"
        )
        
        logger.info(f"Выбрано {top_nodes.count()} узлов из топ-{args.top_nodes} сообществ")
        
        # 9. Получаем все ребра между узлами выбранных сообществ
        logger.info("Фильтруем ребра для выбранных сообществ")
        node_ids = top_nodes.select("id").collect()
        node_set = {row["id"] for row in node_ids}
        
        # Фильтруем ребра, где оба конца принадлежат выбранным сообществам
        filtered_edges_rdd = final_edges.rdd.filter(
            lambda row: row["src"] in node_set and row["dst"] in node_set
        )
        filtered_edges = spark.createDataFrame(filtered_edges_rdd, final_edges.schema)
        
        logger.info(f"Отфильтровано {filtered_edges.count()} рёбер между узлами выбранных сообществ")
        
        # 10. Экспорт графа в GraphML
        logger.info("Экспортируем граф в GraphML")
        G = nx.Graph()
        
        # Добавляем узлы с атрибутами
        vertices_df = top_nodes.toPandas()
        
        # Выводим структуру данных для проверки
        logger.info(f"Структура DataFrame: {vertices_df.dtypes}")
        
        # Удаляем дублирующийся столбец community_id
        vertices_df = vertices_df.loc[:,~vertices_df.columns.duplicated()]
        
        # Теперь можем посчитать распределение
        community_stats = vertices_df.groupby('community_id').size()
        logger.info("Распределение узлов по экспортируемым сообществам:")
        for comm_id, size in community_stats.items():
            logger.info(f"Сообщество {comm_id}: {size} узлов")
        
        # Добавляем узлы в граф
        for _, row in vertices_df.iterrows():
            G.add_node(
                row['id'],
                community_id=int(row['community_id']),
                influence_score=float(row['influence_score']),
                community_size=int(row['community_size']),
                w_degree=float(row['w_degree']),
                degree=int(row['degree']),
                node_type='client' if str(row['id']).startswith('client_') else 'community'
            )
        
                        
        # Добавляем ребра
        edges_df = filtered_edges.toPandas()
        for _, row in edges_df.iterrows():
            G.add_edge(
                row['src'],
                row['dst'],
                weight=float(row['weight']),
                relationships=row['relationships_str']
            )
        
        # Сохраняем статистику
        stats = {
            "total_nodes": int(result.count()),
            "total_edges": int(final_edges.count()),
            "filtered_nodes": len(G.nodes),
            "filtered_edges": len(G.edges),
            "communities": int(community_sizes.count()),
            "exported_communities": len(community_stats),  # Изменено с community_nodes на community_stats
            "community_distribution": community_stats.to_dict(),  # Изменено с community_nodes на community_stats
            "largest_community_size": int(community_sizes.agg({"community_size": "max"}).collect()[0][0]),
            "smallest_community_size": int(community_sizes.agg({"community_size": "min"}).collect()[0][0]),
            "avg_community_size": float(community_sizes.agg({"community_size": "avg"}).collect()[0][0]),
            "avg_degree": float(degrees.agg({"degree": "avg"}).collect()[0][0]),
            "max_degree": int(degrees.agg({"degree": "max"}).collect()[0][0]),
            "avg_weighted_degree": float(weighted_degrees.agg({"w_degree": "avg"}).collect()[0][0])
        }

        

        
        # Сохраняем в файлы
        os.makedirs("/reports/", exist_ok=True)
        nx.write_graphml(G, "/reports/client_graph.graphml")
        
        with open("/reports/graph_stats.json", "w") as f:
            json.dump(stats, f, indent=2)
        
        logger.info(f"Граф сохранен в /reports/client_graph.graphml")
        logger.info(f"Статистика сохранена в /reports/graph_stats.json")
        
        # 11. Сохранение результатов в БД
        logger.info("Сохраняем результаты в graph.client_communities")
        result.select(
            F.col("id").alias("client_id"),
            F.col("community_id"),
            F.col("influence_score"),
            F.col("community_size"),
            F.col("w_degree"),
            F.col("degree")  # Изменено с connections на degree
        ).write.jdbc(
            url=jdbc_url,
            table="graph.client_communities",
            mode="overwrite",
            properties=properties
        )

        
        logger.info("Анализ графа успешно завершен")
    
    except Exception as e:
        logger.error(f"Ошибка при обработке транзакционного графа: {e}", exc_info=True)
        raise
    finally:
        spark.stop()
        logger.info("SparkSession остановлена")

if __name__ == "__main__":
    main()
