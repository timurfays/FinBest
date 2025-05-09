# -*- coding: utf-8 -*-
"""
DAG superset_sync – создает Postgres-датасет, чарты и дашборд
"""

import json, time, datetime, logging
from typing import Optional
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

SUPERSET = "http://superset:8088"
ADMIN = PASSWORD = "admin"
DB_NAME = "FinBest Postgres"
TABLE = "suspicious_transactions"
SCHEMA = "mart"
RETRIES = 20
DELAY = 3

log = logging.getLogger(__name__)

def superset_session():
    sess = requests.Session()
    for _ in range(RETRIES):
        try:
            r = sess.post(f"{SUPERSET}/api/v1/security/login",
                          json={"username":ADMIN,"password":PASSWORD,
                                "provider":"db","refresh":True},timeout=10)
            r.raise_for_status()
            jwt = r.json()["access_token"]
            sess.get(f"{SUPERSET}/login/",timeout=10)
            csrf = sess.get(f"{SUPERSET}/api/v1/security/csrf_token/",
                            headers={"Authorization":f"Bearer {jwt}"},timeout=10)
            csrf.raise_for_status()
            head = {"Authorization":f"Bearer {jwt}",
                    "X-CSRFToken":csrf.json()["result"],
                    "Content-Type":"application/json"}
            return sess, head
        except Exception as e:
            log.info("Superset еще не готов (%s); ждем…", e)
            time.sleep(DELAY)
    raise RuntimeError("Superset недоступен")

def db_id(sess, head)->int:
    r=sess.get(f"{SUPERSET}/api/v1/database/",
               params={"q":"(filters:!((col:database_name,opr:eq,value:'FinBest Postgres')))"},
               headers=head,timeout=10).json()
    return r["result"][0]["id"]

def dataset_id(sess, head, dbid, table_name)->Optional[int]:
    page=0
    while True:
        r=sess.get(f"{SUPERSET}/api/v1/dataset/",
                   params={"page":page,"page_size":1000},
                   headers=head,timeout=10).json()
        for ds in r["result"]:
            if ds["table_name"]==table_name and ds["schema"]==SCHEMA and ds["database"]["id"]==dbid:
                return ds["id"]
        if (page+1)*1000>=r["count"]:break
        page+=1

def mark_temporal(sess, head, ds_id, col="datetime"):
    sess.put(f"{SUPERSET}/api/v1/dataset/{ds_id}/refresh/", headers=head, timeout=10)
    r = sess.get(f"{SUPERSET}/api/v1/dataset/{ds_id}", headers=head, timeout=10).json()
    for c in r["result"]["columns"]:
        if c["column_name"] == col and not c["is_dttm"]:
            sess.put(
                f"{SUPERSET}/api/v1/dataset/column/{c['id']}",
                headers=head,
                data=json.dumps({"is_dttm": True}),
                timeout=10,
            ).raise_for_status()
            log.info("✔ %s помечен как temporal", col)
            break

def ensure_dataset(sess, head, dbid, table_name) -> int:
    ds = dataset_id(sess, head, dbid, table_name)
    if not ds:
        payload = json.dumps({"database": dbid, "schema": SCHEMA, "table_name": table_name})
        ds = sess.post(f"{SUPERSET}/api/v1/dataset/", headers=head, data=payload, timeout=10).json()["id"]
        log.info("✔ создан датасет id=%s", ds)
    else:
        log.info("Dataset уже есть (id=%s)", ds)
    mark_temporal(sess, head, ds)
    return ds

def t_dataset(**_):
    sess, head = superset_session()
    return ensure_dataset(sess, head, db_id(sess, head), TABLE)

def t_chart(ti, **_):
    sess, head = superset_session()
    ds = ti.xcom_pull(task_ids="create_superset_dataset")
    q = "(filters:!((col:slice_name,opr:eq,value:'TOP-10 Big Spenders')))"
    if sess.get(f"{SUPERSET}/api/v1/chart/", params={"q": q}, headers=head, timeout=10).json()["result"]:
        log.info("Chart уже есть"); return
    params = {
        "query_mode": "aggregate",
        "x_axis": "datetime",
        "granularity_sqla": "datetime",
        "time_grain_sqla": "PT1H",
        "time_range": "No filter",
        "groupby": ["client_id"],
        "metrics": [{
            "expressionType": "SIMPLE",
            "column": {"column_name": "amount"},
            "aggregate": "SUM",
            "label": "sum__amount"
        }],
        "row_limit": 10,
        "series_limit": 10,
        "order_desc": True,
        "order_by_cols": json.dumps([["sum__amount", False]]),
        "y_axis_format": ",d",
    }
    payload = json.dumps({
        "slice_name": "TOP-10 Big Spenders",
        "viz_type": "echarts_timeseries_bar",
        "datasource_id": ds,
        "datasource_type": "table",
        "params": json.dumps(params),
    })
    sess.post(f"{SUPERSET}/api/v1/chart/", headers=head, data=payload, timeout=10).raise_for_status()
    log.info("✔ создан ECharts Bar Chart (X=datetime, серии=client_id)")

def t_dashboard(**_):
    sess, head = superset_session()
    q = "(filters:!((col:dashboard_title,opr:eq,value:'FinBest AML')))"
    if sess.get(f"{SUPERSET}/api/v1/dashboard/", params={"q": q}, headers=head, timeout=10).json()["result"]:
        log.info("Dashboard уже есть"); return
    sess.post(f"{SUPERSET}/api/v1/dashboard/", headers=head, data=json.dumps({"dashboard_title": "FinBest AML"}), timeout=10).raise_for_status()
    log.info("✔ создан Dashboard")

def t_graph_dashboard(ti, **_):
    sess, head = superset_session()
    dbid = db_id(sess, head)
    ds_id = ensure_dataset(sess, head, dbid, "fact_graph_analytics")
    
    charts_to_create = [
        {
            "name": "Распределение клиентов по сообществам",
            "viz_type": "pie",
            "params": {
                "adhoc_filters": [],
                "datasource": f"{ds_id}__table",
                "groupby": ["community_id"],
                "metric": "count",
                "row_limit": 10000,
                "viz_type": "pie",
                "show_legend": True,
                "show_labels": True,
                "label_type": "key",
                "donut": True,
                "color_scheme": "supersetColors"
            }
        },
        {
            "name": "Топ-10 влиятельных клиентов",
            "viz_type": "box_plot",
            "params": {
                "adhoc_filters": [],
                "datasource": f"{ds_id}__table",
                "granularity_sqla": "datetime",
                "groupby": ["client_id"],
                "metrics": [{
                    "aggregate": "SUM",
                    "column": {"column_name": "amount"},
                    "expressionType": "SIMPLE",
                    "label": "Сумма транзакций"
                }],
                "order_desc": True,
                "row_limit": 10,
                "time_grain_sqla": "P1D",
                "viz_type": "echarts_timeseries",
                "x_axis_title": "Дата",
                "y_axis_title": "Сумма",
                "y_axis_format": "SMART_NUMBER"
            }
        }
    ]
    
    # Создаем чарты
    for chart in charts_to_create:
        q = f"(filters:!((col:slice_name,opr:eq,value:'{chart['name']}')))"
        if sess.get(f"{SUPERSET}/api/v1/chart/", params={"q": q}, headers=head, timeout=10).json()["count"] == 0:
            payload = json.dumps({
                "slice_name": chart['name'],
                "viz_type": chart['viz_type'],
                "datasource_id": ds_id,
                "datasource_type": "table",
                "params": json.dumps(chart['params'])
            })
            sess.post(f"{SUPERSET}/api/v1/chart/", headers=head, data=payload, timeout=10).raise_for_status()
            log.info("✔ создан чарт '%s'", chart['name'])
    
    # Создаем дашборд
    dashboard_name = "FinBest Graph Analytics"
    q = f"(filters:!((col:dashboard_title,opr:eq,value:'{dashboard_name}')))"
    if sess.get(f"{SUPERSET}/api/v1/dashboard/", params={"q": q}, headers=head, timeout=10).json()["count"] == 0:
        dashboard_payload = json.dumps({"dashboard_title": dashboard_name, "published": True})
        sess.post(f"{SUPERSET}/api/v1/dashboard/", headers=head, data=dashboard_payload, timeout=10).raise_for_status()
        log.info("✔ создан дашборд '%s'", dashboard_name)


default_args = {"owner": "airflow", "retries": 1, "retry_delay": datetime.timedelta(minutes=2)}
with DAG("superset_sync", start_date=datetime.datetime(2025,4,29),
         schedule_interval=None, catchup=False, default_args=default_args,
         tags=["finbest", "superset"]) as dag:
    create_dataset = PythonOperator(task_id="create_superset_dataset", python_callable=t_dataset)
    create_chart = PythonOperator(task_id="create_superset_chart", python_callable=t_chart)
    create_dashboard = PythonOperator(task_id="create_superset_dashboard", python_callable=t_dashboard)
    create_graph_dashboard = PythonOperator(task_id="create_graph_dashboard", python_callable=t_graph_dashboard)

create_dataset >> create_chart >> create_dashboard >> create_graph_dashboard