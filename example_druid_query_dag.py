"""
Example DAG: Druid SQL Query Executor
=====================================
Bu DAG, `citadel.druid.query_executor` modülü aracılığıyla Apache Druid üzerinde
doğrudan SQL sorgusu çalıştırma örneğini gösterir.

Ne zaman kullanılır?
--------------------
- JupyterHub yerine doğrudan Airflow worker üzerinden hafif SQL sorguları çalıştırmak istediğinizde
- Druid tablosundaki kayıt sayısını, max/min tarihleri veya metrikleri hızlıca kontrol etmek için

Kullanılan Servis:
------------------
- `citadel.druid.query_executor.execute_query`
- `citadel.config_loader.config`
"""

from datetime import timedelta
import pendulum
from airflow.decorators import dag, task

from citadel.config_loader import config
from citadel.druid.query_executor import execute_query

default_args = {
    "owner": "obasekin",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

@dag(
    dag_id="example_citadel_druid_query",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Istanbul"),
    schedule=None,  # Manuel tetiklemeli örnek DAG
    catchup=False,
    tags=["example", "citadel", "druid", "sql"],
    description="Citadel execute_query kullanarak Druid SQL sorgusu çalıştırma örneği",
)
def example_druid_query_workflow():

    @task(task_id="run_simple_count_query")
    def run_count_query() -> dict:
        """
        Druid SQL endpoint'ine basit bir SELECT sorgusu gönderir.
        """
        # 1. SQL Sorgusu (Örnek tablo: TUR)
        query = """
        SELECT "day", COUNT(*) AS record_count
        FROM "TUR"
        WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
        GROUP BY "day"
        ORDER BY "day" DESC
        LIMIT 5
        """

        # 2. execute_query çağrısı
        # conn_id: Airflow UI'da tanımlı Druid connection ID (varsayılan: citadel_config.yaml içindeki druid.conn_id)
        # timeout: HTTP timeout saniyesi
        print(f"Executing Druid query:\n{query}")
        results = execute_query(
            query=query,
            conn_id=config.druid.get("conn_id", "druid_default"),
            timeout=config.druid.get("status_timeout", 60),
        )

        print(f"Received {len(results)} rows from Druid:")
        for row in results:
            print(f"  {row}")

        return {"row_count": len(results), "sample": results[:3] if results else []}

    @task(task_id="process_query_results")
    def process_results(query_data: dict):
        """
        Önceki task'ın sonucunu işler / loglar.
        """
        print(f"Query returned {query_data['row_count']} records.")
        for item in query_data.get("sample", []):
            print(f"Sample row: {item}")

    # Task akışı
    results = run_count_query()
    process_results(results)


example_druid_query_workflow()
