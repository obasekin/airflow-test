from datetime import datetime, timedelta
import json
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook

from ingestion_druid import run_ingestion


DAG_ID = "druid_ingestion_task"


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Submit Druid ingestion task from Airflow and query result",
    start_date=datetime(2026, 8, 18),
    schedule=None,
    catchup=False,
    tags=["druid", "ingestion", "airflow"],
) as dag:

    @task
    def ingest_druid():
        conn = BaseHook.get_connection("druid_default")

        druid_url = (conn.host or "").rstrip("/")
        username = conn.login
        password = conn.password

        if not druid_url:
            raise ValueError("Druid URL is not configured")

        if not username or not password:
            raise ValueError("Druid username/password is not configured")

        result = run_ingestion(
            datasource_name="druid_airflow_test_data1",
            druid_url=druid_url,
            username=username,
            password=password,
            query_after_ingest=True,
            query_limit=10,
        )

        logging.info("Druid ingestion finished: %s", json.dumps(result, indent=2, ensure_ascii=False))
        return result

    ingest_druid()
