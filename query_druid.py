from datetime import datetime, timedelta
import json
import logging

import requests

from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook


DAG_ID = "druid_test_query"


default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
}


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Test Druid SQL query from Airflow",
    start_date=datetime(2026, 8, 18),
    schedule=None,
    catchup=False,
    tags=["druid", "test"],
) as dag:

    @task
    def query_druid():

        # ----------------------------------------------------
        # AIRFLOW CONNECTION
        # ----------------------------------------------------

        conn = BaseHook.get_connection("druid_default")

        druid_url = conn.host.rstrip("/")
        username = conn.login
        password = conn.password

        if not druid_url:
            raise ValueError("Druid URL is not configured")

        if not username or not password:
            raise ValueError(
                "Druid username/password is not configured"
            )

        # ----------------------------------------------------
        # SQL
        # ----------------------------------------------------

        sql = """
            SELECT *
            FROM "TURtest"
            WHERE "day" IN (4)
            LIMIT 10
        """

        payload = {
            "query": sql
        }

        query_url = f"{druid_url}/druid/v2/sql"

        headers = {
            "Content-Type": "application/json"
        }

        logging.info("=" * 60)
        logging.info("DRUID SQL QUERY")
        logging.info("=" * 60)
        logging.info("Datasource: TURtest")
        logging.info('Filter: "day" IN (4)')
        logging.info("Limit: 10")

        # ----------------------------------------------------
        # REQUEST
        # ----------------------------------------------------

        try:

            response = requests.post(
                query_url,
                headers=headers,
                json=payload,
                auth=(username, password),
                timeout=120,
            )

        except requests.Timeout as exc:

            logging.error("Druid request timed out")

            raise exc

        except requests.RequestException as exc:

            logging.error(
                "Druid connection error: %s",
                exc,
            )

            raise exc

        # ----------------------------------------------------
        # HTTP STATUS
        # ----------------------------------------------------

        logging.info(
            "Druid HTTP Status: %s",
            response.status_code,
        )

        if response.status_code != 200:

            logging.error(
                "Druid query failed: %s",
                response.text,
            )

            raise RuntimeError(
                f"Druid query failed "
                f"with HTTP {response.status_code}"
            )

        # ----------------------------------------------------
        # JSON
        # ----------------------------------------------------

        try:

            result = response.json()

        except ValueError as exc:

            logging.error(
                "Druid returned invalid JSON: %s",
                response.text,
            )

            raise exc

        # ----------------------------------------------------
        # RESULT
        # ----------------------------------------------------

        logging.info("Druid query successful")

        logging.info(
            "Returned rows: %d",
            len(result),
        )

        logging.info(
            "Result:\n%s",
            json.dumps(
                result,
                indent=2,
                ensure_ascii=False,
            ),
        )

        return {
            "status": "success",
            "row_count": len(result),
        }


    query_druid()