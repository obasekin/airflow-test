from datetime import datetime, timedelta
import json
import logging

from airflow import DAG
from airflow.decorators import task

from jupyterhub_credentials import check_jupyterhub_connection, get_jupyterhub_config


DAG_ID = "jupyterhub_credentials_check"


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Validate JupyterHub connection settings from Airflow UI connection metadata",
    start_date=datetime(2026, 8, 18),
    schedule=None,
    catchup=False,
    tags=["jupyterhub", "credentials", "airflow"],
) as dag:

    @task
    def verify_jupyterhub_connection():
        conn_id = "jupyterhub_default"
        config = get_jupyterhub_config(conn_id)
        result = check_jupyterhub_connection(conn_id)

        logging.info("JupyterHub base URL: %s", config["base_url"])
        logging.info("JupyterHub username: %s", config["username"])
        logging.info(
            "JupyterHub check result: %s",
            json.dumps(result, indent=2, ensure_ascii=False),
        )

        return result

    verify_jupyterhub_connection()
