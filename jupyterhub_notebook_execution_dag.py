import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

from citadel.jupyter.jupyter_executor import (
    check_jupyterhub_connection,
    execute_notebook_code,
    get_jupyterhub_config,
)


DAG_ID = "jupyterhub_notebook_execution"
CONN_ID = "jupyterhub_default"

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Execute code in JupyterHub and return notebook results via XCom",
    start_date=datetime(2026, 8, 18),
    schedule=None,
    catchup=False,
    tags=["jupyterhub", "notebook", "airflow"],
) as dag:

    @task
    def verify_jupyterhub_connection() -> dict:
        config = get_jupyterhub_config(CONN_ID)
        result = check_jupyterhub_connection(CONN_ID)
        logging.info("JupyterHub base URL: %s", config["base_url"])
        logging.info("JupyterHub username: %s", config["username"])
        logging.info("JupyterHub check result: %s", json.dumps(result, indent=2))
        return result

    @task
    def execute_notebook_calculation() -> dict:
        code = "result = 2 * 2\nprint(f'Calculation: 2 * 2 = {result}')"
        result = execute_notebook_code(code=code, conn_id=CONN_ID)
        logging.info("Notebook URL: %s", result.get("notebook_url"))
        logging.info(
            "Notebook outputs:\n%s",
            json.dumps(result.get("outputs", []), indent=2),
        )
        return result

    @task
    def process_notebook_result(result: dict) -> dict:
        logging.info("Execution status: %s", result.get("execution_status"))
        for output in result.get("outputs", []):
            if output.get("type") == "stream":
                logging.info("Notebook output: %s", output.get("text", "").strip())
            elif output.get("type") == "error":
                logging.error("Notebook error: %s", output.get("evalue"))
        return {
            "notebook_url": result.get("notebook_url"),
            "status": result.get("execution_status"),
        }

    check_conn = verify_jupyterhub_connection()
    exec_result = execute_notebook_calculation()
    process_result = process_notebook_result(exec_result)
    check_conn >> exec_result >> process_result
