from datetime import datetime, timedelta
import json
import logging

from airflow import DAG
from airflow.decorators import task

from jupyterhub_credentials import (
    check_jupyterhub_connection,
    get_jupyterhub_config,
    run_notebook_workflow,
)


DAG_ID = "jupyterhub_notebook_execution"


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Execute code in JupyterHub notebook and return results via XCom",
    start_date=datetime(2026, 8, 18),
    schedule=None,
    catchup=False,
    tags=["jupyterhub", "notebook", "airflow"],
) as dag:

    @task
    def verify_jupyterhub_connection():
        """Verify JupyterHub connection from Airflow UI metadata."""
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

    @task
    def execute_notebook_calculation():
        """
        Execute a calculation in JupyterHub notebook (2 * 2).
        Returns notebook link and output via XCom.
        """
        conn_id = "jupyterhub_default"

        code = "result = 2 * 2\nprint(f'Calculation: 2 * 2 = {result}')"

        logging.info("Starting notebook workflow...")
        workflow_result = run_notebook_workflow(
            code=code,
            conn_id=conn_id,
        )

        logging.info("=" * 60)
        logging.info("NOTEBOOK EXECUTION RESULT")
        logging.info("=" * 60)
        logging.info(
            "Notebook URL: %s",
            workflow_result.get("notebook_url"),
        )
        logging.info(
            "Execution Status: %s",
            workflow_result.get("execution_status"),
        )
        logging.info(
            "Outputs:\n%s",
            json.dumps(
                workflow_result.get("outputs", []),
                indent=2,
                ensure_ascii=False,
            ),
        )
        logging.info("=" * 60)

        return workflow_result

    @task
    def process_notebook_result(result: dict):
        """
        Process notebook result from XCom.
        Log notebook link and extracted calculation output.
        """
        logging.info("=" * 60)
        logging.info("NOTEBOOK RESULT SUMMARY")
        logging.info("=" * 60)
        logging.info("Notebook Name: %s", result.get("notebook_name"))
        logging.info("Notebook URL: %s", result.get("notebook_url"))
        logging.info("Kernel ID: %s", result.get("kernel_id"))
        logging.info("Execution Status: %s", result.get("execution_status"))

        logging.info("\nCalculation Output:")
        for output in result.get("outputs", []):
            if output.get("type") == "stream":
                logging.info("  %s", output.get("text").strip())
            elif output.get("type") == "execute_result":
                data = output.get("data", {})
                if "text/plain" in data:
                    logging.info("  Result: %s", data["text/plain"])
            elif output.get("type") == "error":
                logging.error(
                    "  Error: %s",
                    output.get("evalue"),
                )

        logging.info("=" * 60)

        return {
            "notebook_url": result.get("notebook_url"),
            "status": result.get("execution_status"),
        }

    # DAG workflow
    check_conn = verify_jupyterhub_connection()
    exec_result = execute_notebook_calculation()
    process_result = process_notebook_result(exec_result)

    check_conn >> exec_result >> process_result
