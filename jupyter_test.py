import os
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook


JUPYTER_URL = os.getenv("JUPYTER_URL", "https://server.arcanor.com")
CF_ACCESS_CLIENT_ID = os.getenv("CF_ACCESS_CLIENT_ID")
CF_ACCESS_CLIENT_SECRET = os.getenv("CF_ACCESS_CLIENT_SECRET")

BUCKET_NAME = "arcanor-airflow-logs"
OBJECT_NAME = "airflow-test/gcs-test.txt"


def check_jupyter_api():
    headers = {}
    if CF_ACCESS_CLIENT_ID and CF_ACCESS_CLIENT_SECRET:
        headers = {
            "CF-Access-Client-Id": CF_ACCESS_CLIENT_ID,
            "CF-Access-Client-Secret": CF_ACCESS_CLIENT_SECRET,
        }

    response = requests.get(
        f"{JUPYTER_URL}/hub/api",
        headers=headers,
        timeout=60,
    )

    print("JupyterHub status:", response.status_code)
    print("JupyterHub response:", response.text[:500])
    response.raise_for_status()

    return {
        "status_code": response.status_code,
        "url": f"{JUPYTER_URL}/hub/api",
        "response_preview": response.text[:500],
    }


def write_to_gcs():
    hook = GCSHook(gcp_conn_id="google_cloud_default")

    hook.upload(
        bucket_name=BUCKET_NAME,
        object_name=OBJECT_NAME,
        data="Hello from Airflow GCS test",
        mime_type="text/plain",
    )

    print(f"Successfully uploaded gs://{BUCKET_NAME}/{OBJECT_NAME}")
    return f"gs://{BUCKET_NAME}/{OBJECT_NAME}"


with DAG(
    dag_id="test_jupyter_and_gcs",
    description="Check JupyterHub and upload a file to GCS",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "jupyter", "gcs"],
) as dag:

    start = EmptyOperator(task_id="start")

    check_jupyter = PythonOperator(
        task_id="check_jupyter_api",
        python_callable=check_jupyter_api,
    )

    write_file = PythonOperator(
        task_id="write_to_gcs",
        python_callable=write_to_gcs,
    )

    end = EmptyOperator(task_id="end")

    start >> check_jupyter >> write_file >> end