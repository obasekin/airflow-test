from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook


BUCKET_NAME = "arcanor-airflow-logs"
OBJECT_NAME = "airflow-test/gcs-test.txt"


def write_to_gcs():
    hook = GCSHook(gcp_conn_id="google_cloud_default")

    hook.upload(
        bucket_name=BUCKET_NAME,
        object_name=OBJECT_NAME,
        data="Hello from Airflow GCS test",
        mime_type="text/plain",
    )

    print(f"Successfully uploaded gs://{BUCKET_NAME}/{OBJECT_NAME}")


with DAG(
    dag_id="test_gcs_write",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "gcs"],
) as dag:

    write_file = PythonOperator(
        task_id="write_to_gcs",
        python_callable=write_to_gcs,
    )