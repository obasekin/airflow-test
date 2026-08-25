import importlib
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook


@dag(
    dag_id="ingestion_process_dag",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 18, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    render_template_as_native_obj=True,
    tags=["druid", "ingestion", "child-dag"],
)
def ingestion_process_workflow():

    @task
    def read_request(**kwargs) -> dict:
        conf = kwargs["dag_run"].conf or {}
        country = conf.get("country")
        files = conf.get("files")

        if country not in ("BEL", "NLD", "TURv2"):
            raise ValueError("country must be BEL, NLD or TURv2")
        if not isinstance(files, list) or not files:
            raise ValueError("files must contain at least one parquet URI")

        return conf

    @task(execution_timeout=timedelta(hours=2), retries=3)
    def execute_idempotent_druid_ingestion(request: dict) -> dict:
        country = request["country"]
        ingestion_module = importlib.import_module(
            f"scripts.{country}.{country}_druid_ingestion.ingestion_druid"
        )
        conn = BaseHook.get_connection("druid_default")
        druid_url = (conn.host or "").rstrip("/")
        if not druid_url or not conn.login or not conn.password:
            raise ValueError("druid_default must contain host, login and password")

        return ingestion_module.run_ingestion(
            parquet_files=request["files"],
            druid_url=druid_url,
            username=conn.login,
            password=conn.password,
        )

    request = read_request()
    ingestion = execute_idempotent_druid_ingestion(request=request)
    request >> ingestion


ingestion_process_workflow()
