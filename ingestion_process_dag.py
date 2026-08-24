import importlib
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.sensors.dag_run import DagRunSensor
from airflow.providers.google.cloud.hooks.gcs import GCSHook


K_SUFFIXES = ("k1", "k2", "k3", "k4")


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
        folders = conf.get("folders")

        if country not in ("BEL", "NLD"):
            raise ValueError("country must be BEL or NLD")
        if not isinstance(folders, dict) or set(folders) != set(K_SUFFIXES):
            raise ValueError("folders must contain k1, k2, k3 and k4")

        return conf

    @task.sensor(
        poke_interval=timedelta(minutes=5),
        timeout=timedelta(hours=6),
        mode="reschedule",
        execution_timeout=timedelta(hours=6),
        retries=3,
        retry_delay=timedelta(minutes=5),
    )
    def check_manifest_ready(
        request: dict,
        k_suffix: str,
    ) -> PokeReturnValue:
        country = request["country"]
        folder_name = request["folders"][k_suffix]
        checker = importlib.import_module(
            f"scripts.{country}.{country}_druid_ingestion.manifest_checker"
        )
        result = checker.find_manifest(
            folder_name=folder_name,
            k_suffix=k_suffix,
        )
        return PokeReturnValue(is_done=result is not None, xcom_value=result)

    @task(execution_timeout=timedelta(minutes=18), retries=2)
    def get_parquet_files(manifest_info: dict) -> list:
        hook = GCSHook(gcp_conn_id="google_cloud_default")
        folder_name = manifest_info["folder_name"]
        file_name = manifest_info["file_name"]
        bucket_name, folder_prefix = folder_name.replace("gs://", "", 1).split("/", 1)
        objects = hook.list(
            bucket_name=bucket_name,
            prefix=folder_prefix.rstrip("/") + "/",
        )
        parquet_files = [
            f"gs://{bucket_name}/{object_name}"
            for object_name in objects
            if object_name.rstrip("/").split("/")[-1].startswith(file_name)
            and object_name.endswith(".parquet")
        ]
        if not parquet_files:
            raise FileNotFoundError(
                f"No parquet files found for prefix '{file_name}' in {folder_name}"
            )
        return parquet_files

    @task(execution_timeout=timedelta(hours=2), retries=3)
    def execute_idempotent_druid_ingestion(
        request: dict,
        manifest_info: dict,
        parquet_files: list,
        k_suffix: str,
    ) -> dict:
        country = request["country"]
        ingestion_module = importlib.import_module(
            f"scripts.{country}.{country}_druid_ingestion.ingestion_druid"
        )
        conn = BaseHook.get_connection("druid_default")
        druid_url = (conn.host or "").rstrip("/")
        if not druid_url or not conn.login or not conn.password:
            raise ValueError("druid_default must contain host, login and password")

        return ingestion_module.run_ingestion(
            parquet_files=parquet_files,
            druid_url=druid_url,
            username=conn.login,
            password=conn.password,
        )

    request = read_request()

    @task_group
    def process_k_group(current_k: str):
        manifest_info = check_manifest_ready.override(
            task_id=f"is_manifest_ready_{current_k}"
        )(request=request, k_suffix=current_k)
        parquet_files = get_parquet_files.override(
            task_id=f"get_parquet_files_{current_k}"
        )(manifest_info=manifest_info)
        ingestion = execute_idempotent_druid_ingestion.override(
            task_id=f"ingestion_process_{current_k}"
        )(
            request=request,
            manifest_info=manifest_info,
            parquet_files=parquet_files,
            k_suffix=current_k,
        )
        manifest_info >> parquet_files >> ingestion

    previous_group = None
    for k_suffix in K_SUFFIXES:
        current_group = process_k_group.override(group_id=f"group_{k_suffix}")(
            current_k=k_suffix
        )
        if previous_group:
            previous_group >> current_group
        previous_group = current_group


ingestion_process_workflow()
