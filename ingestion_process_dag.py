"""
Ingestion Process DAG.

This DAG coordinates the Druid ingestion workflow for parquet data files,
fetching blocklist geohashes and executing idempotent Druid ingestion tasks.
"""

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from citadel.config_loader import config
from citadel.druid.ingestion import run_ingestion
from citadel.notifications.email import EmailNotifier

failure_email = EmailNotifier(
    to_email=config.notifications["default_to"],
    conn_id=config.notifications["smtp_conn_id"],
)

default_args = {
    "owner": "obasekin",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": failure_email,
}


@dag(
    dag_id="ingestion_process_dag",
    default_args=default_args,
    schedule=None,
    start_date=pendulum.datetime(
        2026,
        8,
        18,
        tz="UTC",
    ),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    render_template_as_native_obj=True,
    tags=[
        "druid",
        "ingestion",
        "child-dag",
    ],
)
def ingestion_process_workflow():

    @task
    def read_request(**kwargs) -> dict:
        conf = kwargs["dag_run"].conf or {}
        country = conf.get("country")
        files = conf.get("files")
        ingestion_spec_path = conf.get("ingestion_spec_path")

        allowed_countries = config.ingestion.get("allowed_countries", [])
        if country not in allowed_countries:
            raise ValueError(
                f"country '{country}' is not in allowed_countries: {allowed_countries}. "
                f"Add it to citadel_config.yaml -> ingestion -> allowed_countries"
            )

        if not isinstance(files, list) or not files:
            raise ValueError(
                "files must contain at least one parquet URI"
            )

        if not ingestion_spec_path:
            raise ValueError(
                "ingestion_spec_path is required"
            )

        # 4-way consistency check:
        # 1. Flow country
        # 2. Spec directory structure (scripts/<country>/<country>_druid_ingestion/ingestion_spec.json)
        # 3. Data origin (parquet file paths contain /<country>/)
        # 4. Data destination (Druid dataSource in spec equals country)
        import json
        from citadel.druid.spec_loader import IngestionSpecLoader

        with open(ingestion_spec_path, "r", encoding="utf-8") as f:
            spec = json.load(f)

        IngestionSpecLoader.validate_consistency(
            country=country,
            ingestion_spec_path=ingestion_spec_path,
            parquet_files=files,
            spec=spec,
        )

        return conf

    @task(execution_timeout=timedelta(minutes=15), retries=3)
    def fetch_blocklist_geohashes(request: dict) -> list:
        country = request["country"]
        files = request.get("files", [])
        target_date = request.get("target_date")

        if not target_date and files:
            from citadel.druid.ingestion import extract_date_from_parquet_files

            target_date = extract_date_from_parquet_files(files)

        if not target_date:
            return []

        from citadel.utilities.read_csv import read_blocklist_geohashes
 
        return read_blocklist_geohashes(
            country=country,
            date_str=target_date,
            conn_id=config.gcs["conn_id"],
        )

    @task(execution_timeout=timedelta(hours=2), retries=3)
    def execute_idempotent_druid_ingestion(
        request: dict,
        blocklist_geohashes: list,
    ) -> dict:

        return run_ingestion(
            parquet_files=request["files"],
            ingestion_spec_path=request["ingestion_spec_path"],
            blocklist_geohashes=blocklist_geohashes,
            country=request.get("country"),
        )

    request = read_request()
    blocklist = fetch_blocklist_geohashes(request=request)
    ingestion = execute_idempotent_druid_ingestion(
        request=request,
        blocklist_geohashes=blocklist,
    )
    request >> blocklist >> ingestion


ingestion_process_workflow()
