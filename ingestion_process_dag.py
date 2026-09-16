from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from citadel.druid.ingestion import run_ingestion
from citadel.notifications.email import EmailNotifier
from config import NOTIFICATION_EMAILS, SMTP_CONN_ID

failure_email = EmailNotifier(
    to_email=NOTIFICATION_EMAILS,
    conn_id=SMTP_CONN_ID,
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

        if country not in ("BEL", "NLD", "TURv2", "TUR"):
            raise ValueError(
                "country must be BEL, NLD, TURv2, TUR or BELtest"
            )

        if not isinstance(files, list) or not files:
            raise ValueError(
                "files must contain at least one parquet URI"
            )

        if not ingestion_spec_path:
            raise ValueError(
                "ingestion_spec_path is required"
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
        from config import GCS_CONN_ID

        return read_blocklist_geohashes(
            country=country,
            date_str=target_date,
            conn_id=GCS_CONN_ID,
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
        )

    request = read_request()
    blocklist = fetch_blocklist_geohashes(request=request)
    ingestion = execute_idempotent_druid_ingestion(
        request=request,
        blocklist_geohashes=blocklist,
    )
    request >> blocklist >> ingestion


ingestion_process_workflow()
