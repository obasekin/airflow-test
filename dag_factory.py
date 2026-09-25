"""
DAG Factory for Country Ingestion Pipelines

This module auto-discovers country configurations from `scripts/*/country_config.yaml`
and generates one DAG per country.

How to add a new country:
1. Create a new directory under `scripts/` (e.g., `scripts/FRA/`).
2. Add a `country_config.yaml` file with the required configuration.
3. The DAG will be automatically generated as `<COUNTRY>_daily_dag`.

Customizing behavior per country:
The `country_config.yaml` controls all parameters like schedule, retries,
owner, and manifest_prefixes.

Feature flags:
- `features.druid_query_before`: If true, executes a notebook Druid query before ingestion.
- `features.druid_query_after`: If true, executes a notebook Druid query after ingestion.
- `features.send_email_report`: If true, sends an email report (requires before/after queries).

Run days & Offsets:
- `run_days` maps a weekday (0=Monday, ..., 6=Sunday) to a list of integer offsets.
- The DAG will branch depending on `logical_date.weekday()`.
- For each mapped offset, a TaskGroup is created for the pipeline.
- If the current weekday is not in `run_days`, the DAG skips execution for that day.
"""

import os
import re
import yaml
import pendulum
import json
from pathlib import Path
from datetime import timedelta

from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.base import PokeReturnValue
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator

from citadel.config_loader import config as citadel_config, get_country_base_path, get_ingestion_spec_path
from citadel.utilities.manifest import find_manifest
from citadel.notifications.email import EmailNotifier, EmailService
from citadel.druid.ingestion import run_ingestion
from citadel.jupyter.jupyter_executor import execute_druid_query_in_notebook

local_tz = pendulum.timezone(citadel_config.general.get("timezone", "Europe/Istanbul"))
FAIL_ON_NOTEBOOK_ERROR = True

def create_country_dag(yaml_config: dict):
    country = yaml_config["country"]
    manifest_prefixes = yaml_config.get("manifest_prefixes", {})
    features = yaml_config.get("features", {})
    run_days = {int(k): v for k, v in yaml_config.get("run_days", {}).items()}
    query_template = yaml_config.get("druid_query", "")
    notebook_code_template = yaml_config.get("notebook_code", "")
    
    dag_id = f"{country}_daily_dag"
    
    gcs_bucket_name = citadel_config.gcs.get("bucket_name", "arcanor-orion")
    gcs_conn_id = citadel_config.gcs.get("conn_id", "google_cloud_default")
    smtp_conn_id = citadel_config.notifications.get("smtp_conn_id", "smtp_default")
    notebook_timeout = citadel_config.jupyter.get("timeout", 1800)
    
    gcs_base_path = get_country_base_path(country)
    ingestion_spec = get_ingestion_spec_path(country)
    
    notification_emails = yaml_config.get("notification_emails", citadel_config.notifications.get("default_to", []))
    failure_email = EmailNotifier(
        to_email=notification_emails,
        conn_id=smtp_conn_id,
    )
    
    default_args = {
        "owner": yaml_config.get("owner", "airflow"),
        "retries": yaml_config.get("retries", 3),
        "retry_delay": timedelta(minutes=yaml_config.get("retry_delay_minutes", 5)),
        "on_failure_callback": failure_email,
    }
    
    start_date_str = yaml_config.get("start_date", "2026-08-18")
    year, month, day = map(int, start_date_str.split("-"))
    
    @dag(
        dag_id=dag_id,
        default_args=default_args,
        start_date=pendulum.datetime(year, month, day, tz=local_tz),
        schedule=yaml_config.get("schedule", "0 8 * * *"),
        catchup=False,
        tags=["druid", "ingestion", "taskgroup", "branching", country],
    )
    def generated_dag():
        
        @task
        def calculate_folder_date(offset_days: int, **kwargs) -> str:
            run_date = kwargs["logical_date"]
            target_date = run_date.subtract(days=offset_days)
            target_date_str = target_date.strftime("%Y-%m-%d")
            
            print("=" * 80)
            print("CALCULATE FOLDER DATE")
            print("=" * 80)
            print(f"Logical date : {run_date}")
            print(f"Offset days  : {offset_days}")
            print(f"Target date  : {target_date_str}")
            print("=" * 80)
            return target_date_str

        @task
        def get_todays_expected_folder(target_date_str: str) -> str:
            hook = GCSHook(gcp_conn_id=gcs_conn_id)
            date_path = target_date_str.replace("-", "/")
            prefix = f"{gcs_base_path}/{date_path}/"
            folder_path = f"gs://{gcs_bucket_name}/{prefix}"
            
            print(f"Checking GCS folder: {folder_path}")
            objects = hook.list(bucket_name=gcs_bucket_name, prefix=prefix)
            if not objects:
                raise FileNotFoundError(f"GCS folder does not exist or is empty: {folder_path}")
                
            print(f"GCS folder exists and contains {len(objects)} object(s).")
            for obj in objects:
                print(f"  - gs://{gcs_bucket_name}/{obj}")
            return folder_path

        @task.sensor(
            poke_interval=60 * 5,
            timeout=16 * 60 * 60,
            mode="reschedule",
            execution_timeout=timedelta(hours=16),
            retries=3,
            retry_delay=timedelta(minutes=5),
        )
        def check_manifest_ready(folder_name: str, k_suffix: str, manifest_prefix: str) -> PokeReturnValue:
            print("=" * 80)
            print("CHECK MANIFEST")
            print("=" * 80)
            print(f"Folder : {folder_name}")
            print(f"K      : {k_suffix}")
            print("=" * 80)
            
            result = find_manifest(
                folder_name=folder_name,
                k_suffix=k_suffix,
                manifest_prefixes={k_suffix: manifest_prefix},
            )
            
            if result is None:
                print(f"Manifest not found for {k_suffix}.")
                print("Sensor will retry in 5 minutes.")
                return PokeReturnValue(is_done=False)
                
            print("Manifest found!")
            print(f"Folder        : {result['folder_name']}")
            print(f"File name     : {result['file_name']}")
            print(f"Manifest path : {result['manifest_path']}")
            print("=" * 80)
            return PokeReturnValue(is_done=True, xcom_value=result)

        @task
        def collect_parquet_files(manifest_infos: list) -> list:
            hook = GCSHook(gcp_conn_id=gcs_conn_id)
            files = []
            for manifest in manifest_infos:
                folder_name = manifest["folder_name"]
                bucket_name, folder_prefix = folder_name.replace("gs://", "", 1).split("/", 1)
                objects = hook.list(
                    bucket_name=bucket_name,
                    prefix=folder_prefix.rstrip("/") + "/",
                )
                files.extend(
                    f"gs://{bucket_name}/{object_name}"
                    for object_name in objects
                    if object_name.rstrip("/").split("/")[-1].startswith(manifest["file_name"])
                    and object_name.endswith(".parquet")
                )
            if not files:
                raise FileNotFoundError("No parquet files found for ingestion request")
            return sorted(set(files))

        @task
        def build_ingestion_request(
            folder_name: str,
            manifest_infos: list,
            parquet_files: list,
            ingestion_spec_path: str,
            target_date: str = "",
            **kwargs,
        ) -> dict:
            return {
                "country": country,
                "source_dag_id": kwargs["dag"].dag_id,
                "files": sorted(set(parquet_files)),
                "ingestion_spec_path": ingestion_spec_path,
                "target_date": target_date,
            }

        def format_logical_date(**kwargs) -> dict:
            logical_date = kwargs["logical_date"]
            start_time = (logical_date - timedelta(days=7)).strftime("%Y-%m-%d 00:00:00")
            end_time = logical_date.strftime("%Y-%m-%d 00:00:00")
            return {"start_time": start_time, "end_time": end_time}

        def execute_notebook_druid_query(**kwargs) -> dict:
            ti = kwargs["ti"]
            current_task_id = kwargs["task"].task_id
            if "execute_notebook_druid_query_before" in current_task_id:
                format_task_id = current_task_id.replace("execute_notebook_druid_query_before", "format_logical_date")
            else:
                format_task_id = current_task_id.replace("execute_notebook_druid_query_after", "format_logical_date")
                
            date_range = ti.xcom_pull(task_ids=format_task_id)
            if not date_range:
                raise ValueError(f"XCom'dan date_range alınamadı! (Aranan task_id: {format_task_id})")
            
            formatted_query = query_template.format(
                start_time=date_range["start_time"],
                end_time=date_range["end_time"],
                country=country,
            )
            result = execute_druid_query_in_notebook(
                code=notebook_code_template.replace("{query}", formatted_query.strip()),
                fail_on_execution_error=FAIL_ON_NOTEBOOK_ERROR,
                timeout=notebook_timeout,
            )
            return result

        def _parse_query_result(query_result: dict, title: str) -> str:
            html = f"<h3>{title}</h3>\n"
            if not query_result:
                html += "<p>No outputs returned from notebook (result is empty).</p>"
                return html
                
            notebook_url = query_result.get("notebook_url", "")
            if notebook_url:
                html += f'<p><b>Notebook:</b> <a href="{notebook_url}">{notebook_url}</a></p>\n'
                
            outputs = query_result.get("outputs", [])
            if not outputs:
                html += "<p>No outputs returned from notebook.</p>"
                return html
                
            for item in outputs:
                if isinstance(item, dict) and item.get("type") == "stream" and "text" in item:
                    text = item["text"]
                    match = re.search(r'Result:\s*\[(.*?)\]', text, re.DOTALL)
                    if match:
                        rows_str = match.group(1)
                        row_matches = re.findall(r'Row\((.*?)\)', rows_str)
                        if row_matches:
                            html += "<table border='1' cellpadding='8' cellspacing='0' style='border-collapse: collapse;'>\n"
                            first_row_items = row_matches[0].split(',')
                            headers = [i.split('=')[0].strip() for i in first_row_items]
                            html += "<tr style='background-color: #f2f2f2;'>" + "".join(f"<th>{h}</th>" for h in headers) + "</tr>\n"
                            
                            for row_str in row_matches:
                                html += "<tr>"
                                items = row_str.split(',')
                                for i in items:
                                    val = i.split('=', 1)[1].strip() if '=' in i else ""
                                    html += f"<td>{val}</td>"
                                html += "</tr>\n"
                                
                            html += "</table>\n<br>\n"
                    
                    time_matches = re.findall(r'Time:\s*(.*)', text)
                    if time_matches:
                        html += "<h4>Execution Time</h4>\n<ul>\n"
                        for tm in time_matches:
                            html += f"<li>{tm}</li>\n"
                        html += "</ul>\n"
            return html

        def send_email_report(**kwargs):
            ti = kwargs["ti"]
            dag_id = kwargs["dag"].dag_id
            logical_date_str = kwargs["logical_date"].strftime("%Y-%m-%d")
            
            task_id_before = kwargs["task"].task_id.replace("send_email_report", "execute_notebook_druid_query_before")
            task_id_after = kwargs["task"].task_id.replace("send_email_report", "execute_notebook_druid_query_after")
            
            result_before = ti.xcom_pull(task_ids=task_id_before)
            result_after = ti.xcom_pull(task_ids=task_id_after)
            
            subject_title = f"{logical_date_str} {dag_id} {country} druid ingestion ended successfully"
            
            html = f"<h2>{subject_title}</h2>\n"
            if result_before:
                html += _parse_query_result(result_before, "Before Ingestion")
                html += "<hr>\n"
            if result_after:
                html += _parse_query_result(result_after, "After Ingestion")

            email_service = EmailService(conn_id=smtp_conn_id)
            email_service.send_email(
                to=notification_emails,
                subject=subject_title,
                cc=yaml_config.get("email_cc"),
                html_content=html,
            )

        @task_group
        def ingestion_pipeline_for_offset(offset_days: int):
            target_date_task = calculate_folder_date(offset_days=offset_days)
            folder_task = get_todays_expected_folder(target_date_str=target_date_task)
            
            manifest_infos = [
                check_manifest_ready.override(
                    task_id=f"is_manifest_ready_{k_suffix}"
                )(
                    folder_name=folder_task,
                    k_suffix=k_suffix,
                    manifest_prefix=prefix,
                )
                for k_suffix, prefix in manifest_prefixes.items()
            ]
            
            parquet_files = collect_parquet_files(manifest_infos=manifest_infos)
            
            request = build_ingestion_request(
                folder_name=folder_task,
                manifest_infos=manifest_infos,
                parquet_files=parquet_files,
                ingestion_spec_path=str(ingestion_spec),
                target_date=target_date_task,
            )
            
            country_seconds_offset = sum(ord(c) for c in country) % 55
            
            trigger_run_id = "{{ dag.dag_id }}__{{ run_id }}__" + f"{request.operator.task_id}"
            triggered_logical_date_expr = (
                "{{ logical_date + macros.timedelta(minutes="
                f"{offset_days}, seconds={country_seconds_offset}"
                ") }}"
            )
            
            trigger = TriggerDagRunOperator(
                task_id="trigger_ingestion_process_dag",
                trigger_dag_id="ingestion_process_dag",
                trigger_run_id=trigger_run_id,
                logical_date=triggered_logical_date_expr,
                conf=request,
                wait_for_completion=False,
                reset_dag_run=False,
                skip_when_already_exists=True,
            )
            
            wait = ExternalTaskSensor(
                task_id="wait_ingestion_process_dag",
                external_dag_id="ingestion_process_dag",
                external_task_id=None,
                execution_date_fn=lambda logical_date, _offset=offset_days, _seconds=country_seconds_offset, **kwargs: logical_date + timedelta(minutes=_offset, seconds=_seconds),
                allowed_states=["success"],
                failed_states=["failed"],
                mode="reschedule",
                poke_interval=60,
                timeout=60 * 60 * 24,
                trigger_rule="none_failed_min_one_success",
            )
            
            last_task = request
            
            if features.get("druid_query_before"):
                format_date_task = PythonOperator(
                    task_id="format_logical_date",
                    python_callable=format_logical_date,
                )
                query_before = PythonOperator(
                    task_id="execute_notebook_druid_query_before",
                    python_callable=execute_notebook_druid_query,
                )
                last_task >> format_date_task >> query_before >> trigger
            else:
                last_task >> trigger
                
            trigger >> wait
            
            wait_downstream = wait
            
            if features.get("druid_query_after"):
                query_after = PythonOperator(
                    task_id="execute_notebook_druid_query_after",
                    python_callable=execute_notebook_druid_query,
                )
                wait_downstream >> query_after
                wait_downstream = query_after
                
            if features.get("send_email_report"):
                send_email_task = PythonOperator(
                    task_id="send_email_report",
                    python_callable=send_email_report,
                    retries=1,
                )
                wait_downstream >> send_email_task

        # Branching Logic
        start_task = EmptyOperator(task_id="start")
        end_task = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")
        skip_task = EmptyOperator(task_id="skip_day")

        # Create TaskGroups for all unique offsets statically
        all_offsets = set()
        for d, offsets in run_days.items():
            all_offsets.update(offsets)
            
        offset_groups = {}
        for offset in all_offsets:
            group = ingestion_pipeline_for_offset.override(group_id=f"process_offset_{offset}")(offset_days=offset)
            offset_groups[offset] = group
            group >> end_task

        @task.branch(task_id="branch_by_day")
        def branch_by_day(**kwargs):
            weekday = kwargs["logical_date"].weekday()
            if weekday in run_days:
                offsets = run_days[weekday]
                if offsets:
                    return [f"process_offset_{offset}.calculate_folder_date" for offset in offsets]
            return "skip_day"

        branch_task = branch_by_day()

        start_task >> branch_task
        branch_task >> skip_task >> end_task
        
    return generated_dag()

def load_configs():
    # Search for scripts/ directory relative to this file or under AIRFLOW_HOME
    candidates = [
        Path(__file__).resolve().parent / "scripts",
        Path(citadel_config.general.get("airflow_home", "/opt/airflow")) / "dags" / "repo" / "scripts",
    ]
    scripts_dir = None
    for candidate in candidates:
        if candidate.is_dir():
            scripts_dir = candidate
            break
    if scripts_dir is None:
        return
        
    for yaml_path in scripts_dir.rglob("country_config.yaml"):
        with open(yaml_path, "r", encoding="utf-8") as f:
            yaml_config = yaml.safe_load(f)
            if yaml_config and "country" in yaml_config:
                dag_obj = create_country_dag(yaml_config)
                globals()[dag_obj.dag_id] = dag_obj

load_configs()
