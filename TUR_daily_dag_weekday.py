import os
import re

import pendulum

from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.base import PokeReturnValue
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator

from datetime import timedelta
import json
from pathlib import Path

from citadel.utilities.manifest import find_manifest
from citadel.notifications.email import EmailNotifier, EmailService
from citadel.druid.ingestion import run_ingestion
from citadel.jupyter.jupyter_executor import (
    execute_druid_query_in_notebook,
)
from config.settings import NOTIFICATION_EMAILS, SMTP_CONN_ID



from config import (
    GCS_BUCKET_NAME,
    GCS_CONN_ID,
    NOTIFICATION_EMAILS,
    SMTP_CONN_ID,
    TIMEZONE,
    get_country_base_path,
    get_ingestion_spec_path,
)

# ============================================================
# TURKEY TIME
# ============================================================

local_tz = pendulum.timezone(TIMEZONE)


# ============================================================
# CONFIG
# ============================================================

COUNTRY = "TUR"

GCS_BASE_PATH = get_country_base_path(COUNTRY)

MANIFEST_PREFIXES = {
    "k1": "eskimi",
    "k2": "location",
    "k4": "veraset",
}

failure_email = EmailNotifier(
    to_email=NOTIFICATION_EMAILS,
    conn_id=SMTP_CONN_ID,
)

INGESTION_SPEC = get_ingestion_spec_path(COUNTRY)
FAIL_ON_NOTEBOOK_ERROR = True
QUERY = """
select COUNT(DISTINCT "maid"), "day" from "TUR"
WHERE __time > TIMESTAMP '{start_time}'
  AND __time <= TIMESTAMP '{end_time}'
GROUP BY "day"
"""

NOTEBOOK_CODE = """
from pydruid.db import connect

druid_connection = connect(
    host={host!r},
    port={port!r},
    path={path!r},
    scheme={scheme!r},
    user={username!r},
    password={password!r},
)

druid_cursor = druid_connection.cursor()

query = \"\"\"
{query}
\"\"\"

druid_cursor.execute(query)
result = druid_cursor.fetchall()
print("Result:", result)
"""



# ============================================================
# DEFAULT ARGS
# ============================================================

default_args = {
    "owner": "obasekin",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": failure_email,
}


# ============================================================
# DAG
# ============================================================

@dag(
    dag_id=f"{COUNTRY}_daily_dag_weekday",
    default_args=default_args,
    start_date=pendulum.datetime(
        2026,
        8,
        18,
        tz=local_tz,
    ),
    schedule="0 8 * * *",
    catchup=False,
    tags=[
        "druid",
        "ingestion",
        "taskgroup",
        "idempotent",
        "branching",
    ],
)
def druid_ingestion_workflow():

    # ========================================================
    # 1. CALCULATE TARGET DATE
    # ========================================================

    @task
    def calculate_folder_date(
        offset_days: int,
        **kwargs,
    ) -> str:

        run_date = kwargs["logical_date"]

        target_date = run_date.subtract(
            days=offset_days
        )

        target_date_str = target_date.strftime(
            "%Y-%m-%d"
        )

        print("=" * 80)
        print("CALCULATE FOLDER DATE")
        print("=" * 80)
        print(f"Logical date : {run_date}")
        print(f"Offset days  : {offset_days}")
        print(f"Target date  : {target_date_str}")
        print("=" * 80)

        return target_date_str


    # ========================================================
    # 2. GET / CHECK GCS FOLDER
    # ========================================================

    @task
    def get_todays_expected_folder(
        target_date_str: str,
    ) -> str:

        hook = GCSHook(
            gcp_conn_id=GCS_CONN_ID
        )

        date_path = target_date_str.replace(
            "-",
            "/",
        )

        prefix = (
            f"{GCS_BASE_PATH}/"
            f"{date_path}/"
        )

        folder_path = (
            f"gs://{GCS_BUCKET_NAME}/"
            f"{prefix}"
        )

        print(
            f"Checking GCS folder: "
            f"{folder_path}"
        )

        objects = hook.list(
            bucket_name=GCS_BUCKET_NAME,
            prefix=prefix,
        )

        if not objects:

            raise FileNotFoundError(
                f"GCS folder does not exist "
                f"or is empty: {folder_path}"
            )

        print(
            f"GCS folder exists and contains "
            f"{len(objects)} object(s)."
        )

        for obj in objects:
            print(
                f"  - gs://"
                f"{GCS_BUCKET_NAME}/"
                f"{obj}"
            )

        return folder_path


    # ========================================================
    # 3. MANIFEST SENSOR
    # ========================================================

    @task.sensor(
        poke_interval=60 * 5,
        timeout=16 * 60 * 60,
        mode="reschedule",
        execution_timeout=timedelta(hours=16),
        retries=3,
        retry_delay=timedelta(minutes=5),
    )
    def check_manifest_ready(
        folder_name: str,
        k_suffix: str,
    ) -> PokeReturnValue:


        print("=" * 80)
        print("CHECK MANIFEST")
        print("=" * 80)
        print(f"Folder : {folder_name}")
        print(f"K      : {k_suffix}")
        print("=" * 80)

        result = find_manifest(
            folder_name=folder_name,
            k_suffix=k_suffix,
            manifest_prefixes=MANIFEST_PREFIXES,
        )

        # ----------------------------------------------------
        # Manifest henüz yok
        # ----------------------------------------------------

        if result is None:

            print(
                f"Manifest not found for "
                f"{k_suffix}."
            )

            print(
                "Sensor will retry in "
                "5 minutes."
            )

            return PokeReturnValue(
                is_done=False
            )

        # ----------------------------------------------------
        # Manifest bulundu
        # ----------------------------------------------------

        print("Manifest found!")

        print(
            f"Folder        : "
            f"{result['folder_name']}"
        )

        print(
            f"File name     : "
            f"{result['file_name']}"
        )

        print(
            f"Manifest path : "
            f"{result['manifest_path']}"
        )

        print("=" * 80)

        return PokeReturnValue(
            is_done=True,
            xcom_value=result,
        )


    # ========================================================
    # 4. GET PARQUET FILES
    # ========================================================

    @task(
        execution_timeout=timedelta(hours=0.3),
        retries=2,
    )
    def get_parquet_files(
        manifest_info: dict,
    ) -> list:

        hook = GCSHook(
            gcp_conn_id=GCS_CONN_ID
        )

        folder_name = manifest_info[
            "folder_name"
        ]

        file_name = manifest_info[
            "file_name"
        ]

        # ----------------------------------------------------
        # gs://bucket/path/ formatından
        # bucket ve prefix'i ayır
        # ----------------------------------------------------

        folder_without_scheme = (
            folder_name
            .replace("gs://", "", 1)
        )

        bucket_name, folder_prefix = (
            folder_without_scheme.split(
                "/",
                1,
            )
        )

        folder_prefix = (
            folder_prefix.rstrip("/")
            + "/"
        )

        print("=" * 80)
        print("GET PARQUET FILES")
        print("=" * 80)
        print(f"Folder : {folder_name}")
        print(f"Prefix : {file_name}")
        print("=" * 80)

        # ----------------------------------------------------
        # Folder altındaki bütün object'leri al
        # ----------------------------------------------------

        objects = hook.list(
            bucket_name=bucket_name,
            prefix=folder_prefix,
        )

        parquet_files = []

        for object_name in objects:

            object_file_name = (
                object_name
               .rstrip("/")
                .split("/")[-1]
            )

            # ------------------------------------------------
            # Sadece:
            #
            # prefix ile başlayan
            # VE
            # .parquet ile biten
            #
            # dosyaları al
            # ------------------------------------------------

            if not object_file_name.startswith(
                file_name
            ):
                continue

            if not object_file_name.endswith(
                ".parquet"
            ):
                continue

            parquet_path = (
                f"gs://{bucket_name}/"
                f"{object_name}"
            )

            parquet_files.append(
                parquet_path
            )

        # ----------------------------------------------------
        # Sonuç
        # ----------------------------------------------------

        print(
            f"Found {len(parquet_files)} "
            f"parquet file(s)."
        )

        for parquet_file in parquet_files:
            print(
                f"  - {parquet_file}"
            )

        if not parquet_files:

            raise FileNotFoundError(
                f"No parquet files found "
                f"for prefix '{file_name}' "
                f"in {folder_name}"
            )

        print("=" * 80)

        return parquet_files


    # ========================================================
    # 5. DRUID INGESTION
    # ========================================================

    @task(
        execution_timeout=timedelta(hours=2),
        retries=3,
    )
    def execute_idempotent_druid_ingestion(
        manifest_info: dict,
        parquet_files: list,
        k_suffix: str,
    ):

        # ========================================================
        # MANIFEST INFORMATION
        # ========================================================

        folder_name = manifest_info[
            "folder_name"
        ]

        file_name = manifest_info[
            "file_name"
        ]

        manifest_path = manifest_info[
            "manifest_path"
        ]

        print("=" * 80)
        print("DRUID INGESTION")
        print("=" * 80)

        print(
            f"K             : {k_suffix}"
        )

        print(
            f"Folder        : {folder_name}"
        )

        print(
            f"File name     : {file_name}"
        )

        print(
            f"Manifest path : {manifest_path}"
        )

        print(
            f"Parquet count : {len(parquet_files)}"
        )

        print("=" * 80)

        for parquet_file in parquet_files:
            print(parquet_file)

        # ========================================================
        # INGESTION
        # ========================================================

        result = run_ingestion(
            parquet_files=parquet_files,
            ingestion_spec_path=str(INGESTION_SPEC),
        )

        print("=" * 80)
        print("DRUID INGESTION RESULT")
        print("=" * 80)

        print(
            json.dumps(
                result,
                indent=2,
                ensure_ascii=False,
            )
        )

        print("=" * 80)

        return result


    # ========================================================
    # 6. DAILY INGESTION PROCESS
    # ========================================================

    @task_group
    def legacy_daily_ingestion_process(
        offset_days: int,
    ):

        # ----------------------------------------------------
        # Calculate target date
        # ----------------------------------------------------

        calc_date = calculate_folder_date(
            offset_days=offset_days,
        )

        # ----------------------------------------------------
        # Generate + validate GCS folder
        # ----------------------------------------------------

        folder_task = (
            get_todays_expected_folder(
                target_date_str=calc_date,
            )
        )

        # ----------------------------------------------------
        # K1 / K2 / K3 / K4
        # ----------------------------------------------------

        for k in MANIFEST_PREFIXES:

            @task_group(
                group_id=f"group_{k}",
                ui_color="#F4ECF7",
            )
            def process_k_group(
                folder_arg: str,
                current_k: str,
            ):

                # --------------------------------------------
                # Manifest sensor
                # --------------------------------------------

                manifest_result = (
                    check_manifest_ready.override(
                        task_id=(
                            f"is_manifest_ready_"
                            f"{current_k}"
                        )
                    )(
                        folder_name=folder_arg,
                        k_suffix=current_k,
                    )
                )

                # --------------------------------------------
                # Parquet files
                #
                # Manifest bulunduğunda çalışır.
                # Manifest'in "_" öncesindeki prefix'i
                # kullanarak parquet dosyalarını bulur.
                # --------------------------------------------

                parquet_files = (
                    get_parquet_files.override(
                        task_id=(
                            f"get_parquet_files_"
                            f"{current_k}"
                        )
                    )(
                        manifest_info=manifest_result,
                    )
                )

                # --------------------------------------------
                # Druid ingestion
                # --------------------------------------------

                ingestion = (
                    execute_idempotent_druid_ingestion.override(
                        task_id=(
                            f"ingestion_process_"
                            f"{current_k}"
                        )
                    )(
                        manifest_info=manifest_result,
                        parquet_files=parquet_files,
                        k_suffix=current_k,
                    )
                )

                # Explicit dependencies
                manifest_result >> parquet_files >> ingestion


            k_group = process_k_group(
                folder_arg=folder_task,
                current_k=k,
            )

            folder_task >> k_group


    @task
    def build_ingestion_request(
        folder_name: str,
        manifest_infos: list,
        parquet_files: list,
        ingestion_spec_path: str,
        target_date: str = "",
        **kwargs,
    ) -> dict:

        if not parquet_files:
            raise FileNotFoundError(
                f"No parquet files found in {folder_name}"
            )

        return {
            "country": COUNTRY,
            "source_dag_id": kwargs["dag"].dag_id,
            "files": sorted(set(parquet_files)),
            "ingestion_spec_path": ingestion_spec_path,
            "target_date": target_date,
        }


    @task
    def collect_parquet_files(manifest_infos: list) -> list:
        hook = GCSHook(gcp_conn_id=GCS_CONN_ID)
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


    @task_group
    def daily_ingestion_process(
        offset_days: int,
    ):
        target_date_task = calculate_folder_date(offset_days=offset_days)
        folder_task = get_todays_expected_folder(
            target_date_str=target_date_task,
        )
        manifest_infos = [
            check_manifest_ready.override(
                task_id=f"is_manifest_ready_{k_suffix}"
            )(
                folder_name=folder_task,
                k_suffix=k_suffix,
                manifest_prefix=manifest_prefix,
            )
            for k_suffix, manifest_prefix in MANIFEST_PREFIXES.items()
        ]
        parquet_files = collect_parquet_files(manifest_infos=manifest_infos)
        request = build_ingestion_request(
            folder_name=folder_task,
            manifest_infos=manifest_infos,
            parquet_files=parquet_files,
            ingestion_spec_path=str(INGESTION_SPEC),
            target_date=target_date_task,
        )
        trigger_run_id = (
            "{{ dag.dag_id }}__{{ run_id }}__"
            f"{request.operator.task_id}"
        )

        # Her offset_days grubu için benzersiz, deterministik logical_date.
        # Trigger ve sensor AYNI hesaplamayı yapmalı.
        COUNTRY_SECONDS_OFFSET = sum(
            ord(c) for c in COUNTRY
        ) % 55

        triggered_logical_date_expr = (
            "{{ logical_date + macros.timedelta(minutes="
            f"{offset_days}, seconds={COUNTRY_SECONDS_OFFSET}"
            ") }}"
        )

        def format_logical_date(**kwargs) -> dict:
            logical_date = kwargs["logical_date"]
            start_time = (logical_date - timedelta(days=6)).strftime("%Y-%m-%d 00:00:00")
            end_time = logical_date.strftime("%Y-%m-%d 00:00:00")
            return {"start_time": start_time, "end_time": end_time}


        def execute_notebook_druid_query(**kwargs) -> dict:
            ti = kwargs["ti"]
            
            # Bulunduğumuz task grubuna göre format task'ının id'sini bul
            current_task_id = kwargs["task"].task_id
            if "execute_notebook_druid_query_before" in current_task_id:
                format_task_id = current_task_id.replace("execute_notebook_druid_query_before", "format_logical_date")
            else:
                format_task_id = current_task_id.replace("execute_notebook_druid_query_after", "format_logical_date")
                
            date_range = ti.xcom_pull(task_ids=format_task_id)
            
            if not date_range:
                raise ValueError(f"XCom'dan date_range alınamadı! (Aranan task_id: {format_task_id})")
            
            formatted_query = QUERY.format(
                start_time=date_range["start_time"],
                end_time=date_range["end_time"]
            )
            result = execute_druid_query_in_notebook(
                code=NOTEBOOK_CODE.replace("{query}", formatted_query.strip()),
                fail_on_execution_error=FAIL_ON_NOTEBOOK_ERROR,
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
            
            # TaskGroup isimlerini dinamik olarak çözümlemek için replace kullanıyoruz
            task_id_before = kwargs["task"].task_id.replace("send_email_report", "execute_notebook_druid_query_before")
            task_id_after = kwargs["task"].task_id.replace("send_email_report", "execute_notebook_druid_query_after")
            
            result_before = ti.xcom_pull(task_ids=task_id_before)
            result_after = ti.xcom_pull(task_ids=task_id_after)
            
            subject_title = f"{logical_date_str} {dag_id} {COUNTRY} druid ingestion ended successfully"
            
            html = f"<h2>{subject_title}</h2>\n"
            html += _parse_query_result(result_before, "Before Ingestion")
            html += "<hr>\n"
            html += _parse_query_result(result_after, "After Ingestion")

            email_service = EmailService(conn_id=SMTP_CONN_ID)
            email_service.send_email(
                to=NOTIFICATION_EMAILS,
                subject=subject_title,
                cc="kkalle@arcanor.com",
                html_content=html,
            )


        format_date_task = PythonOperator(
            task_id="format_logical_date",
            python_callable=format_logical_date,
        )

        execute_query_task_before = PythonOperator(
            task_id="execute_notebook_druid_query_before",
            python_callable=execute_notebook_druid_query,
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

            execution_date_fn=lambda logical_date,
                _offset=offset_days,
                _seconds=COUNTRY_SECONDS_OFFSET,
                **kwargs: logical_date + timedelta(
                    minutes=_offset,
                    seconds=_seconds,
                ),

            allowed_states=["success"],
            failed_states=["failed"],
            mode="reschedule",
            poke_interval=60,
            timeout=60 * 60 * 24,
            trigger_rule="none_failed_min_one_success",
        )

        execute_query_task_after = PythonOperator(
            task_id="execute_notebook_druid_query_after",
            python_callable=execute_notebook_druid_query,
        )

        send_email_task = PythonOperator(
            task_id="send_email_report",
            python_callable=send_email_report,
            retries=1,
        )

        folder_task >> manifest_infos >> parquet_files >> request >> format_date_task >> execute_query_task_before >> trigger >> wait >> execute_query_task_after >> send_email_task


    # ========================================================
    # 7. BRANCHING
    # ========================================================

    @task.branch(
        task_id="branch_weekend_or_weekday"
    )
    def check_weekend_or_weekday(
        **kwargs,
    ):

        if kwargs[
            "logical_date"
        ].weekday() in (5, 6):

            return "is_weekend"

        return "is_weekday"


    @task.branch(
        task_id="branch_monday_or_regular"
    )
    def check_monday_or_regular(
        **kwargs,
    ):

        if kwargs[
            "logical_date"
        ].weekday() == 0:

            return "is_monday"

        return "is_regular_weekday"


    # ========================================================
    # 8. MAIN FLOW NODES
    # ========================================================

    start_daily_dag = EmptyOperator(
        task_id="start_daily_dag"
    )

    end_daily_dag = EmptyOperator(
        task_id="end_daily_dag",
        trigger_rule="none_failed_min_one_success",
    )

    check_day_type = (
        check_weekend_or_weekday()
    )

    check_monday_type = (
        check_monday_or_regular()
    )

    is_weekend = EmptyOperator(
        task_id="is_weekend"
    )

    is_weekday = EmptyOperator(
        task_id="is_weekday"
    )

    pass_task = EmptyOperator(
        task_id="pass_task"
    )

    is_monday = EmptyOperator(
        task_id="is_monday"
    )

    is_regular_weekday = EmptyOperator(
        task_id="is_regular_weekday"
    )


    # ========================================================
    # 9. START
    # ========================================================

    start_daily_dag >> check_day_type


    # ========================================================
    # 10. WEEKEND
    # ========================================================

    check_day_type >> is_weekend

    is_weekend >> pass_task >> end_daily_dag


    # ========================================================
    # 11. WEEKDAY
    # ========================================================

    check_day_type >> is_weekday

    is_weekday >> check_monday_type


    # ========================================================
    # 12. NORMAL WEEKDAY - T5
    # ========================================================

    regular_day = (
        daily_ingestion_process.override(
            group_id="process_current_day",
            ui_color="#FDEBD0",
        )(
            offset_days=5
        )
    )

    check_monday_type >> is_regular_weekday

    is_regular_weekday >> regular_day >> end_daily_dag


    # ========================================================
    # 13. MONDAY
    #
    # Saturday -> T7
    # Sunday   -> T6
    # Monday   -> T5
    # ========================================================

    monday_for_sat = (
        daily_ingestion_process.override(
            group_id="process_saturday_T7",
            ui_color="#EAFAF1",
        )(
            offset_days=7
        )
    )

    monday_for_sun = (
        daily_ingestion_process.override(
            group_id="process_sunday_T6",
            ui_color="#E8F4F8",
        )(
            offset_days=6
        )
    )

    monday_for_mon = (
        daily_ingestion_process.override(
            group_id="process_monday_T5",
            ui_color="#FDEBD0",
        )(
            offset_days=5
        )
    )

    check_monday_type >> is_monday

    is_monday >> [
        monday_for_sat,
        monday_for_sun,
        monday_for_mon,
    ]

    monday_for_sat >> end_daily_dag
    monday_for_sun >> end_daily_dag
    monday_for_mon >> end_daily_dag


# ============================================================
# INITIALIZE DAG
# ============================================================

druid_ingestion_workflow()