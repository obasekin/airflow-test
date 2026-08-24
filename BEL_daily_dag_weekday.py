import pendulum

from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)
from airflow.sensors.base import PokeReturnValue
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from datetime import timedelta
import json

# ============================================================
# BELGIUM TIME
# ============================================================

local_tz = pendulum.timezone("Europe/Brussels")


# ============================================================
# GCS
# ============================================================

GCS_BUCKET_NAME = "arcanor-orion"

GCS_BASE_PATH = "output/mobility/BEL"


# ============================================================
# DEFAULT ARGS
# ============================================================

default_args = {
    "owner": "obasekin",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
    "email": ["obasekin@arcanor.com"],
}


# ============================================================
# DAG
# ============================================================

@dag(
    dag_id="BEL_daily_dag_weekday",
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
            gcp_conn_id="google_cloud_default"
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
        timeout=6 * 60 * 60,
        mode="reschedule",
        execution_timeout=timedelta(hours=6),
        retries=3,
        retry_delay=timedelta(minutes=5),
    )
    def check_manifest_ready(
        folder_name: str,
        k_suffix: str,
    ) -> PokeReturnValue:

        from scripts.BEL.BEL_druid_ingestion.manifest_checker import (
            find_manifest
        )

        print("=" * 80)
        print("CHECK MANIFEST")
        print("=" * 80)
        print(f"Folder : {folder_name}")
        print(f"K      : {k_suffix}")
        print("=" * 80)

        result = find_manifest(
            folder_name=folder_name,
            k_suffix=k_suffix,
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
            gcp_conn_id="google_cloud_default"
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

        from airflow.hooks.base import BaseHook

        from scripts.BEL.BEL_druid_ingestion.ingestion_druid import (
            run_ingestion,
        )

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
        # DRUID CONNECTION
        # ========================================================

        conn = BaseHook.get_connection(
            "druid_default"
        )

        druid_url = (
            conn.host or ""
        ).rstrip("/")

        username = conn.login
        password = conn.password

        if not druid_url:
            raise ValueError(
                "Druid URL is not configured"
            )

        if not username:
            raise ValueError(
                "Druid username is not configured"
            )

        if not password:
            raise ValueError(
                "Druid password is not configured"
            )

        # ========================================================
        # INGESTION
        # ========================================================

        result = run_ingestion(
            parquet_files=parquet_files,
            druid_url=druid_url,
            username=username,
            password=password,
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

        for k in [
            "k1",
            "k2",
            "k3",
            "k4",
        ]:

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


    @task.sensor(
        poke_interval=60 * 5,
        timeout=6 * 60 * 60,
        mode="reschedule",
        execution_timeout=timedelta(hours=6),
        retries=3,
        retry_delay=timedelta(minutes=5),
    )
    def build_ingestion_request(
        folder_name: str,
        **kwargs,
    ) -> PokeReturnValue:
        country = "BEL"
        checker = __import__(
            f"scripts.{country}.{country}_druid_ingestion.manifest_checker",
            fromlist=["find_manifest"],
        )
        hook = GCSHook(gcp_conn_id="google_cloud_default")
        files = []
        for k_suffix in ("k1", "k2", "k3", "k4"):
            manifest = checker.find_manifest(
                folder_name=folder_name,
                k_suffix=k_suffix,
            )
            if manifest is None:
                return PokeReturnValue(is_done=False)
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
        return PokeReturnValue(is_done=True, xcom_value={
            "country": "BEL",
            "source_dag_id": kwargs["dag"].dag_id,
            "files": sorted(set(files)),
        })


    @task_group
    def daily_ingestion_process(
        offset_days: int,
    ):
        folder_task = get_todays_expected_folder(
            target_date_str=calculate_folder_date(offset_days=offset_days),
        )
        request = build_ingestion_request(folder_name=folder_task)
        trigger = TriggerDagRunOperator(
            task_id="trigger_ingestion_process_dag",
            trigger_dag_id="ingestion_process_dag",
            trigger_run_id=(
                "{{ dag.dag_id }}__{{ run_id }}__"
                f"{request.operator.task_id}"
            ),
            conf=request,
            wait_for_completion=True,
            deferrable=True,
            allowed_states=["success"],
            failed_states=["failed"],
            reset_dag_run=False,
        )
        wait = EmptyOperator(
            task_id="wait_ingestion_process_dag",
        )
        folder_task >> request >> trigger >> wait


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