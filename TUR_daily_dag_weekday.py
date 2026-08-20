import pendulum

from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.sensors.base import PokeReturnValue
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from datetime import timedelta


# ============================================================
# TÜRKİYE SAATİ
# ============================================================

local_tz = pendulum.timezone("Europe/Istanbul")


# ============================================================
# GCS BASE PATH
# ============================================================

GCS_BASE_PATH = "gs://arcanor-orion/output/mobility/TUR"


# ============================================================
# DEFAULT ARGS
# ============================================================

default_args = {
    "owner": "obasekin",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
    "email": ["data@arcanor.com"],
}


# ============================================================
# DAG
# ============================================================

@dag(
    dag_id="TUR_daily_dag_weekday",
    default_args=default_args,
    start_date=pendulum.datetime(
        2026,
        8,
        18,
        tz=local_tz,
    ),
    schedule="0 12 * * *",
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
        """
        DAG logical date'inden offset_days kadar geriye gider.

        Örnek:

            logical_date = 2026-08-20
            offset_days  = 5

            result = 2026-08-15

        Ay ve yıl geçişlerini Pendulum otomatik yönetir.

        Örnek:

            2026-01-03 - 5 gün
            = 2025-12-29
        """

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

        bucket_name = "arcanor-orion"

        date_path = target_date_str.replace("-", "/")

        prefix = (
            f"output/mobility/TUR/"
            f"{date_path}/"
        )

        folder_path = (
            f"gs://{bucket_name}/{prefix}"
        )

        print(f"Checking GCS folder: {folder_path}")

        # Folder içerisinde object var mı?
        objects = hook.list(
            bucket_name=bucket_name,
            prefix=prefix,
        )

        if not objects:
            raise FileNotFoundError(
                f"GCS folder does not exist or is empty: "
                f"{folder_path}"
            )

        print(
            f"GCS folder exists and contains "
            f"{len(objects)} object(s)."
        )

        for obj in objects:
            print(f"  - gs://{bucket_name}/{obj}")

        return folder_path


    # ========================================================
    # 3. MANIFEST SENSOR
    # ========================================================

    @task.sensor(
        poke_interval=60 * 5,
        timeout=6 * 60 * 60,
        mode="reschedule",
        execution_timeout=timedelta(hours=24),
        retries=3,
        retry_delay=timedelta(minutes=5),
    )
    def check_manifest_ready(
        folder_name: str,
        k_suffix: str,
    ) -> PokeReturnValue:

        from scripts.TUR.manifest_checker import (
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
                f"Manifest not found for {k_suffix}."
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
    # 4. DRUID INGESTION
    # ========================================================

    @task(
        execution_timeout=timedelta(hours=2),
        retries=3,
    )
    def execute_idempotent_druid_ingestion(
        manifest_info: dict,
        k_suffix: str,
    ):

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

        print(f"K             : {k_suffix}")
        print(f"Folder        : {folder_name}")
        print(f"File name     : {file_name}")
        print(f"Manifest path : {manifest_path}")

        # ====================================================
        # Druid ingestion burada yapılacak.
        # ====================================================

        pass


    # ========================================================
    # 5. DAILY INGESTION PROCESS
    # ========================================================

    @task_group
    def daily_ingestion_process(
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
                        k_suffix=current_k,
                    )
                )

                manifest_result >> ingestion


            k_group = process_k_group(
                folder_arg=folder_task,
                current_k=k,
            )

            folder_task >> k_group


    # ========================================================
    # 6. BRANCHING
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
    # 7. MAIN FLOW NODES
    # ========================================================

    start = EmptyOperator(
        task_id="start"
    )

    end = EmptyOperator(
        task_id="end",
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
    # 8. START
    # ========================================================

    start >> check_day_type


    # ========================================================
    # 9. WEEKEND
    # ========================================================

    check_day_type >> is_weekend

    is_weekend >> pass_task >> end


    # ========================================================
    # 10. WEEKDAY
    # ========================================================

    check_day_type >> is_weekday

    is_weekday >> check_monday_type


    # ========================================================
    # 11. NORMAL WEEKDAY
    #
    # T-5
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

    is_regular_weekday >> regular_day >> end


    # ========================================================
    # 12. MONDAY
    #
    # Saturday -> T-7
    # Sunday   -> T-6
    # Monday   -> T-5
    #
    # Üçü paralel çalışır.
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

    monday_for_sat >> end
    monday_for_sun >> end
    monday_for_mon >> end


# ============================================================
# INITIALIZE DAG
# ============================================================

druid_ingestion_workflow()