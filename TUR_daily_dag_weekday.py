import pendulum

from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.sensors.base import PokeReturnValue

from datetime import timedelta


# ============================================================
# TÜRKİYE SAATİ
# ============================================================

local_tz = pendulum.timezone("Europe/Istanbul")


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
    # 1. MANIFEST SENSOR
    # ========================================================

    @task.sensor(
        poke_interval=60 * 5,
        timeout=86400,
        mode="reschedule",
        execution_timeout=timedelta(hours=24),
        retries=3,
    )
    def check_manifest_ready(
        folder_name: str,
        k_suffix: str,
    ) -> PokeReturnValue:

        from scripts.TUR.manifest_checker import find_manifest

        print("=" * 80)
        print("Checking manifest")
        print(f"Folder : {folder_name}")
        print(f"K      : {k_suffix}")
        print("=" * 80)

        result = find_manifest(
            folder_name=folder_name,
            k_suffix=k_suffix,
        )

        # ----------------------------------------------------
        # Manifest henüz gelmemiş
        # ----------------------------------------------------

        if result is None:

            print(
                f"Manifest not found for {k_suffix}. "
                f"Sensor will retry."
            )

            return PokeReturnValue(
                is_done=False
            )

        # ----------------------------------------------------
        # Manifest bulundu
        # ----------------------------------------------------

        print("Manifest found!")
        print(f"Folder        : {result['folder_name']}")
        print(f"File name     : {result['file_name']}")
        print(f"Manifest path : {result['manifest_path']}")

        return PokeReturnValue(
            is_done=True,
            xcom_value=result,
        )


    # ========================================================
    # 2. DRUID INGESTION
    # ========================================================

    @task(
        execution_timeout=timedelta(hours=2),
        retries=3,
    )
    def execute_idempotent_druid_ingestion(
        manifest_info: dict,
        k_suffix: str,
    ):

        folder_name = manifest_info["folder_name"]
        file_name = manifest_info["file_name"]
        manifest_path = manifest_info["manifest_path"]

        print("=" * 80)
        print("Druid ingestion")
        print("=" * 80)

        print(f"K             : {k_suffix}")
        print(f"Folder        : {folder_name}")
        print(f"File name     : {file_name}")
        print(f"Manifest path : {manifest_path}")

        # ====================================================
        # BURASI SONRA DOLDURULACAK
        #
        # Druid ingestion burada manifest_path kullanacak.
        # ====================================================

        pass


    # ========================================================
    # 3. DAILY INGESTION PROCESS
    # ========================================================

    @task_group
    def daily_ingestion_process(offset_days: int):

        # ----------------------------------------------------
        # Calculate target date
        # ----------------------------------------------------

        @task
        def calculate_folder_date(
            offset: int,
            **kwargs,
        ) -> str:

            run_date = kwargs["logical_date"]

            target_date = run_date.subtract(
                days=offset
            )

            return target_date.strftime("%Y-%m-%d")


        # ----------------------------------------------------
        # Generate GCS folder path
        # ----------------------------------------------------

        @task
        def get_todays_expected_folder(
            target_date_str: str,
        ) -> str:

            # 2026-08-13
            #      ↓
            # 2026/08/13

            date_path = target_date_str.replace("-", "/")

            folder = (
                "gs://arcanor-orion/"
                "output/mobility/TUR/"
                f"{date_path}/"
            )

            print(f"Expected GCS folder: {folder}")

            return folder


        calc_date = calculate_folder_date(
            offset_days
        )

        folder_task = get_todays_expected_folder(
            calc_date
        )


        # ----------------------------------------------------
        # K1 / K2 / K3 / K4
        # ----------------------------------------------------

        for k in ["k1", "k2", "k3", "k4"]:

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

                manifest_result = check_manifest_ready.override(
                    task_id=f"is_manifest_ready_{current_k}"
                )(
                    folder_name=folder_arg,
                    k_suffix=current_k,
                )

                # --------------------------------------------
                # Druid ingestion
                # --------------------------------------------

                ingestion = (
                    execute_idempotent_druid_ingestion.override(
                        task_id=f"ingestion_process_{current_k}"
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
    # 4. BRANCHING
    # ========================================================

    @task.branch(
        task_id="branch_weekend_or_weekday"
    )
    def check_weekend_or_weekday(**kwargs):

        if kwargs["logical_date"].weekday() in (5, 6):
            return "is_weekend"

        return "is_weekday"


    @task.branch(
        task_id="branch_monday_or_regular"
    )
    def check_monday_or_regular(**kwargs):

        if kwargs["logical_date"].weekday() == 0:
            return "is_monday"

        return "is_regular_weekday"


    # ========================================================
    # 5. FLOW NODES
    # ========================================================

    start = EmptyOperator(
        task_id="start"
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    check_day_type = check_weekend_or_weekday()

    check_monday_type = check_monday_or_regular()

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
    # 6. MAIN FLOW
    # ========================================================

    start >> check_day_type


    # ========================================================
    # WEEKEND
    # ========================================================

    check_day_type >> is_weekend

    is_weekend >> pass_task >> end


    # ========================================================
    # WEEKDAY
    # ========================================================

    check_day_type >> is_weekday

    is_weekday >> check_monday_type


    # ========================================================
    # NORMAL WEEKDAY
    #
    # T-5
    # ========================================================

    regular_day = daily_ingestion_process.override(
        group_id="process_current_day",
        ui_color="#FDEBD0",
    )(
        offset_days=5
    )

    check_monday_type >> is_regular_weekday

    is_regular_weekday >> regular_day >> end


    # ========================================================
    # MONDAY
    #
    # Saturday -> T-7
    # Sunday   -> T-6
    # Monday   -> T-5
    #
    # Üçü paralel çalışır.
    # ========================================================

    monday_for_sat = daily_ingestion_process.override(
        group_id="process_saturday_T7",
        ui_color="#EAFAF1",
    )(
        offset_days=7
    )


    monday_for_sun = daily_ingestion_process.override(
        group_id="process_sunday_T6",
        ui_color="#E8F4F8",
    )(
        offset_days=6
    )


    monday_for_mon = daily_ingestion_process.override(
        group_id="process_monday_T5",
        ui_color="#FDEBD0",
    )(
        offset_days=5
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