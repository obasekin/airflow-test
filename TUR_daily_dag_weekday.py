import pendulum
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.sensors.base import PokeReturnValue
from datetime import timedelta

# TÜRKİYE SAATİ (TRT / UTC+3)
local_tz = pendulum.timezone("Europe/Istanbul")

# --- DEFAULT ARGS ---
default_args = {
    "owner": "obasekin",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
    "email": ["data@arcanor.com"]
}

@dag(
    dag_id="TUR_daily_dag_weekday",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 8, 18, tz=local_tz),
    schedule="0 12 * * *",  # Her gün Türkiye saatiyle öğlen 12:00
    catchup=False,
    tags=["druid", "ingestion", "taskgroup", "idempotent", "branching"]
)
def druid_ingestion_workflow():

    # =========================================================================
    # 1. ORTAK SENSÖR VE INGESTION TASKLARI (Şablon olarak kullanılacaklar)
    # =========================================================================
    
    @task.sensor(
        poke_interval=60 * 5,
        timeout=86400,
        mode="reschedule",
        execution_timeout=timedelta(hours=24),
        retries=3
    )
    def check_manifest_ready(folder_name: str, k_suffix: str) -> PokeReturnValue:
        """ 24 saat boyunca manifest bekleyen sensör. """
        pass

    @task(execution_timeout=timedelta(hours=2), retries=3)
    def execute_idempotent_druid_ingestion(folder_name: str, k_suffix: str):
        """ 2 saat timeout'lu, Idempotent Druid ingestion işlemi. """
        pass

    # =========================================================================
    # 2. GÜNLÜK İŞLEM İSKELETİ (TEKRAR KULLANILABİLİR TASK GROUP)
    # =========================================================================
    # Bu grup, kendisine verilen 'offset_days' (kaç gün geriye gidileceği) 
    # bilgisine göre o günün tüm k1..k4 işlemlerini halleder.
    
    @task_group
    def daily_ingestion_process(offset_days: int):
        
        @task
        def calculate_folder_date(offset: int, **kwargs) -> str:
            run_date = kwargs["logical_date"]
            target_date = run_date.subtract(days=offset)
            return target_date.strftime("%Y-%m-%d")

        @task
        def get_todays_expected_folder(target_date_str: str) -> str:
            return f"/data/druid_ingestion/{target_date_str}/"

        calc_date = calculate_folder_date(offset_days)
        folder_task = get_todays_expected_folder(calc_date)

        for k in ["k1", "k2", "k3", "k4"]:
            @task_group(group_id=f"group_{k}")
            def process_k_group(folder_arg: str, current_k: str):
                manifest = check_manifest_ready.override(task_id=f"is_manifest_ready_{current_k}")(
                    folder_name=folder_arg, 
                    k_suffix=current_k
                )
                ingestion = execute_idempotent_druid_ingestion.override(task_id=f"ingestion_process_{current_k}")(
                    folder_name=folder_arg, 
                    k_suffix=current_k
                )
                manifest >> ingestion

            k_group = process_k_group(folder_arg=folder_task, current_k=k)
            folder_task >> k_group
            
    # =========================================================================
    # 3. DALLANMA (BRANCHING) KURALLARI
    # =========================================================================

    @task.branch(task_id="branch_weekend_or_weekday")
    def check_weekend_or_weekday(**kwargs):
        # 5 = Cumartesi, 6 = Pazar
        if kwargs['logical_date'].weekday() in (5, 6):
            return "is_weekend"
        return "is_weekday"

    @task.branch(task_id="branch_monday_or_regular")
    def check_monday_or_regular(**kwargs):
        # 0 = Pazartesi
        if kwargs['logical_date'].weekday() == 0:
            return "is_monday"
        return "is_regular_weekday"

    # =========================================================================
    # 4. AKIŞ DÜĞÜMLERİ (TASKS) VE BAĞLANTILAR
    # =========================================================================
    
    start = EmptyOperator(task_id="start")
    
    # DİKKAT: Branching kullanıldığında 'end' taskının çalışabilmesi için 
    # trigger_rule "none_failed_min_one_success" olmalıdır. 
    # Yoksa atlanan dallar yüzünden end taskı da atlanır (skipped olur).
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    # Dallanma kontrol tasklarını oluştur
    check_day_type = check_weekend_or_weekday()
    check_monday_type = check_monday_or_regular()

    # Yönlendirme (Dummy/Empty) Taskları
    is_weekend = EmptyOperator(task_id="is_weekend")
    is_weekday = EmptyOperator(task_id="is_weekday")
    pass_task = EmptyOperator(task_id="pass_task") # Hafta sonu hiçbir şey yapmadan geçilecek adım
    
    is_monday = EmptyOperator(task_id="is_monday")
    is_regular_weekday = EmptyOperator(task_id="is_regular_weekday")

    # --- ANA AKIŞI BAĞLAMA ---
    start >> check_day_type

    # SENARYO 1: HAFTA SONU (Direkt end'e gider)
    check_day_type >> is_weekend >> pass_task >> end

    # SENARYO 2: HAFTA İÇİ
    check_day_type >> is_weekday >> check_monday_type

    # SENARYO 2.A: NORMAL HAFTA İÇİ (Salı, Çarşamba, Perşembe, Cuma)
    regular_day = daily_ingestion_process.override(group_id="process_current_day")(offset_days=5)
    
    check_monday_type >> is_regular_weekday >> regular_day >> end

    # SENARYO 2.B: PAZARTESİ (3 Günlük işlem paralel başlar)
    monday_for_sat = daily_ingestion_process.override(group_id="process_saturday_T7")(offset_days=7)
    monday_for_sun = daily_ingestion_process.override(group_id="process_sunday_T6")(offset_days=6)
    monday_for_mon = daily_ingestion_process.override(group_id="process_monday_T5")(offset_days=5)

    check_monday_type >> is_monday >> [monday_for_sat, monday_for_sun, monday_for_mon]
    monday_for_sat >> end
    monday_for_sun >> end
    monday_for_mon >> end

# DAG'ı initialize et
druid_ingestion_workflow()