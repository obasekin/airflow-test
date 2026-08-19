import pendulum
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.sensors.base import PokeReturnValue
from datetime import timedelta

# TÜRKİYE SAATİ (TRT / UTC+3)
local_tz = pendulum.timezone("Europe/Istanbul")

# --- DEFAULT ARGS (TÜM TASKLAR İÇİN GEÇERLİ VARSAYILANLAR) ---
default_args = {
    "owner": "data_engineering",
    "retries": 3,                         # Hata durumunda varsayılan retry sayısı
    "retry_delay": timedelta(minutes=5),  # Hata sonrası yeniden denemeden önce bekleme süresi
    "email_on_failure": True,             # Fail durumunda mail at!
    "email_on_retry": False,              # Sadece fail'da atsın, her retry'da spam yapmasın isterseniz
    "email": ["data_team@sirketiniz.com"] # Uyarıların gideceği mail adresi
}

@dag(
    dag_id="druid_safe_ingestion_workflow",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 8, 18, tz=local_tz),
    schedule="0 12 * * *",  # Her gün Türkiye saatiyle öğlen 12:00
    catchup=False,
    tags=["druid", "ingestion", "taskgroup", "idempotent"]
)
def druid_ingestion_workflow():
    
    # --- 1. DOSYA KONTROL SENSÖRÜ (24 SAAT TIMEOUT) ---
    # timeout=86400 saniye (24 Saat). 24 saat içinde dosya gelmezse sensör FAIL olur.
    # execution_timeout ile task seviyesinde de 24 saat kısıtlaması ekliyoruz.
    @task.sensor(
        poke_interval=60 * 5,  # Her 5 dakikada bir kontrol et (sistemi yormamak için 1 dk yerine 5 dk idealdir)
        timeout=86400,         # Sensörün kendi timeout süresi (24 saat)
        mode="reschedule",     # Bekleme sırasında worker slotunu serbest bırakır
        execution_timeout=timedelta(hours=24), # Task seviyesinde maksimum çalışma süresi
        retries=3              # Sensör fail olursa 3 kere daha baştan (retry) dener
    )
    def check_manifest_ready(folder_name: str, k_suffix: str) -> PokeReturnValue:
        """
        Klasördeki manifest dosyalarını kontrol eder. 24 saat boyunca (5 dk aralıklarla) bekler.
        Gelmezse Fail olur, retry atar ve default_args sayesinde on_failure email'i gönderir.
        """
        pass

    # --- 2. DRUID INGESTION TASKI (2 SAAT TIMEOUT) ---
    # execution_timeout=timedelta(hours=2) sayesinde task, ingestion başlattıktan sonra 2 saati geçerse Airflow tarafından acımasızca kesilir (Killed) ve Fail statüsüne çekilir.
    # Fail olduğunda retry çalışır ve sizin idempotent kodunuz sayesinde işlemi EN BAŞTAN log okuyarak (ingestion devam mı ediyor, bitti mi vs.) kontrol eder.
    @task(
        execution_timeout=timedelta(hours=2),
        retries=3
    )
    def execute_idempotent_druid_ingestion(folder_name: str, k_suffix: str):
        """
        Idempotent Druid ingestion işlemi. (Maksimum 2 saat sürebilir).
        Eğer Druid'de task takılırsa 2 saat sonra Airflow bu taskı fail edip retry atacak.
        Retry attığında kod yine 'Is there any run?' mantığıyla kontrol edeceği için duplicate YARATILMAYACAKTIR.
        """
        pass

    # --- AKIŞIN ANA İSKELETİ ---
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task
    def calculate_folder_date(**kwargs) -> str:
        """ Airflow Logical Date - 5 Gün (T-5) Hesaplaması """
        run_date = kwargs["logical_date"]
        target_date = run_date.subtract(days=5)
        return target_date.strftime("%Y-%m-%d")

    @task
    def get_todays_expected_folder(target_date_str: str) -> str:
        """ T-5 tarihini alıp hedef klasör stringini oluşturur. """
        return f"/data/druid_ingestion/{target_date_str}/"

    # Başlangıç ve Tarih/Klasör Ayarlaması
    calculated_date = calculate_folder_date()
    folder_task = get_todays_expected_folder(target_date_str=calculated_date)

    start >> calculated_date

    # --- K1, K2, K3, K4 İÇİN TASK GROUP (PARALEL DÖNGÜ) ---
    k_list = ["k1", "k2", "k3", "k4"]
    
    for k in k_list:
        
        @task_group(group_id=f"group_{k}")
        def process_group(folder_arg: str, current_k: str):
            
            manifest = check_manifest_ready.override(task_id=f"is_manifest_ready_{current_k}")(
                folder_name=folder_arg, 
                k_suffix=current_k
            )
            
            ingestion = execute_idempotent_druid_ingestion.override(task_id=f"ingestion_process_{current_k}")(
                folder_name=folder_arg, 
                k_suffix=current_k
            )
            
            manifest >> ingestion

        # Grubu yarat ve akışa bağla
        k_group = process_group(folder_arg=folder_task, current_k=k)
        
        folder_task >> k_group >> end

# DAG'ı initialize et
druid_ingestion_workflow()