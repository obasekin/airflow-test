import pendulum
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.sensors.base import PokeReturnValue
from datetime import timedelta

# TÜRKİYE SAATİ (TRT / UTC+3)
local_tz = pendulum.timezone("Europe/Istanbul")

# --- DEFAULT ARGS (TÜM TASKLAR İÇİN GEÇERLİ VARSAYILANLAR) ---
default_args = {
    "owner": "obasekin",
    "retries": 3,                         # Hata durumunda varsayılan retry sayısı
    "retry_delay": timedelta(minutes=5),  # Hata sonrası yeniden denemeden önce bekleme süresi
    "email_on_failure": True,             # Fail durumunda mail at!
    "email_on_retry": False,              # Sadece fail'da atsın, her retry'da spam yapmasın isterseniz
    "email": ["obasekin@arcanor.com"] # Uyarıların gideceği mail adresi
}

@dag(
    dag_id="TUR_daily_dag",
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

        Görseldeki Adım: 'Manifest Check' -> 'does all files ready' -> False ise 'wait' döngüsü.
        Not: Klasördeki dosyaların tam olup olmadığı (manifest) kontrol edilir.
        - Eğer dosyalar tamamsa: return PokeReturnValue(is_done=True) -> Bir sonraki taska geçer.
        - Eğer eksikse: return PokeReturnValue(is_done=False) -> Airflow uykuya dalar (wait) ve poke_interval süresi sonra tekrar bakar.
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

        Görseldeki Adım: 'Druid Check' -> 'Start Ingestion' -> 'Check Druid Run Status' -> Fail ise başa dön.
        
        Kritik İhtiyaç Çözümü: Taskın körü körüne retry edip duplicate oluşturmaması için tüm bu blok tek task içindedir.
        Airflow bu taskı retry ettiğinde işlemler her seferinde şu sırayla çalışır:
        
        1. DRUID & BUCKET LOG CHK ('Is there any run?'):
           - Loglardan veya Druid API'den bu ingestion'ın daha önce tetiklenip tetiklenmediği kontrol edilir.
        
        2. DURUM YÖNETİMİ ('Is there same run Success or Running?'):
           - Durum SUCCESS ise: Task başarılı sayılır ve işlem bitirilir (return).
           - Durum RUNNING ise: Ingestion zaten devam ediyordur, bitene kadar beklenir (While Running -> wait status).
        
        3. START INGESTION:
           - Eğer önceki kontrollerden bir run bulunamazsa (False), İŞTE SADECE O ZAMAN 'Start Ingestion' başlatılır.
        
        4. CHECK DRUID RUN STATUS:
           - Başlatılan veya mevcut olan run'ın bitmesi beklenir.
           - Başarılı olursa (Success) işlem tamamlanır.
           - Hata alırsak (Fail) Exception fırlatılır. 
        
        *SONUÇ*: Exception fırlatıldığında Airflow taskı baştan başlatır (1. adıma döner). 
        Böylece log/run kontrolü yapmadan asla yeni bir Ingestion POST isteği atılmaz!
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
    """
    Görseldeki Adım: 'whats todays expected folder/files' (DAG Run Date - T-5 is Folder name)
    Not: Burada T-5 gününün klasör adı hesaplanır.
    - Gerekli tarih işlemleri yapılır.
    - Hedef klasör yolu string olarak return edilir.
    """  

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