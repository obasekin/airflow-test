"""
Example DAG: Druid Ingestion Service
====================================
Bu DAG, `citadel.druid.ingestion` modülündeki `DruidIngestionService` sınıfı
ve `run_ingestion` wrapper fonksiyonunun kullanımını gösterir.

Ne zaman kullanılır?
--------------------
- GCS üzerindeki Parquet dosyalarını Druid'e index_parallel olarak yüklemek istediğinizde
- Idempotent (mükerrer çalışmayı engelleyen) ve state takipli batch ingestion çalıştırmak için

Önemli Özellikler:
-------------------
1. Global Slot Kontrolü: Druid'de çalışan başka bir index_parallel task varsa bekler.
2. 4'lü Tutarlılık Kontrolü: Ülke kodu (TUR), spec klasörü (TUR_druid_ingestion),
   parquet yolları (/TUR/) ve spec içindeki dataSource (TUR) eşleşmek zorundadır.
3. GCS State Yönetimi: Durumu GCS bucket'ına (state.json) yazar, tekrar çalışırsa SKIPPED döner.

Kullanılan Servis:
------------------
- `citadel.druid.ingestion.DruidIngestionService`
- `citadel.druid.ingestion.run_ingestion`
"""

from datetime import timedelta
import pendulum
from airflow.decorators import dag, task

from citadel.config_loader import config, get_ingestion_spec_path
from citadel.druid.ingestion import DruidIngestionService, run_ingestion

default_args = {
    "owner": "obasekin",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="example_citadel_druid_ingestion",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Istanbul"),
    schedule=None,  # Manuel tetiklemeli örnek DAG
    catchup=False,
    tags=["example", "citadel", "druid", "ingestion"],
    description="Citadel DruidIngestionService ve run_ingestion batch ingestion örneği",
)
def example_druid_ingestion_workflow():

    @task(task_id="ingestion_via_oop_service")
    def run_via_service_class() -> dict:
        """
        Yöntem 1: DruidIngestionService sınıfını doğrudan kullanarak çalıştırma (OOP).
        Gelişmiş ayarları (retry, timeout, poll_interval) override etmek için idealdir.
        """
        country = "TUR"

        # 1. Dummy Parquet dosyaları (GCS URI'larında /TUR/ yer almalı)
        dummy_parquet_files = [
            "gs://arcanor-orion/output/mobility/TUR/2026/08/18/dummy_part_000.parquet",
            "gs://arcanor-orion/output/mobility/TUR/2026/08/18/dummy_part_001.parquet",
        ]

        # 2. Spec dosyasının yolu (scripts/TUR/TUR_druid_ingestion/ingestion_spec.json)
        spec_path = str(get_ingestion_spec_path(country))

        # 3. Opsiyonel blocklist geohash'leri (filtrelemek istenen bölgeler)
        dummy_blocklist = ["sp6w", "sp6x", "sp6y"]

        print(f"--- Yöntem 1: DruidIngestionService (OOP) ---")
        print(f"Country   : {country}")
        print(f"Spec Path : {spec_path}")
        print(f"Files     : {dummy_parquet_files}")

        # Servisi ilklendiriyoruz (parametreler verilmezse citadel_config.yaml default'ları kullanılır)
        service = DruidIngestionService(
            conn_id=config.druid.get("conn_id", "druid_default"),
            poll_interval=config.druid.get("poll_interval", 60),
            max_retries=config.druid.get("max_retries", 3),
        )

        # NOT: Gerçek bir submit yerine bu örnekte parametrelerin nasıl verildiği gösterilmektedir.
        # Canlıda çalıştırmak için:
        # result = service.run(
        #     parquet_files=dummy_parquet_files,
        #     ingestion_spec_path=spec_path,
        #     blocklist_geohashes=dummy_blocklist,
        #     country=country,
        # )
        # return result

        return {
            "method": "DruidIngestionService",
            "country": country,
            "status": "DEMO_READY",
            "spec_path": spec_path,
            "file_count": len(dummy_parquet_files),
        }

    @task(task_id="ingestion_via_convenience_function")
    def run_via_function() -> dict:
        """
        Yöntem 2: run_ingestion kolaylık (convenience) fonksiyonunu kullanarak çalıştırma.
        Tek satırda basit çağrılar için uygundur.
        """
        country = "BEL"

        # Dummy Parquet dosyaları (GCS URI'larında /BEL/ yer almalı)
        dummy_parquet_files = [
            "gs://arcanor-orion/output/mobility/BEL/2026/08/18/dummy_part_000.parquet"
        ]
        spec_path = str(get_ingestion_spec_path(country))

        print(f"--- Yöntem 2: run_ingestion fonksiyonu ---")
        print(f"Country   : {country}")
        print(f"Spec Path : {spec_path}")

        # Canlıda çalıştırmak için:
        # result = run_ingestion(
        #     parquet_files=dummy_parquet_files,
        #     ingestion_spec_path=spec_path,
        #     country=country,
        # )
        # return result

        return {
            "method": "run_ingestion",
            "country": country,
            "status": "DEMO_READY",
            "spec_path": spec_path,
        }

    res1 = run_via_service_class()
    res2 = run_via_function()


example_druid_ingestion_workflow()
