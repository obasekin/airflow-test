"""
Example DAG: GCS Utilities (Manifest & Blocklist Reader)
=======================================================
Bu DAG, `citadel.utilities` modülündeki:
- `find_manifest` (GCS üzerindeki klasörde manifest dosyası arama)
- `read_blocklist_geohashes` (GCS üzerindeki ignore.csv dosyasını indirip parse etme)
fonksiyonlarının kullanımını gösterir.

Ne zaman kullanılır?
--------------------
- Veri hazırlık aşamasında ilgili günün manifest dosyasının oluşup oluşmadığını kontrol etmek için
- Druid ingestion öncesi filtrelenecek geohash blocklist listesini GCS'ten çekmek için

Kullanılan Servis:
------------------
- `citadel.utilities.manifest.find_manifest`
- `citadel.utilities.read_csv.read_blocklist_geohashes`
- `citadel.config_loader.get_blocklist_candidates`
- `citadel.config_loader.get_country_base_path`
"""

from datetime import timedelta
import pendulum
from airflow.decorators import dag, task

from citadel.config_loader import (
    config,
    get_blocklist_candidates,
    get_country_base_path,
)
from citadel.utilities.manifest import find_manifest
from citadel.utilities.read_csv import read_blocklist_geohashes

default_args = {
    "owner": "obasekin",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

@dag(
    dag_id="example_citadel_gcs_utilities",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Istanbul"),
    schedule=None,  # Manuel tetiklemeli örnek DAG
    catchup=False,
    tags=["example", "citadel", "gcs", "manifest", "utilities"],
    description="Citadel GCS manifest bulucu ve CSV geohash blocklist okuyucu örneği",
)
def example_gcs_utilities_workflow():

    @task(task_id="check_manifest_example")
    def check_manifest_demo() -> dict:
        """
        find_manifest fonksiyonunu kullanarak GCS üzerindeki bir klasörde
        belirli k-prefix ile eşleşen manifest dosyasını arar.
        """
        country = "TUR"
        target_date_str = "2026/08/18"

        # GCS klasör yolu oluşturma
        country_base = get_country_base_path(country)
        bucket_name = config.gcs.get("bucket_name", "arcanor-orion")
        folder_url = f"gs://{bucket_name}/{country_base}/{target_date_str}/"

        print(f"--- Manifest Arama Örneği ---")
        print(f"Hedef Klasör: {folder_url}")

        # Aranacak prefix eşleştirmeleri (country_config.yaml içindeki manifest_prefixes)
        manifest_prefixes = {
            "k1": "eskimi",
            "k2": "location",
            "k4": "veraset",
        }

        print("Arama kriterleri:", manifest_prefixes)

        # find_manifest çağrısı (Örnek k1 için)
        # Bulunamazsa None döner, bulunursa dict döner:
        # { 'folder_name': ..., 'file_name': ..., 'manifest_path': ... }
        try:
            result = find_manifest(
                folder_name=folder_url,
                k_suffix="k1",
                manifest_prefixes=manifest_prefixes,
            )
            print(f"Manifest sonucu: {result}")
            return result or {"status": "NOT_FOUND_DEMO", "searched_folder": folder_url}
        except Exception as e:
            print(f"Manifest kontrol simülasyonu: {e}")
            return {"status": "DEMO_SIMULATION", "error": str(e)}

    @task(task_id="read_blocklist_example")
    def read_blocklist_demo() -> list:
        """
        read_blocklist_geohashes fonksiyonunu kullanarak GCS üzerindeki
        ignore.csv dosyasını bulur ve içindeki geohash listesini okur.
        """
        country = "TUR"
        date_str = "2026-08-18"

        print(f"\n--- Geohash Blocklist Okuma Örneği ---")
        print(f"Country : {country}")
        print(f"Date    : {date_str}")

        # Aday dosya yollarını inceleyelim
        candidates = get_blocklist_candidates(country, date_str)
        print("Taranacak aday GCS yolları:")
        for c in candidates:
            print(f"  - {c}")

        # read_blocklist_geohashes çağrısı
        # GCS üzerinde dosya bulunamazsa boş liste [] döner
        geohashes = read_blocklist_geohashes(
            country=country,
            date_str=date_str,
            conn_id=config.gcs.get("conn_id", "google_cloud_default"),
        )

        print(f"Okunan geohash sayısı: {len(geohashes)}")
        if geohashes:
            print(f"İlk 5 geohash: {geohashes[:5]}")

        return geohashes

    m_res = check_manifest_demo()
    b_res = read_blocklist_demo()


example_gcs_utilities_workflow()

