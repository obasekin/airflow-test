# Arcanor Airflow & Citadel Ingestion Framework

Bu repo, Apache Druid batch veri yükleme (ingestion), analitik sorgular, JupyterHub üzerinde dinamik/statik notebook çalıştırma, otomatik HTML e-posta raporlama ve GCS yardımcı araçlarını içeren **Airflow DAG** ve **Citadel** servis kütüphanesini barındırır.

OOP ve SOLID prensipleriyle yeniden yapılandırılmış olup, kod tekrarını önleyen **DAG Factory** ve merkezi konfigürasyon (`citadel_config.yaml`) mimarisine sahiptir.

---

## İçindekiler
1. [Genel Mimari ve Dizin Yapısı](#1-genel-mimari-ve-dizin-yap%C4%B1s%C4%B1)
2. [Merkezi Konfigürasyon (`citadel_config.yaml`)](#2-merkezi-konfig%C3%BCrasyon-citadel_configyaml)
3. [Citadel Servisleri ve Örnek DAG'lar](#3-citadel-servisleri-ve-%C3%B6rnek-daglar)
   - [3.1. Druid SQL Query Executor (`example_druid_query_dag.py`)](#31-druid-sql-query-executor)
   - [3.2. Druid Batch Ingestion Service (`example_druid_ingestion_dag.py`)](#32-druid-batch-ingestion-service)
   - [3.3. JupyterHub Dinamik Notebook Çalıştırıcı (`example_jupyter_executor_dag.py`)](#33-jupyterhub-dinamik-notebook-%C3%A7al%C4%B1%C5%9Ft%C4%B1r%C4%B1c%C4%B1)
   - [3.4. JupyterHub Mevcut Notebook Çalıştırıcı (`existing_notebook_runner_dag.py`)](#34-jupyterhub-mevcut-notebook-%C3%A7al%C4%B1%C5%9Ft%C4%B1r%C4%B1c%C4%B1)
   - [3.5. E-posta Servisi ve Hata Bildirimleri (`example_email_service_dag.py`)](#35-e-posta-servisi-ve-hata-bildirimleri)
   - [3.6. GCS Yardımcı Araçları: Manifest & Blocklist (`example_gcs_utilities_dag.py`)](#36-gcs-yard%C4%B1mc%C4%B1-ara%C3%A7lar%C4%B1-manifest--blocklist)
4. [DAG Factory: Konfigürasyon Odaklı Ülke DAG'ları](#4-dag-factory-konfig%C3%BCrasyon-odakl%C4%B1-%C3%BClke-daglar%C4%B1)
5. [4'lü Ingestion Tutarlılık Kontrolü (Guardrail)](#5-4l%C3%BC-ingestion-tutarl%C4%B1l%C4%B1k-kontrol%C3%BC-guardrail)
6. [Yeni Ülke Ekleme Rehberi](#6-yeni-%C3%BClke-ekleme-rehberi)
7. [Airflow Connection Tanımları](#7-airflow-connection-tan%C4%B1mlar%C4%B1)

---

## 1. Genel Mimari ve Dizin Yapısı

```
airflow-test/
├── citadel/                                 # Citadel Servis Kütüphanesi
│   ├── citadel_config.yaml                  # Tüm servislerin merkezi default ayarları
│   ├── config_loader.py                     # Singleton config yükleyici ve path helper'lar
│   ├── druid/                               # Druid servisleri
│   │   ├── credentials.py                   # Airflow connection'dan URL ve auth okuyucu
│   │   ├── ingestion.py                     # DruidIngestionService (Facade) & run_ingestion
│   │   ├── query_executor.py                # Doğrudan HTTP SQL sorgu çalıştırıcı
│   │   ├── spec_loader.py                   # Spec JSON okuyucu, URI injection & 4'lü doğrulama
│   │   └── state_manager.py                 # GCS üzerinde state.json takibi & hashing
│   ├── jupyter/                             # JupyterHub servisleri
│   │   ├── credentials.py                   # JupyterHub token & Cloudflare access auth
│   │   ├── jupyter_executor.py              # Dinamik notebook oluşturup WebSocket ile koşturma
│   │   └── jupyter_runner.py                # Var olan .ipynb dosyasını API ile koşturup kaydetme
│   ├── notifications/                       # Bildirim servisleri
│   │   └── email.py                         # EmailService (HTML mail) & EmailNotifier (Callback)
│   └── utilities/                           # Ortak araçlar
│       ├── manifest.py                      # GCS üzerinde manifest dosyası arama
│       └── read_csv.py                      # GCS ignore.csv'den geohash listesi okuma
├── scripts/                                 # Ülke bazlı konfigürasyon ve spec'ler
│   ├── BEL/
│   │   ├── country_config.yaml              # BEL DAG schedule, offset, feature flag'leri
│   │   └── BEL_druid_ingestion/
│   │       └── ingestion_spec.json          # Druid batch ingestion spec şablonu
│   ├── NLD/ ...
│   └── TUR/ ...
├── dag_factory.py                           # scripts/*/country_config.yaml tarayıp DAG üreten fabrika
├── ingestion_process_dag.py                 # Child DAG: Asıl batch ingestion'ı koşturan alt iş akışı
├── existing_notebook_runner_dag.py          # Örnek: Var olan notebook çalıştırma
├── example_druid_query_dag.py               # Örnek: Druid SQL sorgusu
├── example_druid_ingestion_dag.py           # Örnek: Druid batch ingestion
├── example_jupyter_executor_dag.py          # Örnek: Dinamik Jupyter notebook
├── example_email_service_dag.py             # Örnek: HTML E-posta ve Notifier
└── example_gcs_utilities_dag.py             # Örnek: Manifest arama ve geohash okuma
```

---

## 2. Merkezi Konfigürasyon (`citadel_config.yaml`)

Citadel'deki tüm default ayarlar tek bir dosyada toplanmıştır: `citadel/citadel_config.yaml`.
Kod içinde doğrudan erişim için `citadel.config_loader.config` kullanılır.

### Örnek `citadel_config.yaml`:
```yaml
druid:
  conn_id: "druid_default"
  poll_interval: 60        # Task durum sorgulama sıklığı (saniye)
  max_retries: 3            # Başarısızlık durumunda tekrar deneme sayısı
  status_timeout: 30

jupyter:
  conn_id: "jupyterhub_default"
  timeout: 1800             # Notebook hücre çalışma zaman aşımı (30 dakika)
  default_kernel: "python3"

notifications:
  smtp_conn_id: "smtp_default"
  default_to:
    - "obasekin@arcanor.com"
    - "ucelik@arcanor.com"

gcs:
  conn_id: "google_cloud_default"
  bucket_name: "arcanor-orion"
  mobility_base_path: "output/mobility"
  blocklist_base_path: "control/druid/ingestion/blocklist/geohash"
  blocklist_filename: "ignore.csv"

ingestion:
  allowed_countries:
    - "TUR"
    - "TURv2"
    - "BEL"
    - "NLD"
```

### Kod İçinde Kullanım:
```python
from citadel.config_loader import config

druid_conn = config.druid.get("conn_id")
timeout = config.jupyter.get("timeout", 1800)
bucket = config.gcs.get("bucket_name")
allowed = config.ingestion.get("allowed_countries", [])
```

---

## 3. Citadel Servisleri ve Örnek DAG'lar

Her servis için hazırlanmış örnek DAG'lar `schedule=None` olarak ayarlanmıştır. Airflow UI üzerinden güvenle incelenip manuel tetiklenebilir.

---

### 3.1. Druid SQL Query Executor
- **İlgili Örnek DAG:** [`example_druid_query_dag.py`](file:///Users/omerbasekin/Documents/Local/airflow/airflow-test/example_druid_query_dag.py)
- **Modül:** `citadel.druid.query_executor.execute_query`

#### Çalışma Mekanizması:
1. Airflow Connection (`druid_default`) üzerinden Druid Router / Broker URL'ini ve Basic Auth credential'larını alır.
2. HTTP POST isteğiyle `/druid/v2/sql` endpoint'ine `{ "query": "SELECT ..." }` JSON payload'ı iletir.
3. Sonuçları doğrudan Python `list[dict]` formatında çözer ve geri döner.
4. Ağır Jupyter kernel'larına ihtiyaç duymadan hafif sorguları saniyeler içinde çalıştırır.

#### Kod Örneği:
```python
from citadel.druid.query_executor import execute_query

results = execute_query(
    query="SELECT COUNT(*) FROM TUR WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY",
    conn_id="druid_default",
    timeout=60,
)
for row in results:
    print(row)
```

---

### 3.2. Druid Batch Ingestion Service
- **İlgili Örnek DAG:** [`example_druid_ingestion_dag.py`](file:///Users/omerbasekin/Documents/Local/airflow/airflow-test/example_druid_ingestion_dag.py)
- **Modül:** `citadel.druid.ingestion.DruidIngestionService` ve `run_ingestion`

#### Çalışma Mekanizması:
1. **4'lü Tutarlılık Kontrolü:** `country`, `spec_path`, `parquet_files` ve `dataSource` eşleşmesini doğrular (Bkz. [Bölüm 5](#5-4l%C3%BC-ingestion-tutarl%C4%B1l%C4%B1k-kontrol%C3%BC-guardrail)).
2. **Ingestion Key & GCS State:** Datasource adı ve sıralı parquet dosya listesinin SHA-256 hash'i alınarak `ingestion_key` üretilir.
3. **Idempotency:** GCS üzerinde ilgili key için `state.json` kontrol edilir. Eğer daha önce `SUCCESS` olmuşsa yeni task oluşturulmaz, `SKIPPED` dönülür.
4. **Global Slot Kuyruğu:** Druid üzerinde çalışan aktif bir `index_parallel` task'ı varsa, o task tamamlanana kadar 60 saniyede bir polling yaparak bekler.
5. **Dinamik Spec Injection:** `ingestion_spec.json` okunur; bellek üzerinde `uris` alanına parquet listesi, `transformSpec.filter` alanına ise blocklist geohash'leri dinamik inject edilir (asıl JSON dosyası değiştirilmez).
6. **Takip & Retry:** Task Druid'e submit edilir, `RUNNING` durumu GCS'e yazılır ve periyodik olarak izlenir. Başarısızlık durumunda `max_retries` sınırına kadar slot bekleyerek yeniden dener.

#### Kod Örneği:
```python
# Yöntem 1: OOP Servis Yaklaşımı
from citadel.druid.ingestion import DruidIngestionService

service = DruidIngestionService(poll_interval=30, max_retries=3)
result = service.run(
    parquet_files=["gs://arcanor-orion/output/mobility/TUR/2026/08/18/part-0.parquet"],
    ingestion_spec_path="scripts/TUR/TUR_druid_ingestion/ingestion_spec.json",
    country="TUR",
)

# Yöntem 2: Tek Satırlık Wrapper
from citadel.druid.ingestion import run_ingestion

result = run_ingestion(
    parquet_files=["gs://arcanor-orion/output/mobility/BEL/2026/08/18/part-0.parquet"],
    ingestion_spec_path="scripts/BEL/BEL_druid_ingestion/ingestion_spec.json",
    country="BEL",
)
```

---

### 3.3. JupyterHub Dinamik Notebook Çalıştırıcı
- **İlgili Örnek DAG:** [`example_jupyter_executor_dag.py`](file:///Users/omerbasekin/Documents/Local/airflow/airflow-test/example_jupyter_executor_dag.py)
- **Modül:** `citadel.jupyter.jupyter_executor`

#### Çalışma Mekanizması:
1. `jupyterhub_default` connection'ından JupyterHub URL'i, API Token ve Cloudflare Access Service Token (`CF-Access-Client-Id` / `CF-Access-Client-Secret`) bilgilerini okur.
2. REST API üzerinden kullanıcı sunucusunda dinamik olarak yeni bir notebook dosyası (`airflow_notebook_<timestamp>.ipynb`) yaratır.
3. Jupyter WebSocket API (`/api/kernels/<kernel_id>/channels`) üzerinden kernel'a bağlanır.
4. Kod hücresini kernel'da `execute_request` mesajı ile koşturur; `stream` (stdout/stderr), `execute_result` ve `display_data` çıktılarını canlı dinler.
5. Çalışma bittiğinde notebook'u çıktılarıyla beraber kaydeder, kernel oturumunu kapatır ve oluşturulan notebook'un HTTP URL'ini döner.

#### Kod Örneği:
```python
from citadel.jupyter.jupyter_executor import execute_notebook_code

result = execute_notebook_code(
    code="import pandas as pd\nprint(pd.__version__)",
    timeout=1800,
    fail_on_execution_error=True,
)
print("Notebook Linki:", result["notebook_url"])
print("Hücre Çıktıları:", result["outputs"])
```

---

### 3.4. JupyterHub Mevcut Notebook Çalıştırıcı
- **İlgili Örnek DAG:** [`existing_notebook_runner_dag.py`](file:///Users/omerbasekin/Documents/Local/airflow/airflow-test/existing_notebook_runner_dag.py)
- **Modül:** `citadel.jupyter.jupyter_runner.run_existing_notebook`

#### Çalışma Mekanizması:
1. Yeni bir notebook yaratmak yerine, sunucuda önceden var olan bir `.ipynb` dosyasını (`notebook_path`) Jupyter Contents API ile çeker.
2. Notebook içindeki tüm `code` hücrelerini sırayla tek bir kernel oturumu üzerinde koşturur (önceki hücrelerin değişkenleri sonrakilere aktarılır).
3. Hücre çıktılarını güncellenmiş haliyle tekrar aynı notebook dosyasına geri kaydeder.
4. Airflow'dan hazır analitik ve rapor notebook'larını parametrik olarak tetiklemek için idealdir.

#### Kod Örneği:
```python
from citadel.jupyter.jupyter_runner import run_existing_notebook

result = run_existing_notebook(
    notebook_path="reports/daily_executive_report.ipynb",
    timeout=1800,
    fail_on_execution_error=True,
)
print("Çalıştırılan Notebook:", result["notebook_url"])
```

---

### 3.5. E-posta Servisi ve Hata Bildirimleri
- **İlgili Örnek DAG:** [`example_email_service_dag.py`](file:///Users/omerbasekin/Documents/Local/airflow/airflow-test/example_email_service_dag.py)
- **Modül:** `citadel.notifications.email`

#### Çalışma Mekanizması:
- **`EmailService`:** `smtp_default` Airflow Connection üzerinden SMTP host, port, user ve şifresini okur. `to`, `cc`, `subject` ve `html_content` parametreleriyle HTML formatında e-posta gönderir.
- **`EmailNotifier`:** Airflow `BaseNotifier` sınıfından türetilmiştir. DAG veya Task seviyesinde `on_failure_callback=EmailNotifier(...)` şeklinde tanımlandığında, task fail olduğu anda hata logu ve context linkiyle birlikte alıcılara otomatik alarm maili atar.

#### Kod Örneği:
```python
from citadel.notifications.email import EmailService, EmailNotifier

# 1. Callback Tanımı (Task veya DAG default_args içine)
failure_notifier = EmailNotifier(
    to_email=["alerts@arcanor.com"],
    conn_id="smtp_default",
)

# 2. Rapor Maili Gönderme
service = EmailService()
service.send_email(
    to=["team@arcanor.com"],
    subject="Günlük İşlem Özeti",
    html_content="<h1>Rapor</h1><p>Tüm adımlar başarıyla bitti.</p>",
    cc="manager@arcanor.com",
)
```

---

### 3.6. GCS Yardımcı Araçları: Manifest & Blocklist
- **İlgili Örnek DAG:** [`example_gcs_utilities_dag.py`](file:///Users/omerbasekin/Documents/Local/airflow/airflow-test/example_gcs_utilities_dag.py)
- **Modül:** `citadel.utilities`

#### Çalışma Mekanizması:
- **`find_manifest`:** GCS üzerindeki hedef klasörde `_manifest` ile biten veya `k1`, `k2`, `k4` gibi prefix kurallarına uyan dosyayı bulur. Manifest henüz hazır değilse sensor mantığında `None` döner.
- **`read_blocklist_geohashes`:** İlgili ülke ve tarih için aday GCS yollarını (`get_blocklist_candidates`) sırayla tarar. Bulduğu `ignore.csv` dosyasını GCSHook ile indirir ve filtrelenecek geohash'leri `list[str]` olarak parse eder.

#### Kod Örneği:
```python
from citadel.utilities.manifest import find_manifest
from citadel.utilities.read_csv import read_blocklist_geohashes

# Manifest kontrolü
manifest = find_manifest(
    folder_name="gs://arcanor-orion/output/mobility/TUR/2026/08/18/",
    k_suffix="k1",
    manifest_prefixes={"k1": "eskimi"},
)

# Geohash listesi alma
blocklist = read_blocklist_geohashes(
    country="TUR",
    date_str="2026-08-18",
)
print(f"{len(blocklist)} adet geohash filtrelenecek.")
```

---

## 4. DAG Factory: Konfigürasyon Odaklı Ülke DAG'ları

Eski yapıda her ülke için (`TUR_daily_dag_weekday.py`, `BEL_daily_dag_weekday.py`, `NLD_daily_dag_weekday.py`) yüzlerce satırlık kopya kod bulunuyordu.
Yeni yapıda **tek bir [`dag_factory.py`](file:///Users/omerbasekin/Documents/Local/airflow/airflow-test/dag_factory.py)** dosyası, `scripts/*/country_config.yaml` dosyalarını dinamik olarak okuyup her ülke için `<COUNTRY>_daily_dag` üretir.

### Özellikler:
1. **Config-Driven Schedule & Run Days:**
   Eski haftasonu / pazartesi branching (`is_weekend`, `is_monday`, `pass_task`) kalktı.
   Hangi gün hangi offset'in çalışacağı doğrudan config'deki `run_days` matrisinden yönetilir:
   ```yaml
   run_days:
     0: [5]       # Pazartesi: T-5 offset'i çalıştır
     1: [5]       # Salı: T-5 offset'i çalıştır
     5: [5, 6]    # Cumartesi: T-5 ve T-6 paralel çalıştır
     # 6 yok -> Pazar günü çalışmaz, Airflow task'ları otomatik atlar (skip_day task'ı yoktur)
   ```
2. **Feature Flags:**
   İstenen özellikler config üzerinden anında açılıp kapatılabilir:
   ```yaml
   features:
     druid_query_before: true   # Ingestion öncesi Druid kontrol sorgusu
     druid_query_after: true    # Ingestion sonrası Druid doğrulama sorgusu
     send_email_report: true    # E-posta raporu gönder
   ```
3. **Özelleştirilebilir Notebook Kodu:**
   Her ülke kendi `notebook_code` şablonunu ve `druid_query` sorgusunu config dosyasında bağımsız olarak tanımlayabilir.

---

## 5. 4'lü Ingestion Tutarlılık Kontrolü (Guardrail)

Veri kirlenmesini ve yanlış ülkeye veri yazılmasını önlemek amacıyla her ingestion işleminde **4'lü sıkı kontrol** uygulanır:

```
                ┌──────────────────────────────────────────────┐
                │ 1. Flow Country (Örn: "BEL")                │
                └──────────────────────┬───────────────────────┘
                                       │
        ┌──────────────────────────────┼──────────────────────────────┐
        ▼                              ▼                              ▼
┌───────────────────────┐  ┌───────────────────────┐  ┌───────────────────────┐
│ 2. Spec Klasör Yolu   │  │ 3. Datanın Geldiği Yer│  │ 4. Datanın Gittiği Yer│
│ scripts/BEL/          │  │ Parquet URI içinde    │  │ spec.json içindeki    │
│ BEL_druid_ingestion/  │  │ ".../BEL/..."         │  │ "dataSource": "BEL"   │
│ ingestion_spec.json   │  │ bulunmak zorundadır   │  │ olmak zorundadır      │
└───────────────────────┘  └───────────────────────┘  └───────────────────────┘
```

Bu 4 unsurdan herhangi biri eşleşmezse, sistem `ValueError` fırlatarak ingestion'ı başlamadan durdurur.

---

## 6. Yeni Ülke Ekleme Rehberi

Yeni bir ülke (örneğin Fransa - `FRA`) eklemek için **hiçbir Python kodunu değiştirmenize gerek yoktur:**

1. **`citadel_config.yaml` dosyasına ülkeyi ekleyin:**
   ```yaml
   ingestion:
     allowed_countries:
       - "FRA"
   ```
2. **`scripts/FRA/` dizinini oluşturun:**
   ```bash
   mkdir -p scripts/FRA/FRA_druid_ingestion
   ```
3. **`scripts/FRA/country_config.yaml` dosyasını ekleyin:**
   `scripts/TUR/country_config.yaml` dosyasını kopyalayıp `country: "FRA"`, schedule, manifest_prefixes ve query ayarlarını düzenleyin.
4. **`scripts/FRA/FRA_druid_ingestion/ingestion_spec.json` dosyasını ekleyin:**
   Spec içindeki `"dataSource": "FRA"` olduğundan emin olun.
5. **Hazır!** Airflow otomatik olarak `FRA_daily_dag` isimli yeni DAG'ı keşfedecektir.

---

## 7. Airflow Connection Tanımları

Citadel servislerinin çalışması için Airflow Admin -> Connections ekranında aşağıdaki bağlantıların tanımlı olması gereklidir:

| Connection ID | Conn Type | Açıklama |
|---|---|---|
| `druid_default` | HTTP | Druid Router Host, Port ve Basic Auth bilgileri |
| `jupyterhub_default` | HTTP | JupyterHub Host, Extra JSON içine `{"api_token": "...", "cf_access_client_id": "...", "cf_access_client_secret": "..."}` |
| `smtp_default` | Email / SMTP | SMTP Host, Port, Kullanıcı adı ve Şifresi |
| `google_cloud_default` | Google Cloud | GCS erişim Service Account Key veya Workload Identity |
