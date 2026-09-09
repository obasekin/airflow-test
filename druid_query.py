import json
import sys
import os
from dotenv import load_dotenv
import requests

# --- Ortam değişkenlerini yükle ---
load_dotenv()

# --- DRUID KONFİGÜRASYON ---
DRUID_URL = os.getenv("DRUID_URL", "").rstrip("/")
DRUID_USERNAME = os.getenv("DRUID_USERNAME", "")
DRUID_PASSWORD = os.getenv("DRUID_PASSWORD", "")

if not DRUID_URL or not DRUID_USERNAME or not DRUID_PASSWORD:
    print("❌ .env dosyasında eksik konfigürasyon")
    sys.exit(1)

AUTH = (DRUID_USERNAME, DRUID_PASSWORD)

HEADERS = {
    "Content-Type": "application/json"
}


# ============================================================
# SQL QUERY
# ============================================================

sql_query = {
    "query": """
        SELECT *
        FROM "TURtest"
        WHERE "day" IN (4)
        LIMIT 10
    """
}


# ============================================================
# DRUID SQL ENDPOINT
# ============================================================

query_url = f"{DRUID_URL}/druid/v2/sql"


print("=" * 60)
print("DRUID SQL QUERY")
print("=" * 60)

print("Datasource : TUR")
print('Filter     : "day" IN (26)')
print("Limit      : 10")
print()


# ============================================================
# QUERY GÖNDER (RETRY İLE)
# ============================================================

max_retries = 3
retry_delay = 5
response = None

for attempt in range(1, max_retries + 1):
    try:
        print(f"Deneme {attempt}/{max_retries}...")
        
        response = requests.post(
            query_url,
            headers=HEADERS,
            json=sql_query,
            auth=AUTH,
            timeout=120
        )
        
        print("✅ Bağlantı başarılı!")
        break

    except requests.Timeout:
        print(f"⏱️  Timeout ({attempt}/{max_retries})")
        if attempt < max_retries:
            print(f"{retry_delay} saniye sonra yeniden deneniyor...")
            import time
            time.sleep(retry_delay)
        else:
            print("❌ Tüm deneme sürelerinizde zaman aşımı oluştu.")
            sys.exit(1)

    except requests.RequestException as e:
        print("❌ Druid'e bağlanırken hata oluştu:")
        print(e)
        sys.exit(1)


# ============================================================
# RESPONSE
# ============================================================

print("HTTP Status:", response.status_code)
print()


if response.status_code != 200:

    print("❌ SQL sorgusu başarısız.")

    print("Druid response:")
    print(response.text)

    sys.exit(1)


try:

    result = response.json()

except ValueError:

    print("❌ Druid geçerli JSON döndürmedi.")

    print(response.text)

    sys.exit(1)


# ============================================================
# RESULT
# ============================================================

print("✅ SQL sorgusu başarılı.")
print()

print("Sonuç:")

print(
    json.dumps(
        result,
        indent=2,
        ensure_ascii=False
    )
)


print()
print("=" * 60)
print("QUERY TAMAMLANDI")
print("=" * 60)
