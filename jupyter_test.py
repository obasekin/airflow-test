import os
import time
import requests

JUPYTER_URL = os.environ["JUPYTER_URL"]

headers = {
    "CF-Access-Client-Id": os.environ["CF_ACCESS_CLIENT_ID"],
    "CF-Access-Client-Secret": os.environ["CF_ACCESS_CLIENT_SECRET"],
}

# 1. JupyterHub API erişimini test et
r = requests.get(
    f"{JUPYTER_URL}/hub/api",
    headers=headers,
    timeout=60,
)

print("JupyterHub status:", r.status_code)
r.raise_for_status()

# 2. Önce API'nin bize cevap verdiğini doğrula
print("JupyterHub response:", r.text[:500])