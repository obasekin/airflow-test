import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None


if load_dotenv is not None:
    load_dotenv()


def get_druid_config(
    druid_url: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> Tuple[str, str, str]:
    if druid_url is None and username is None and password is None:
        try:
            from airflow.hooks.base import BaseHook

            conn = BaseHook.get_connection("druid_default")
            druid_url = (conn.host or "").rstrip("/")
            username = conn.login
            password = conn.password
        except Exception:
            pass

    druid_url = (druid_url or os.getenv("DRUID_URL", "")).rstrip("/")
    username = username or os.getenv("DRUID_USERNAME", "")
    password = password or os.getenv("DRUID_PASSWORD", "")

    if not druid_url:
        raise ValueError("Druid URL is not configured")

    if not username:
        raise ValueError("Druid username is not configured")

    if not password:
        raise ValueError("Druid password is not configured")

    return druid_url, username, password


DEFAULT_ROWS: List[Dict[str, Any]] = [
    {
        "timestamp": "2026-08-10T10:00:00Z",
        "user": "ali",
        "action": "click",
        "value": 5,
    },
    {
        "timestamp": "2026-08-10T10:05:00Z",
        "user": "veli",
        "action": "view",
        "value": 2,
    },
    {
        "timestamp": "2026-08-10T10:10:00Z",
        "user": "ali",
        "action": "purchase",
        "value": 100,
    },
    {
        "timestamp": "2026-08-10T10:15:00Z",
        "user": "ayse",
        "action": "click",
        "value": 3,
    },
]


def load_ingestion_spec_template(spec_path: Optional[str] = None) -> Dict[str, Any]:
    if spec_path is None:
        base_dir = Path(__file__).resolve().parent
        spec_path = str(base_dir / "druid_test_data_ingestion_spec.json")

    with open(spec_path, "r", encoding="utf-8") as file:
        return json.load(file)


def build_ingestion_spec(
    datasource_name: str,
    rows: Optional[List[Dict[str, Any]]] = None,
    spec_path: Optional[str] = None,
) -> Dict[str, Any]:
    inline_rows = rows if rows is not None else DEFAULT_ROWS
    inline_data = "\n".join(json.dumps(row) for row in inline_rows)

    spec = load_ingestion_spec_template(spec_path=spec_path)
    spec["spec"]["ioConfig"]["inputSource"]["data"] = inline_data
    spec["spec"]["dataSchema"]["dataSource"] = datasource_name
    return spec


def submit_ingestion_task(
    druid_url: str,
    username: str,
    password: str,
    datasource_name: str,
    rows: Optional[List[Dict[str, Any]]] = None,
    poll_interval: int = 3,
    timeout: int = 30,
) -> Dict[str, Any]:
    headers = {"Content-Type": "application/json"}
    auth = (username, password)
    ingestion_spec = build_ingestion_spec(datasource_name=datasource_name, rows=rows)

    submit_url = f"{druid_url}/druid/indexer/v1/task"
    response = requests.post(
        submit_url,
        headers=headers,
        json=ingestion_spec,
        auth=auth,
        timeout=timeout,
    )

    print("Submit status code:", response.status_code)
    print("Submit response:", response.text)

    if response.status_code != 200:
        raise RuntimeError("Task gönderimi başarısız oldu.")

    task_id = response.json().get("task")
    if not task_id:
        raise RuntimeError("Task ID alınamadı.")

    print(f"\nTask ID: {task_id}")

    status_url = f"{druid_url}/druid/indexer/v1/task/{task_id}/status"
    current_status = "PENDING"

    while True:
        status_response = requests.get(
            status_url,
            headers=headers,
            auth=auth,
            timeout=timeout,
        )
        status_response.raise_for_status()

        status_json = status_response.json()
        current_status = status_json["status"]["status"]
        print(f"Durum: {current_status}")

        if current_status in ("SUCCESS", "FAILED"):
            break

        time.sleep(poll_interval)

    if current_status == "SUCCESS":
        print(
            f"\n✅ Task başarılı! '{datasource_name}' datasource'una data eklendi."
        )
        return {
            "status": "success",
            "task_id": task_id,
            "datasource": datasource_name,
        }

    print("\n❌ Task başarısız oldu.")
    print("Log:")
    print(f"{druid_url}/druid/indexer/v1/task/{task_id}/log")
    raise RuntimeError(f"Task failed with status: {current_status}")


def query_datasource(
    druid_url: str,
    username: str,
    password: str,
    datasource_name: str,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    headers = {"Content-Type": "application/json"}
    payload = {
        "query": f'SELECT * FROM "{datasource_name}" LIMIT {limit}'
    }

    response = requests.post(
        f"{druid_url}/druid/v2/sql",
        headers=headers,
        json=payload,
        auth=(username, password),
        timeout=30,
    )

    print("\nSQL status code:", response.status_code)
    response.raise_for_status()

    result = response.json()
    print("\nSorgu sonucu:")
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return result


def run_ingestion(
    datasource_name: Optional[str] = None,
    rows: Optional[List[Dict[str, Any]]] = None,
    druid_url: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    query_after_ingest: bool = True,
    query_limit: int = 10,
) -> Dict[str, Any]:
    datasource_name = datasource_name or os.getenv(
        "DATASOURCE_NAME_MAIN",
        "test_datasource_deneme",
    )

    druid_url, username, password = get_druid_config(
        druid_url=druid_url,
        username=username,
        password=password,
    )

    result = submit_ingestion_task(
        druid_url=druid_url,
        username=username,
        password=password,
        datasource_name=datasource_name,
        rows=rows,
    )

    if query_after_ingest:
        query_result = query_datasource(
            druid_url=druid_url,
            username=username,
            password=password,
            datasource_name=datasource_name,
            limit=query_limit,
        )
        result["query_result"] = query_result

    return result


if __name__ == "__main__":
    try:
        run_ingestion()
    except Exception as exc:
        print(f"\n❌ Druid ingestion failed: {exc}", file=sys.stderr)
        raise
