import hashlib
import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from airflow.providers.google.cloud.hooks.gcs import GCSHook


# ============================================================
# CONFIG
# ============================================================

GCS_CONN_ID = "google_cloud_default"

LOG_BUCKET = "arcanor-airflow-logs"

LOG_PREFIX = "logs/ingestion"

POLL_INTERVAL = 10

MAX_RETRIES = 3


logger = logging.getLogger(__name__)


# ============================================================
# SPEC FILE
# ============================================================

SPEC_FILE = (
    Path(__file__).resolve().parent
    / "ingestion_spec.json"
)


# ============================================================
# TIME
# ============================================================

def now():
    return datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )


# ============================================================
# GCS
# ============================================================

def get_gcs_hook():
    return GCSHook(
        gcp_conn_id=GCS_CONN_ID
    )


# ============================================================
# INGESTION KEY
# ============================================================

def calculate_ingestion_key(
    datasource_name: str,
    parquet_files: List[str],
) -> str:
    """
    Aynı datasource + aynı parquet file listesi
    aynı ingestion key'i üretir.

    File sırası önemli değildir.
    """

    sorted_files = sorted(parquet_files)

    payload = (
        f"datasource={datasource_name}\n"
        f"files=\n"
        + "\n".join(sorted_files)
    )

    return hashlib.sha256(
        payload.encode("utf-8")
    ).hexdigest()


# ============================================================
# INPUT FOLDER / DATE
# ============================================================

def extract_date_from_parquet_files(
    parquet_files: List[str],
) -> str:
    """
    Örnek input:

    gs://arcanor-orion/output/mobility/TUR/2026/08/15/xxx.parquet

    Return:

    2026/08/15
    """

    for parquet_file in parquet_files:

        path = (
            parquet_file
            .replace("gs://", "", 1)
            .split("/")
        )

        for i in range(len(path) - 2):

            year = path[i]
            month = path[i + 1]
            day = path[i + 2]

            if (
                len(year) == 4
                and len(month) == 2
                and len(day) == 2
                and year.isdigit()
                and month.isdigit()
                and day.isdigit()
            ):
                return (
                    f"{year}/"
                    f"{month}/"
                    f"{day}"
                )

    raise ValueError(
        "Could not extract date from parquet files"
    )


# ============================================================
# STATE PATH
# ============================================================

def get_state_object_name(
    datasource_name: str,
    parquet_files: List[str],
    ingestion_key: str,
) -> str:

    date_path = (
        extract_date_from_parquet_files(
            parquet_files
        )
    )

    return (
        f"{LOG_PREFIX}/"
        f"{datasource_name}/"
        f"{date_path}/"
        f"{ingestion_key}/"
        f"state.json"
    )


# ============================================================
# READ STATE
# ============================================================

def read_state(
    object_name: str,
) -> Optional[Dict[str, Any]]:

    hook = get_gcs_hook()

    if not hook.exists(
        bucket_name=LOG_BUCKET,
        object_name=object_name,
    ):
        return None

    data = hook.download(
        bucket_name=LOG_BUCKET,
        object_name=object_name,
    )

    return json.loads(
        data.decode("utf-8")
    )


# ============================================================
# WRITE STATE
# ============================================================

def write_state(
    object_name: str,
    ingestion_key: str,
    datasource_name: str,
    parquet_files: List[str],
    task_id: str,
    status: str,
    retry_count: int,
):
    """
    State yalnızca önemli state değişikliklerinde yazılır.

    Polling sırasında her 10 saniyede bir yazılmaz.
    """

    state = {
        "ingestion_key": ingestion_key,
        "datasource": datasource_name,
        "file_count": len(parquet_files),
        "files": sorted(parquet_files),
        "task_id": task_id,
        "status": status,
        "retry_count": retry_count,
        "created_at": now(),
    }

    hook = get_gcs_hook()

    hook.upload(
        bucket_name=LOG_BUCKET,
        object_name=object_name,
        data=json.dumps(
            state,
            indent=2,
            ensure_ascii=False,
        ),
        mime_type="application/json",
    )

    logger.info(
        "State written: gs://%s/%s",
        LOG_BUCKET,
        object_name,
    )


# ============================================================
# DRUID TASK STATUS
# ============================================================

def get_task_status(
    task_id: str,
    druid_url: str,
    username: str,
    password: str,
) -> str:

    url = (
        f"{druid_url}"
        f"/druid/indexer/v1/task/"
        f"{task_id}/status"
    )

    try:

        response = requests.get(
            url,
            auth=(username, password),
            timeout=30,
        )

        if response.status_code == 200:

            data = response.json()

            return (
                data
                .get("status", {})
                .get("status")
            )

        if response.status_code == 404:

            return "NOT_FOUND"

        logger.warning(
            "Task status failed: %s %s",
            response.status_code,
            response.text,
        )

        return "UNKNOWN"

    except requests.RequestException as exc:

        logger.warning(
            "Druid status request error: %s",
            exc,
        )

        return "UNKNOWN"


# ============================================================
# GET DRUID TASK LIST
# ============================================================

def get_task_list(
    endpoint: str,
    druid_url: str,
    username: str,
    password: str,
) -> Optional[List[Dict[str, Any]]]:

    url = (
        f"{druid_url}"
        f"/druid/indexer/v1/{endpoint}"
    )

    try:

        response = requests.get(
            url,
            auth=(username, password),
            timeout=30,
        )

        response.raise_for_status()

        return response.json()

    except requests.RequestException as exc:

        logger.warning(
            "%s request failed: %s",
            endpoint,
            exc,
        )

        return None


# ============================================================
# FIND ACTIVE INGESTION
# ============================================================

def find_active_ingestion(
    datasource_name: str,
    druid_url: str,
    username: str,
    password: str,
) -> Optional[Dict[str, Any]]:
    """
    runningTasks
    pendingTasks
    waitingTasks

    içerisinden datasource + type=index_parallel
    olan task'ı bulur.
    """

    for endpoint in (
        "runningTasks",
        "pendingTasks",
        "waitingTasks",
    ):

        tasks = get_task_list(
            endpoint=endpoint,
            druid_url=druid_url,
            username=username,
            password=password,
        )

        if tasks is None:
            return None

        for task in tasks:

            if (
                task.get("dataSource")
                == datasource_name
                and task.get("type")
                == "index_parallel"
            ):

                return task

    return False


# ============================================================
# LOAD SPEC
# ============================================================

def load_ingestion_spec(
    parquet_files: List[str],
) -> Dict[str, Any]:
    """
    ingestion_spec.json okunur.

    ORİJİNAL DOSYA DEĞİŞTİRİLMEZ.

    Sadece memory'deki copy üzerinde:

        spec.ioConfig.inputSource.uris

    değiştirilir.
    """

    if not SPEC_FILE.exists():

        raise FileNotFoundError(
            f"Ingestion spec not found: "
            f"{SPEC_FILE}"
        )

    with open(
        SPEC_FILE,
        "r",
        encoding="utf-8",
    ) as f:

        spec = json.load(f)

    # --------------------------------------------------------
    # SADECE URIS DEĞİŞTİR
    # --------------------------------------------------------

    spec[
        "spec"
    ][
        "ioConfig"
    ][
        "inputSource"
    ][
        "uris"
    ] = list(parquet_files)

    return spec


# ============================================================
# SUBMIT
# ============================================================

def submit_task(
    parquet_files: List[str],
    druid_url: str,
    username: str,
    password: str,
) -> str:

    ingestion_spec = load_ingestion_spec(
        parquet_files=parquet_files,
    )

    url = (
        f"{druid_url}"
        f"/druid/indexer/v1/task"
    )

    logger.info(
        "Submitting Druid ingestion task"
    )

    response = requests.post(
        url,
        auth=(username, password),
        headers={
            "Content-Type": "application/json"
        },
        json=ingestion_spec,
        timeout=60,
    )

    logger.info(
        "Druid submit response: %s %s",
        response.status_code,
        response.text,
    )

    response.raise_for_status()

    task_id = response.json()["task"]

    logger.info(
        "Druid task created: %s",
        task_id,
    )

    return task_id


# ============================================================
# MONITOR EXISTING / OUR TASK
# ============================================================

def monitor_task(
    task_id: str,
    ingestion_key: str,
    datasource_name: str,
    parquet_files: List[str],
    state_object: str,
    retry_count: int,
    druid_url: str,
    username: str,
    password: str,
) -> str:

    logger.info(
        "Monitoring Druid task: %s",
        task_id,
    )

    while True:

        status = get_task_status(
            task_id=task_id,
            druid_url=druid_url,
            username=username,
            password=password,
        )

        logger.info(
            "Task %s -> %s",
            task_id,
            status,
        )

        # ====================================================
        # ACTIVE
        # ====================================================

        if status in (
            "RUNNING",
            "PENDING",
            "WAITING",
        ):

            time.sleep(
                POLL_INTERVAL
            )

            continue

        # ====================================================
        # SUCCESS
        # ====================================================

        if status == "SUCCESS":

            write_state(
                object_name=state_object,
                ingestion_key=ingestion_key,
                datasource_name=datasource_name,
                parquet_files=parquet_files,
                task_id=task_id,
                status="SUCCESS",
                retry_count=retry_count,
            )

            return "SUCCESS"

        # ====================================================
        # FAILED
        # ====================================================

        if status == "FAILED":

            if retry_count >= MAX_RETRIES:

                write_state(
                    object_name=state_object,
                    ingestion_key=ingestion_key,
                    datasource_name=datasource_name,
                    parquet_files=parquet_files,
                    task_id=task_id,
                    status="FAILED",
                    retry_count=retry_count,
                )

                return "FAILED"

            new_retry_count = (
                retry_count + 1
            )

            logger.warning(
                "Druid task failed. "
                "Retry %s/%s",
                new_retry_count,
                MAX_RETRIES,
            )

            new_task_id = submit_task(
                parquet_files=parquet_files,
                druid_url=druid_url,
                username=username,
                password=password,
            )

            write_state(
                object_name=state_object,
                ingestion_key=ingestion_key,
                datasource_name=datasource_name,
                parquet_files=parquet_files,
                task_id=new_task_id,
                status="RUNNING",
                retry_count=new_retry_count,
            )

            task_id = new_task_id
            retry_count = new_retry_count

            continue

        # ====================================================
        # NOT FOUND
        # ====================================================

        if status == "NOT_FOUND":

            logger.warning(
                "Task %s was not found in Druid.",
                task_id,
            )

            # Güvenli davranıyoruz.
            # Burada otomatik submit etmiyoruz.
            return "NOT_FOUND"

        # ====================================================
        # UNKNOWN
        # ====================================================

        if status == "UNKNOWN":

            logger.warning(
                "Druid task status is currently unknown."
            )

            time.sleep(
                POLL_INTERVAL
            )

            continue

        # ====================================================
        # OTHER
        # ====================================================

        logger.warning(
            "Unexpected task status: %s",
            status,
        )

        return status


# ============================================================
# MAIN
# ============================================================

def run_ingestion(
    parquet_files: List[str],
    druid_url: str,
    username: str,
    password: str,
) -> Dict[str, Any]:

    if not parquet_files:

        raise ValueError(
            "parquet_files cannot be empty"
        )

    # ========================================================
    # DATASOURCE
    # ========================================================
    #
    # Burada datasource'u parametre olarak almıyoruz.
    #
    # ingestion_spec.json içindeki datasource kullanılıyor.
    #
    # Böylece spec'in datasource tanımı korunuyor.
    # ========================================================

    with open(
        SPEC_FILE,
        "r",
        encoding="utf-8",
    ) as f:

        original_spec = json.load(f)

    datasource_name = (
        original_spec
        ["spec"]
        ["dataSchema"]
        ["dataSource"]
    )

    # ========================================================
    # HASH
    # ========================================================

    ingestion_key = calculate_ingestion_key(
        datasource_name=datasource_name,
        parquet_files=parquet_files,
    )

    # ========================================================
    # STATE PATH
    # ========================================================

    state_object = get_state_object_name(
        datasource_name=datasource_name,
        parquet_files=parquet_files,
        ingestion_key=ingestion_key,
    )

    logger.info(
        "Datasource: %s",
        datasource_name,
    )

    logger.info(
        "Ingestion key: %s",
        ingestion_key,
    )

    logger.info(
        "State: gs://%s/%s",
        LOG_BUCKET,
        state_object,
    )

    # ========================================================
    # 1. CHECK OUR STATE
    # ========================================================

    state = read_state(
        state_object
    )

    if state:

        state_status = state.get(
            "status"
        )

        task_id = state.get(
            "task_id"
        )

        retry_count = state.get(
            "retry_count",
            0,
        )

        logger.info(
            "Existing state found: "
            "status=%s task_id=%s retry=%s",
            state_status,
            task_id,
            retry_count,
        )

        # ----------------------------------------------------
        # SUCCESS
        # ----------------------------------------------------

        if state_status == "SUCCESS":

            logger.info(
                "This ingestion already SUCCESS. "
                "No new task will be submitted."
            )

            return {
                "status": "SKIPPED",
                "reason": "ALREADY_SUCCESS",
                "ingestion_key": ingestion_key,
                "task_id": task_id,
            }

        # ----------------------------------------------------
        # RUNNING / PENDING / WAITING
        # ----------------------------------------------------

        if state_status in (
            "RUNNING",
            "PENDING",
            "WAITING",
        ):

            logger.info(
                "Existing ingestion is active. "
                "Monitoring task."
            )

            result = monitor_task(
                task_id=task_id,
                ingestion_key=ingestion_key,
                datasource_name=datasource_name,
                parquet_files=parquet_files,
                state_object=state_object,
                retry_count=retry_count,
                druid_url=druid_url,
                username=username,
                password=password,
            )

            return {
                "status": result,
                "ingestion_key": ingestion_key,
                "task_id": task_id,
            }

        # ----------------------------------------------------
        # FAILED
        # ----------------------------------------------------

        if state_status == "FAILED":

            logger.info(
                "Previous ingestion FAILED."
            )

            if retry_count >= MAX_RETRIES:

                logger.error(
                    "Maximum retry count reached."
                )

                return {
                    "status": "FAILED",
                    "reason": "MAX_RETRIES",
                    "ingestion_key": ingestion_key,
                    "task_id": task_id,
                }

            new_retry_count = (
                retry_count + 1
            )

            new_task_id = submit_task(
                parquet_files=parquet_files,
                druid_url=druid_url,
                username=username,
                password=password,
            )

            write_state(
                object_name=state_object,
                ingestion_key=ingestion_key,
                datasource_name=datasource_name,
                parquet_files=parquet_files,
                task_id=new_task_id,
                status="RUNNING",
                retry_count=new_retry_count,
            )

            result = monitor_task(
                task_id=new_task_id,
                ingestion_key=ingestion_key,
                datasource_name=datasource_name,
                parquet_files=parquet_files,
                state_object=state_object,
                retry_count=new_retry_count,
                druid_url=druid_url,
                username=username,
                password=password,
            )

            return {
                "status": result,
                "ingestion_key": ingestion_key,
                "task_id": new_task_id,
            }

        # ----------------------------------------------------
        # OTHER STATE
        # ----------------------------------------------------

        logger.info(
            "State is '%s'. "
            "No new ingestion will be submitted.",
            state_status,
        )

        return {
            "status": "SKIPPED",
            "reason": f"STATE_{state_status}",
            "ingestion_key": ingestion_key,
            "task_id": task_id,
        }

    # ========================================================
    # 2. NO STATE
    # ========================================================

    logger.info(
        "No previous state found."
    )

    # ========================================================
    # 3. CHECK DRUID ACTIVE TASKS
    # ========================================================

    active_task = find_active_ingestion(
        datasource_name=datasource_name,
        druid_url=druid_url,
        username=username,
        password=password,
    )

    if active_task is None:

        raise RuntimeError(
            "Unable to check Druid active tasks."
        )

    # ========================================================
    # 4. ACTIVE TASK EXISTS
    # ========================================================

    if active_task:

        active_task_id = active_task.get(
            "id"
        )

        logger.info(
            "Found active Druid ingestion: %s",
            active_task_id,
        )

        logger.info(
            "Existing task will be monitored."
        )

        # ----------------------------------------------------
        # Mevcut task'ı bizim state'imize bağla.
        # ----------------------------------------------------

        write_state(
            object_name=state_object,
            ingestion_key=ingestion_key,
            datasource_name=datasource_name,
            parquet_files=parquet_files,
            task_id=active_task_id,
            status="RUNNING",
            retry_count=0,
        )

        result = monitor_task(
            task_id=active_task_id,
            ingestion_key=ingestion_key,
            datasource_name=datasource_name,
            parquet_files=parquet_files,
            state_object=state_object,
            retry_count=0,
            druid_url=druid_url,
            username=username,
            password=password,
        )

        return {
            "status": result,
            "ingestion_key": ingestion_key,
            "task_id": active_task_id,
        }

    # ========================================================
    # 5. NO ACTIVE TASK -> SUBMIT
    # ========================================================

    logger.info(
        "No active Druid ingestion found."
    )

    task_id = submit_task(
        parquet_files=parquet_files,
        druid_url=druid_url,
        username=username,
        password=password,
    )

    # --------------------------------------------------------
    # İlk state
    # --------------------------------------------------------

    write_state(
        object_name=state_object,
        ingestion_key=ingestion_key,
        datasource_name=datasource_name,
        parquet_files=parquet_files,
        task_id=task_id,
        status="RUNNING",
        retry_count=0,
    )

    # ========================================================
    # 6. MONITOR
    # ========================================================

    result = monitor_task(
        task_id=task_id,
        ingestion_key=ingestion_key,
        datasource_name=datasource_name,
        parquet_files=parquet_files,
        state_object=state_object,
        retry_count=0,
        druid_url=druid_url,
        username=username,
        password=password,
    )

    return {
        "status": result,
        "ingestion_key": ingestion_key,
        "task_id": task_id,
    }