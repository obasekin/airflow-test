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

# Druid status kontrol aralığı
# 1 dakika
POLL_INTERVAL = 60

# Failed task retry sayısı
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
    Örnek:

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

    Her polling'de state yazılmaz.
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
# FIND ANY ACTIVE INDEX PARALLEL
# ============================================================

def find_active_ingestion(
    druid_url: str,
    username: str,
    password: str,
) -> Optional[Dict[str, Any]]:
    """
    Druid'de herhangi bir aktif index_parallel
    ingestion task'ı var mı kontrol eder.

    Datasource önemli değildir.

    Örnek:

        TUR       index_parallel
        NLD       index_parallel
        NLDtest   index_parallel

    herhangi biri aktifse yeni ingestion bekler.

    compact ve diğer task tipleri dikkate alınmaz.

    Return:

        Dict  -> aktif ingestion bulundu
        False -> aktif ingestion yok
        None  -> Druid kontrolü başarısız
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

            if task.get("type") != "index_parallel":
                continue

            logger.info(
                "Active Druid ingestion found: "
                "id=%s datasource=%s status=%s",
                task.get("id"),
                task.get("dataSource"),
                task.get("status"),
            )

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

    Original dosya değiştirilmez.

    Sadece memory'deki spec'in:

        spec.ioConfig.inputSource.uris

    alanı değiştirilir.
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
# WAIT FOR GLOBAL DRUID INGESTION SLOT
# ============================================================

def wait_for_druid_ingestion_slot(
    druid_url: str,
    username: str,
    password: str,
):
    """
    Druid'de herhangi bir index_parallel çalışıyorsa
    yeni ingestion başlatılmaz.

    Her 1 dakikada bir tekrar kontrol edilir.

    Örnek:

        10:00 -> NLD RUNNING
        10:01 -> NLD RUNNING
        10:02 -> NLD RUNNING
        ...
        11:17 -> NLD SUCCESS
        11:18 -> yeni ingestion submit

    Yani sadece 1 kere 1 dakika beklemiyor.

    Aktif ingestion bitene kadar sürekli bekliyor.
    """

    while True:

        active_task = find_active_ingestion(
            druid_url=druid_url,
            username=username,
            password=password,
        )

        # ----------------------------------------------------
        # Druid kontrolü başarısız
        # ----------------------------------------------------

        if active_task is None:

            logger.warning(
                "Unable to determine active Druid tasks. "
                "Retrying in %s seconds.",
                POLL_INTERVAL,
            )

            time.sleep(
                POLL_INTERVAL
            )

            continue

        # ----------------------------------------------------
        # SLOT BOŞ
        # ----------------------------------------------------

        if not active_task:

            logger.info(
                "No active Druid ingestion found."
            )

            return

        # ----------------------------------------------------
        # SLOT DOLU
        # ----------------------------------------------------

        logger.info(
            "Druid ingestion slot is busy. "
            "Task=%s datasource=%s. "
            "Next check in %s seconds.",
            active_task.get("id"),
            active_task.get("dataSource"),
            POLL_INTERVAL,
        )

        time.sleep(
            POLL_INTERVAL
        )


# ============================================================
# MONITOR OUR TASK
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
    """
    Bizim oluşturduğumuz Druid task'ı takip eder.

    Her 1 dakikada bir status kontrol edilir.

    Timeout burada yoktur.

    Airflow task timeout yönetimini Airflow yapar.
    """

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

            logger.info(
                "Task %s is still active. "
                "Next check in %s seconds.",
                task_id,
                POLL_INTERVAL,
            )

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

            logger.info(
                "Druid task %s completed successfully.",
                task_id,
            )

            return "SUCCESS"

        # ====================================================
        # FAILED
        # ====================================================

        if status == "FAILED":

            logger.warning(
                "Druid task %s FAILED.",
                task_id,
            )

            # ------------------------------------------------
            # MAX RETRIES
            # ------------------------------------------------

            if retry_count >= MAX_RETRIES:

                logger.error(
                    "Maximum retry count reached: %s/%s",
                    retry_count,
                    MAX_RETRIES,
                )

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

            # ------------------------------------------------
            # NEXT RETRY
            # ------------------------------------------------

            new_retry_count = (
                retry_count + 1
            )

            logger.warning(
                "Preparing retry %s/%s.",
                new_retry_count,
                MAX_RETRIES,
            )

            # ------------------------------------------------
            # GLOBAL DRUID SLOT
            # ------------------------------------------------
            #
            # Retry submit etmeden önce yine Druid'de
            # başka index_parallel var mı kontrol edilir.
            #
            # Varsa 1 dakika aralıklarla beklenir.
            # ------------------------------------------------

            wait_for_druid_ingestion_slot(
                druid_url=druid_url,
                username=username,
                password=password,
            )

            # ------------------------------------------------
            # SUBMIT RETRY
            # ------------------------------------------------

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

            logger.info(
                "Retry task created: %s",
                new_task_id,
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

            write_state(
                object_name=state_object,
                ingestion_key=ingestion_key,
                datasource_name=datasource_name,
                parquet_files=parquet_files,
                task_id=task_id,
                status="NOT_FOUND",
                retry_count=retry_count,
            )

            return "NOT_FOUND"

        # ====================================================
        # UNKNOWN
        # ====================================================

        if status == "UNKNOWN":

            logger.warning(
                "Druid task status is currently unknown. "
                "Retrying in %s seconds.",
                POLL_INTERVAL,
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
    # INGESTION KEY
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
        # ACTIVE
        # ----------------------------------------------------

        if state_status in (
            "RUNNING",
            "PENDING",
            "WAITING",
        ):

            logger.info(
                "Existing ingestion is active. "
                "Monitoring task %s.",
                task_id,
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

            # ------------------------------------------------
            # GLOBAL SLOT
            # ------------------------------------------------

            wait_for_druid_ingestion_slot(
                druid_url=druid_url,
                username=username,
                password=password,
            )

            # ------------------------------------------------
            # SUBMIT RETRY
            # ------------------------------------------------

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
        # NOT FOUND
        # ----------------------------------------------------

        if state_status == "NOT_FOUND":

            raise RuntimeError(
                f"Previous Druid task {task_id} "
                f"was not found."
            )

        # ----------------------------------------------------
        # OTHER
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
    # 3. WAIT FOR GLOBAL DRUID SLOT
    # ========================================================
    #
    # Burada datasource kontrol edilmiyor.
    #
    # Herhangi bir index_parallel varsa bekle.
    #
    # 1 dakika -> tekrar kontrol
    #
    # Bu işlem Druid'deki mevcut task bitene kadar
    # devam eder.
    # ========================================================

    wait_for_druid_ingestion_slot(
        druid_url=druid_url,
        username=username,
        password=password,
    )

    # ========================================================
    # 4. SUBMIT OUR INGESTION
    # ========================================================

    task_id = submit_task(
        parquet_files=parquet_files,
        druid_url=druid_url,
        username=username,
        password=password,
    )

    # ========================================================
    # 5. INITIAL STATE
    # ========================================================

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
    # 6. MONITOR OUR TASK
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