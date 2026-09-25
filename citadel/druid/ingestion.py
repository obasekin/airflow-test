"""
Facade service for managing Druid data ingestion tasks.

This module provides the DruidIngestionService class which handles the complete lifecycle
of a Druid ingestion task: managing state, resolving blocklists, and polling for task completion.

Example usage:
    service = DruidIngestionService(conn_id="druid_default", poll_interval=30)
    result = service.run(
        parquet_files=["gs://bucket/file.parquet"],
        ingestion_spec_path="/path/to/spec.json"
    )

Alternatively, use the convenience function:
    result = run_ingestion(
        parquet_files=["gs://bucket/file.parquet"],
        ingestion_spec_path="/path/to/spec.json",
        conn_id="druid_default"
    )
"""

import json
import logging
import time
from typing import Any, Dict, List, Optional

import requests

from citadel.config_loader import config
from citadel.druid.credentials import get_druid_credentials
from citadel.druid.state_manager import IngestionStateManager
from citadel.druid.spec_loader import IngestionSpecLoader

logger = logging.getLogger(__name__)

class DruidIngestionService:
    """
    Facade class managing the execution and monitoring of Druid ingestion tasks.
    """

    def __init__(
        self,
        conn_id: Optional[str] = None,
        poll_interval: Optional[int] = None,
        max_retries: Optional[int] = None,
        gcs_conn_id: Optional[str] = None,
        log_bucket: Optional[str] = None,
        log_prefix: Optional[str] = None,
    ):
        self.conn_id = conn_id
        self.poll_interval = poll_interval if poll_interval is not None else config.druid.get('poll_interval', 60)
        self.max_retries = max_retries if max_retries is not None else config.druid.get('max_retries', 3)
        self.gcs_conn_id = gcs_conn_id if gcs_conn_id is not None else config.gcs.get('conn_id', 'google_cloud_default')
        self.log_bucket = log_bucket if log_bucket is not None else config.logging.get('log_bucket', 'default-bucket')
        self.log_prefix = log_prefix if log_prefix is not None else config.logging.get('log_prefix', 'logs')

        self.state_manager = IngestionStateManager(
            gcs_conn_id=self.gcs_conn_id,
            log_bucket=self.log_bucket,
            log_prefix=self.log_prefix,
        )
        self.spec_loader = IngestionSpecLoader()

    def _get_task_status(
        self, task_id: str, druid_url: str, username: str, password: str
    ) -> str:
        url = f"{druid_url}/druid/indexer/v1/task/{task_id}/status"
        try:
            response = requests.get(url, auth=(username, password), timeout=30)
            if response.status_code == 200:
                data = response.json()
                return data.get("status", {}).get("status", "UNKNOWN")
            if response.status_code == 404:
                return "NOT_FOUND"
            logger.warning("Task status failed: %s %s", response.status_code, response.text)
            return "UNKNOWN"
        except requests.RequestException as exc:
            logger.warning("Druid status request error: %s", exc)
            return "UNKNOWN"

    def _get_task_list(
        self, endpoint: str, druid_url: str, username: str, password: str
    ) -> Optional[List[Dict[str, Any]]]:
        url = f"{druid_url}/druid/indexer/v1/{endpoint}"
        try:
            response = requests.get(url, auth=(username, password), timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            logger.warning("%s request failed: %s", endpoint, exc)
            return None

    def _find_active_ingestion(
        self, druid_url: str, username: str, password: str
    ) -> Optional[Dict[str, Any]]:
        for endpoint in ("runningTasks", "pendingTasks", "waitingTasks"):
            tasks = self._get_task_list(endpoint, druid_url, username, password)
            if tasks is None:
                return None
            for task in tasks:
                if task.get("type") != "index_parallel":
                    continue
                logger.info(
                    "Active Druid ingestion found: id=%s datasource=%s status=%s",
                    task.get("id"),
                    task.get("dataSource"),
                    task.get("status"),
                )
                return task
        return False

    def _wait_for_druid_ingestion_slot(
        self, druid_url: str, username: str, password: str
    ):
        while True:
            active_task = self._find_active_ingestion(druid_url, username, password)
            if active_task is None:
                logger.warning(
                    "Unable to determine active Druid tasks. Retrying in %s seconds.",
                    self.poll_interval,
                )
                time.sleep(self.poll_interval)
                continue
            if not active_task:
                logger.info("No active Druid ingestion found.")
                return
            logger.info(
                "Druid ingestion slot is busy. Task=%s datasource=%s. Next check in %s seconds.",
                active_task.get("id"),
                active_task.get("dataSource"),
                self.poll_interval,
            )
            time.sleep(self.poll_interval)

    def _submit_task(
        self,
        parquet_files: List[str],
        druid_url: str,
        username: str,
        password: str,
        ingestion_spec_path: str,
        blocklist_geohashes: Optional[List[str]] = None,
        country: Optional[str] = None,
    ) -> str:
        ingestion_spec = self.spec_loader.load(
            parquet_files=parquet_files,
            ingestion_spec_path=ingestion_spec_path,
            blocklist_geohashes=blocklist_geohashes,
            country=country,
        )
        url = f"{druid_url}/druid/indexer/v1/task"
        logger.info("Submitting Druid ingestion task")
        response = requests.post(
            url,
            auth=(username, password),
            headers={"Content-Type": "application/json"},
            json=ingestion_spec,
            timeout=60,
        )
        logger.info("Druid submit response: %s %s", response.status_code, response.text)
        response.raise_for_status()
        task_id = response.json()["task"]
        logger.info("Druid task created: %s", task_id)
        return task_id

    def _monitor_task(
        self,
        task_id: str,
        ingestion_key: str,
        datasource_name: str,
        parquet_files: List[str],
        state_object: str,
        retry_count: int,
        druid_url: str,
        username: str,
        password: str,
        ingestion_spec_path: str,
    ) -> str:
        logger.info("Monitoring Druid task: %s", task_id)
        while True:
            status = self._get_task_status(task_id, druid_url, username, password)
            logger.info("Task %s -> %s", task_id, status)
            if status in ("RUNNING", "PENDING", "WAITING"):
                logger.info(
                    "Task %s is still active. Next check in %s seconds.",
                    task_id,
                    self.poll_interval,
                )
                time.sleep(self.poll_interval)
                continue
            if status == "SUCCESS":
                self.state_manager.write_state(
                    object_name=state_object,
                    ingestion_key=ingestion_key,
                    datasource_name=datasource_name,
                    parquet_files=parquet_files,
                    task_id=task_id,
                    status="SUCCESS",
                    retry_count=retry_count,
                )
                logger.info("Druid task %s completed successfully.", task_id)
                return "SUCCESS"
            if status == "FAILED":
                logger.warning("Druid task %s FAILED.", task_id)
                if retry_count >= self.max_retries:
                    logger.error(
                        "Maximum retry count reached: %s/%s", retry_count, self.max_retries
                    )
                    self.state_manager.write_state(
                        object_name=state_object,
                        ingestion_key=ingestion_key,
                        datasource_name=datasource_name,
                        parquet_files=parquet_files,
                        task_id=task_id,
                        status="FAILED",
                        retry_count=retry_count,
                    )
                    raise RuntimeError(
                        f"Druid ingestion task {task_id} FAILED after reaching max retries "
                        f"({retry_count}/{self.max_retries})."
                    )
                new_retry_count = retry_count + 1
                logger.warning(
                    "Preparing retry %s/%s.", new_retry_count, self.max_retries
                )
                self._wait_for_druid_ingestion_slot(druid_url, username, password)
                new_task_id = self._submit_task(
                    parquet_files=parquet_files,
                    druid_url=druid_url,
                    username=username,
                    password=password,
                    ingestion_spec_path=ingestion_spec_path,
                )
                self.state_manager.write_state(
                    object_name=state_object,
                    ingestion_key=ingestion_key,
                    datasource_name=datasource_name,
                    parquet_files=parquet_files,
                    task_id=new_task_id,
                    status="RUNNING",
                    retry_count=new_retry_count,
                )
                logger.info("Retry task created: %s", new_task_id)
                task_id = new_task_id
                retry_count = new_retry_count
                continue
            if status == "NOT_FOUND":
                logger.warning("Task %s was not found in Druid.", task_id)
                self.state_manager.write_state(
                    object_name=state_object,
                    ingestion_key=ingestion_key,
                    datasource_name=datasource_name,
                    parquet_files=parquet_files,
                    task_id=task_id,
                    status="NOT_FOUND",
                    retry_count=retry_count,
                )
                return "NOT_FOUND"
            if status == "UNKNOWN":
                logger.warning(
                    "Druid task status is currently unknown. Retrying in %s seconds.",
                    self.poll_interval,
                )
                time.sleep(self.poll_interval)
                continue
            logger.warning("Unexpected task status: %s", status)
            return status

    def run(
        self,
        parquet_files: List[str],
        ingestion_spec_path: str,
        blocklist_geohashes: Optional[List[str]] = None,
        country: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Executes the Druid ingestion workflow with 4-way consistency verification.
        """
        if not parquet_files:
            raise ValueError("parquet_files cannot be empty")
        if not ingestion_spec_path:
            raise ValueError("ingestion_spec_path cannot be empty")

        druid_url, username, password = get_druid_credentials(self.conn_id)

        with open(ingestion_spec_path, "r", encoding="utf-8") as f:
            original_spec = json.load(f)
        datasource_name = original_spec["spec"]["dataSchema"]["dataSource"]

        # 4-way consistency check (flow country, spec folder, input parquet paths, Druid dataSource)
        effective_country = country or datasource_name
        self.spec_loader.validate_consistency(
            country=effective_country,
            ingestion_spec_path=ingestion_spec_path,
            parquet_files=parquet_files,
            spec=original_spec,
        )

        if blocklist_geohashes is None:
            try:
                from citadel.utilities.read_csv import read_blocklist_geohashes
                date_path = self.state_manager.extract_date_from_parquet_files(parquet_files)
                blocklist_geohashes = read_blocklist_geohashes(
                    country=datasource_name,
                    date_str=date_path,
                    conn_id=self.gcs_conn_id,
                )
            except Exception as e:
                logger.warning(
                    "Could not auto-fetch blocklist geohashes for %s: %s", datasource_name, e
                )
                blocklist_geohashes = []

        ingestion_key = self.state_manager.calculate_ingestion_key(
            datasource_name=datasource_name, parquet_files=parquet_files
        )
        state_object = self.state_manager.get_state_object_name(
            datasource_name=datasource_name,
            parquet_files=parquet_files,
            ingestion_key=ingestion_key,
        )

        logger.info("Datasource: %s", datasource_name)
        logger.info("Ingestion key: %s", ingestion_key)
        logger.info("State: gs://%s/%s", self.log_bucket, state_object)

        state = self.state_manager.read_state(state_object)
        if state:
            state_status = state.get("status")
            task_id = state.get("task_id")
            retry_count = state.get("retry_count", 0)

            logger.info(
                "Existing state found: status=%s task_id=%s retry=%s",
                state_status,
                task_id,
                retry_count,
            )

            if state_status == "SUCCESS":
                logger.info("This ingestion already SUCCESS. No new task will be submitted.")
                return {
                    "status": "SKIPPED",
                    "reason": "ALREADY_SUCCESS",
                    "ingestion_key": ingestion_key,
                    "task_id": task_id,
                }
            if state_status in ("RUNNING", "PENDING", "WAITING"):
                logger.info("Existing ingestion is active. Monitoring task %s.", task_id)
                result = self._monitor_task(
                    task_id=task_id,
                    ingestion_key=ingestion_key,
                    datasource_name=datasource_name,
                    parquet_files=parquet_files,
                    state_object=state_object,
                    retry_count=retry_count,
                    druid_url=druid_url,
                    username=username,
                    password=password,
                    ingestion_spec_path=ingestion_spec_path,
                )
                return {"status": result, "ingestion_key": ingestion_key, "task_id": task_id}
            if state_status == "FAILED":
                logger.info("Previous ingestion FAILED.")
                if retry_count >= self.max_retries:
                    logger.error("Maximum retry count reached.")
                    raise RuntimeError(
                        f"Druid ingestion task {task_id} previously FAILED and max retries reached "
                        f"({retry_count}/{self.max_retries})."
                    )
                new_retry_count = retry_count + 1
                self._wait_for_druid_ingestion_slot(druid_url, username, password)
                new_task_id = self._submit_task(
                    parquet_files=parquet_files,
                    druid_url=druid_url,
                    username=username,
                    password=password,
                    ingestion_spec_path=ingestion_spec_path,
                    blocklist_geohashes=blocklist_geohashes,
                )
                self.state_manager.write_state(
                    object_name=state_object,
                    ingestion_key=ingestion_key,
                    datasource_name=datasource_name,
                    parquet_files=parquet_files,
                    task_id=new_task_id,
                    status="RUNNING",
                    retry_count=new_retry_count,
                )
                result = self._monitor_task(
                    task_id=new_task_id,
                    ingestion_key=ingestion_key,
                    datasource_name=datasource_name,
                    parquet_files=parquet_files,
                    state_object=state_object,
                    retry_count=new_retry_count,
                    druid_url=druid_url,
                    username=username,
                    password=password,
                    ingestion_spec_path=ingestion_spec_path,
                )
                return {"status": result, "ingestion_key": ingestion_key, "task_id": new_task_id}
            if state_status == "NOT_FOUND":
                raise RuntimeError(f"Previous Druid task {task_id} was not found.")
            
            logger.info("State is '%s'. No new ingestion will be submitted.", state_status)
            return {
                "status": "SKIPPED",
                "reason": f"STATE_{state_status}",
                "ingestion_key": ingestion_key,
                "task_id": task_id,
            }

        logger.info("No previous state found.")
        self._wait_for_druid_ingestion_slot(druid_url, username, password)
        task_id = self._submit_task(
            parquet_files=parquet_files,
            druid_url=druid_url,
            username=username,
            password=password,
            ingestion_spec_path=ingestion_spec_path,
            blocklist_geohashes=blocklist_geohashes,
            country=effective_country,
        )
        self.state_manager.write_state(
            object_name=state_object,
            ingestion_key=ingestion_key,
            datasource_name=datasource_name,
            parquet_files=parquet_files,
            task_id=task_id,
            status="RUNNING",
            retry_count=0,
        )
        result = self._monitor_task(
            task_id=task_id,
            ingestion_key=ingestion_key,
            datasource_name=datasource_name,
            parquet_files=parquet_files,
            state_object=state_object,
            retry_count=0,
            druid_url=druid_url,
            username=username,
            password=password,
            ingestion_spec_path=ingestion_spec_path,
        )
        return {"status": result, "ingestion_key": ingestion_key, "task_id": task_id}


def run_ingestion(
    parquet_files: List[str],
    ingestion_spec_path: str,
    blocklist_geohashes: Optional[List[str]] = None,
    conn_id: Optional[str] = None,
    country: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Backward-compatible wrapper. Use DruidIngestionService directly for more control.

    Args:
        parquet_files: List of GCS URIs for the parquet files to ingest.
        ingestion_spec_path: Path to the ingestion specification JSON.
        blocklist_geohashes: Optional list of geohashes to filter out.
        conn_id: Connection ID for Druid.
        country: Optional country string for 4-way consistency validation.

    Returns:
        A dictionary with the task execution status and metadata.
    """
    service = DruidIngestionService(conn_id=conn_id)
    return service.run(
        parquet_files=parquet_files,
        ingestion_spec_path=ingestion_spec_path,
        blocklist_geohashes=blocklist_geohashes,
        country=country,
    )