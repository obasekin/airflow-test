"""
Manages the state of Druid ingestion tasks.
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
import hashlib
from airflow.providers.google.cloud.hooks.gcs import GCSHook

logger = logging.getLogger(__name__)

class IngestionStateManager:
    """
    Manages state reads and writes for Druid ingestion tasks in Google Cloud Storage.

    Usage:
        manager = IngestionStateManager(
            gcs_conn_id="google_cloud_default",
            log_bucket="my-bucket",
            log_prefix="druid_logs"
        )
        state_path = manager.get_state_object_name("ds", ["file.parquet"], "key")
        manager.write_state(state_path, "key", "ds", ["file.parquet"], "task-1", "RUNNING", 0)
    """

    def __init__(
        self,
        gcs_conn_id: str = "google_cloud_default",
        log_bucket: str = "default_bucket",
        log_prefix: str = "default_prefix",
    ):
        self.gcs_conn_id = gcs_conn_id
        self.log_bucket = log_bucket
        self.log_prefix = log_prefix

    def _now(self) -> str:
        """Returns the current timestamp in string format."""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _get_gcs_hook(self) -> GCSHook:
        """Returns a GCSHook initialized with the configured connection ID."""
        return GCSHook(gcp_conn_id=self.gcs_conn_id)

    def calculate_ingestion_key(self, datasource_name: str, parquet_files: List[str]) -> str:
        """
        Calculates a unique ingestion key for a given datasource and set of parquet files.
        """
        sorted_files = sorted(parquet_files)
        payload = f"datasource={datasource_name}\nfiles=\n" + "\n".join(sorted_files)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @staticmethod
    def extract_date_from_parquet_files(parquet_files: List[str]) -> str:
        """
        Extracts the date path (YYYY/MM/DD) from a list of parquet file paths.
        """
        for parquet_file in parquet_files:
            path = parquet_file.replace("gs://", "", 1).split("/")
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
                    return f"{year}/{month}/{day}"
        raise ValueError("Could not extract date from parquet files")

    def get_state_object_name(self, datasource_name: str, parquet_files: List[str], ingestion_key: str) -> str:
        """
        Generates the GCS object name for the state file.
        """
        date_path = self.extract_date_from_parquet_files(parquet_files)
        return f"{self.log_prefix}/{datasource_name}/{date_path}/{ingestion_key}/state.json"

    def read_state(self, object_name: str) -> Optional[Dict[str, Any]]:
        """
        Reads the ingestion state from GCS.
        """
        hook = self._get_gcs_hook()
        if not hook.exists(bucket_name=self.log_bucket, object_name=object_name):
            return None
        data = hook.download(bucket_name=self.log_bucket, object_name=object_name)
        return json.loads(data.decode("utf-8"))

    def write_state(
        self,
        object_name: str,
        ingestion_key: str,
        datasource_name: str,
        parquet_files: List[str],
        task_id: str,
        status: str,
        retry_count: int,
    ) -> None:
        """
        Writes the ingestion state to GCS.
        """
        state = {
            "ingestion_key": ingestion_key,
            "datasource": datasource_name,
            "file_count": len(parquet_files),
            "files": sorted(parquet_files),
            "task_id": task_id,
            "status": status,
            "retry_count": retry_count,
            "created_at": self._now(),
        }
        hook = self._get_gcs_hook()
        hook.upload(
            bucket_name=self.log_bucket,
            object_name=object_name,
            data=json.dumps(state, indent=2, ensure_ascii=False),
            mime_type="application/json",
        )
        logger.info("State written: gs://%s/%s", self.log_bucket, object_name)
