"""
Handles loading and injecting configuration into Druid ingestion specifications.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

class IngestionSpecLoader:
    """
    Loads a Druid ingestion spec from a file and dynamically injects values like
    input URIs and blocklisted geohashes into the specification in memory.
    """

    def load(
        self,
        parquet_files: List[str],
        ingestion_spec_path: str,
        blocklist_geohashes: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Loads the JSON ingestion spec from the given path and injects the specified
        parquet files and blocklist geohashes into the specification.

        Args:
            parquet_files: List of GCS URIs for the parquet files to ingest.
            ingestion_spec_path: Path to the base JSON ingestion specification.
            blocklist_geohashes: Optional list of geohashes to filter out.

        Returns:
            The loaded and modified ingestion specification as a dictionary.

        Raises:
            FileNotFoundError: If the given ingestion_spec_path does not exist.
        """
        spec_path = Path(ingestion_spec_path)
        if not spec_path.exists():
            raise FileNotFoundError(f"Ingestion spec not found: {spec_path}")

        with spec_path.open("r", encoding="utf-8") as f:
            spec = json.load(f)

        # 1. Inject parquet files into uris
        spec["spec"]["ioConfig"]["inputSource"]["uris"] = list(parquet_files)

        # 2. Inject blocklist geohashes into filter values
        if blocklist_geohashes is not None:
            transform_spec = (
                spec.setdefault("spec", {})
                .setdefault("dataSchema", {})
                .setdefault("transformSpec", {})
            )
            filter_spec = transform_spec.setdefault(
                "filter",
                {
                    "type": "not",
                    "field": {
                        "type": "in",
                        "dimension": "geohash",
                        "values": [],
                    },
                },
            )
            if "field" in filter_spec and isinstance(filter_spec["field"], dict):
                filter_spec["field"]["values"] = list(blocklist_geohashes)
                logger.info(
                    "Injected %d blocklisted geohash(es) into Druid ingestion filter",
                    len(blocklist_geohashes),
                )

        return spec
