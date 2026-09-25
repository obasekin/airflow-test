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

    @staticmethod
    def validate_consistency(
        country: str,
        ingestion_spec_path: str,
        parquet_files: List[str],
        spec: Dict[str, Any],
    ) -> None:
        """
        Validates 4-way consistency across:
        1. country (from daily flow / DAG request)
        2. ingestion spec folder path (scripts/<country>/<country>_druid_ingestion/ingestion_spec.json)
        3. input data source (parquet file GCS URIs must contain /<country>/)
        4. output data destination (Druid dataSource in spec must equal country)

        Raises:
            ValueError: If any mismatch is detected.
        """
        if not country:
            raise ValueError("4-way consistency check failed: 'country' parameter is required.")

        country_clean = country.strip().upper()

        # 1. Scripts folder and Ingestion folder validation
        spec_path = Path(ingestion_spec_path)
        ingestion_folder = spec_path.parent.name.upper()
        country_folder = spec_path.parent.parent.name.upper()
        expected_ingestion_folder = f"{country_clean}_DRUID_INGESTION"

        if country_folder != country_clean:
            raise ValueError(
                f"Ingestion spec country folder mismatch! "
                f"Daily flow country is '{country}', but spec resides in directory '{spec_path.parent.parent.name}'. "
                f"Expected directory structure: .../scripts/{country}/"
            )

        if ingestion_folder != expected_ingestion_folder:
            raise ValueError(
                f"Ingestion spec folder name mismatch! "
                f"Daily flow country is '{country}', but spec folder is '{spec_path.parent.name}'. "
                f"Expected folder: {country}_druid_ingestion/"
            )

        # 2. Druid dataSource (data destination) validation
        try:
            datasource_name = spec["spec"]["dataSchema"]["dataSource"]
        except (KeyError, TypeError) as e:
            raise ValueError(f"Malformed ingestion spec: missing spec.dataSchema.dataSource ({e})")

        if datasource_name.strip().upper() != country_clean:
            raise ValueError(
                f"Druid target dataSource mismatch! "
                f"Daily flow country is '{country}', but spec target dataSource is '{datasource_name}'. "
                f"Data cannot be ingested into an incorrect dataSource table!"
            )

        # 3. Parquet files (data origin) validation
        if not parquet_files:
            raise ValueError("Input data mismatch: parquet_files list cannot be empty.")

        expected_segment = f"/{country_clean}/"
        for f in parquet_files:
            if expected_segment not in f.upper():
                raise ValueError(
                    f"Input data origin mismatch! "
                    f"Daily flow country is '{country}', but parquet file does not contain '{expected_segment}': {f}. "
                    f"Preventing cross-country data contamination."
                )

        logger.info(
            "4-way ingestion consistency verified successfully: "
            "country=%s, spec_folder=%s, dataSource=%s, file_count=%d",
            country,
            spec_path.parent.name,
            datasource_name,
            len(parquet_files),
        )

    def load(
        self,
        parquet_files: List[str],
        ingestion_spec_path: str,
        blocklist_geohashes: Optional[List[str]] = None,
        country: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Loads the JSON ingestion spec from the given path and injects the specified
        parquet files and blocklist geohashes into the specification.

        Args:
            parquet_files: List of GCS URIs for the parquet files to ingest.
            ingestion_spec_path: Path to the base JSON ingestion specification.
            blocklist_geohashes: Optional list of geohashes to filter out.
            country: Optional country string for 4-way consistency validation.

        Returns:
            The loaded and modified ingestion specification as a dictionary.

        Raises:
            FileNotFoundError: If the given ingestion_spec_path does not exist.
            ValueError: If 4-way consistency check fails.
        """
        spec_path = Path(ingestion_spec_path)
        if not spec_path.exists():
            raise FileNotFoundError(f"Ingestion spec not found: {spec_path}")

        with spec_path.open("r", encoding="utf-8") as f:
            spec = json.load(f)

        # 4-way consistency validation
        if country:
            self.validate_consistency(
                country=country,
                ingestion_spec_path=ingestion_spec_path,
                parquet_files=parquet_files,
                spec=spec,
            )

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
