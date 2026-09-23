"""
Citadel Configuration Loader Module.

This module provides a singleton configuration object that loads settings
from the `citadel_config.yaml` file. It searches for the configuration file
in multiple standard locations to accommodate different deployment environments
(e.g., local development vs. Airflow runtime).

The module exposes service-specific configurations as properties (e.g., `config.druid`)
and provides helper functions for generating paths related to ingestion specs and blocklists.

Attributes:
    config: A singleton instance of `CitadelConfig` used to access configurations.
"""

import os
import yaml
from pathlib import Path


class CitadelConfig:
    """
    Singleton configuration class for Citadel services.
    
    This class loads the YAML configuration on initialization and provides
    access to service-specific settings via properties.
    """
    
    _instance = None
    
    def __new__(cls):
        """Implement singleton pattern to ensure only one config instance exists."""
        if cls._instance is None:
            cls._instance = super(CitadelConfig, cls).__new__(cls)
            cls._instance._load_config()
        return cls._instance
        
    def _load_config(self) -> None:
        """Finds and loads the citadel_config.yaml file from possible locations."""
        config_filename = "citadel_config.yaml"
        # Search paths: citadel/ dir, project root, AIRFLOW_HOME/dags/repo/citadel/
        search_paths = [
            Path(__file__).resolve().parent / config_filename,
            Path(__file__).resolve().parent.parent / config_filename,
        ]
        
        airflow_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
        search_paths.append(Path(airflow_home) / "dags" / "repo" / "citadel" / config_filename)
        
        self._config_data = {}
        for path in search_paths:
            if path.is_file():
                with open(path, "r", encoding="utf-8") as f:
                    self._config_data = yaml.safe_load(f) or {}
                break
                
    @property
    def druid(self) -> dict:
        """Druid service configuration."""
        return self._config_data.get("druid", {})
        
    @property
    def jupyter(self) -> dict:
        """Jupyter service configuration."""
        return self._config_data.get("jupyter", {})
        
    @property
    def notifications(self) -> dict:
        """Notifications service configuration."""
        return self._config_data.get("notifications", {})
        
    @property
    def gcs(self) -> dict:
        """GCS configuration."""
        return self._config_data.get("gcs", {})
        
    @property
    def logging(self) -> dict:
        """Logging configuration."""
        return self._config_data.get("logging", {})
        
    @property
    def general(self) -> dict:
        """General configuration."""
        return self._config_data.get("general", {})


# Singleton instance
config = CitadelConfig()


def get_country_base_path(country: str) -> str:
    """
    Get the GCS base path for a specific country.
    
    Args:
        country: The country code (e.g., 'TR', 'DE').
        
    Returns:
        The GCS path string for the country.
    """
    gcs_config = config.gcs
    base_path = gcs_config.get("mobility_base_path", "output/mobility")
    return f"{base_path.rstrip('/')}/{country.upper()}"


def get_ingestion_spec_path(country: str) -> Path:
    """
    Get the path to the Druid ingestion spec file for a specific country.
    
    Args:
        country: The country code (e.g., 'TR', 'DE').
        
    Returns:
        A Path object pointing to the ingestion spec file.
    """
    country_upper = country.upper()
    general_config = config.general
    airflow_home = general_config.get("airflow_home", "/opt/airflow")
    
    gitsync_path = Path(airflow_home) / "dags" / "repo" / "scripts" / country_upper / f"{country_upper}_druid_ingestion" / "ingestion_spec.json"
    if gitsync_path.is_file():
        return gitsync_path
        
    local_path = Path(__file__).resolve().parent.parent / "scripts" / country_upper / f"{country_upper}_druid_ingestion" / "ingestion_spec.json"
    if local_path.is_file():
        return local_path
        
    return gitsync_path


def get_blocklist_candidates(country: str, date_str: str) -> list[str]:
    """
    Returns candidate GCS object paths for the blocklist CSV.

    Supports both single-digit (e.g. 2026/9/10) and two-digit (e.g. 2026/09/10)
    month and day date formats.

    Args:
        country: The country code (e.g., 'TUR', 'BEL').
        date_str: The date string (e.g., '2026-09-10' or '2026/09/10').

    Returns:
        A deduplicated list of candidate GCS object paths.

    Example:
        >>> get_blocklist_candidates("TUR", "2026-09-10")
        ['control/druid/.../TUR/2026/9/10/ignore.csv',
         'control/druid/.../TUR/2026/09/10/ignore.csv']
    """
    gcs_config = config.gcs
    prefix = gcs_config.get(
        "blocklist_base_path",
        "control/druid/ingestion/blocklist/geohash",
    ).strip("/")
    filename = gcs_config.get("blocklist_filename", "ignore.csv").strip("/")
    country_upper = country.upper()

    # Normalize date_str
    cleaned = date_str.replace("-", "/").strip("/")
    parts = cleaned.split("/")

    year, month, day = None, None, None
    for i in range(len(parts) - 2):
        y, m, d = parts[i], parts[i + 1], parts[i + 2]
        if len(y) == 4 and y.isdigit() and m.isdigit() and d.isdigit():
            year, month, day = int(y), int(m), int(d)
            break

    if year is None:
        return [f"{prefix}/{country_upper}/{cleaned}/{filename}"]

    candidates = [
        f"{prefix}/{country_upper}/{year}/{month}/{day}/{filename}",
        f"{prefix}/{country_upper}/{year}/{month:02d}/{day:02d}/{filename}",
    ]
    # Deduplicate while preserving order
    seen = set()
    result = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            result.append(c)
    return result
