import os
from pathlib import Path
from typing import List


def _load_env_file() -> None:
    """
    Search and load environment variables from .env or config.env.
    Checks config/ directory first, then the root directory.
    Uses python-dotenv if available, otherwise falls back to a custom parser.
    """
    config_dir = Path(__file__).resolve().parent
    project_root = config_dir.parent

    candidate_files = [
        config_dir / "config.env",
        config_dir / ".env",
        project_root / "config.env",
        project_root / ".env",
    ]

    for env_file in candidate_files:
        if env_file.is_file():
            try:
                from dotenv import load_dotenv
                load_dotenv(env_file, override=False)
            except ImportError:
                # Built-in fallback parser without external dependencies
                with open(env_file, mode="r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        if "=" in line:
                            key, val = line.split("=", 1)
                            key = key.strip()
                            val = val.strip().strip("'\"")
                            os.environ.setdefault(key, val)
            break


_load_env_file()

# ============================================================
# AIRFLOW & SYSTEM SETTINGS
# ============================================================
AIRFLOW_HOME: str = os.getenv("AIRFLOW_HOME", "/opt/airflow")
TIMEZONE: str = os.getenv("AIRFLOW_TIMEZONE", "Europe/Istanbul")

# ============================================================
# GOOGLE CLOUD STORAGE (GCS) SETTINGS
# ============================================================
GCS_CONN_ID: str = os.getenv("GCS_CONN_ID", "google_cloud_default")
GCS_BUCKET_NAME: str = os.getenv("GCS_BUCKET_NAME", "arcanor-orion")
GCS_MOBILITY_BASE_PATH: str = os.getenv("GCS_MOBILITY_BASE_PATH", "output/mobility")
GCS_BLOCKLIST_BASE_PATH: str = os.getenv(
    "GCS_BLOCKLIST_BASE_PATH",
    "control/druid/ingestion/blocklist/geohash",
)
BLOCKLIST_FILENAME: str = os.getenv("BLOCKLIST_FILENAME", "ignore.csv")

# ============================================================
# LOGGING & DRUID SETTINGS
# ============================================================
LOG_BUCKET: str = os.getenv("LOG_BUCKET", "arcanor-airflow-logs")
LOG_PREFIX: str = os.getenv("LOG_PREFIX", "logs/ingestion")
DRUID_CONN_ID: str = os.getenv("DRUID_CONN_ID", "druid_default")
POLL_INTERVAL: int = int(os.getenv("DRUID_POLL_INTERVAL", "60"))
MAX_RETRIES: int = int(os.getenv("DRUID_MAX_RETRIES", "3"))

# ============================================================
# JUPYTERHUB SETTINGS
# ============================================================
JUPYTERHUB_CONN_ID: str = os.getenv("JUPYTERHUB_CONN_ID", "jupyterhub_default")

# ============================================================
# NOTIFICATION & EMAIL SETTINGS
# ============================================================
_raw_emails = os.getenv(
    "NOTIFICATION_EMAILS",
    "obasekin@arcanor.com, ucelik@arcanor.com",
)
NOTIFICATION_EMAILS: List[str] = [
    email.strip() for email in _raw_emails.split(",") if email.strip()
]
SMTP_CONN_ID: str = os.getenv("SMTP_CONN_ID", "smtp_default")


# ============================================================
# PATH & PREFIX RESOLUTION HELPERS
# ============================================================
def get_country_base_path(country: str) -> str:
    """
    Returns the GCS base folder prefix for a given country.
    E.g. 'output/mobility/BEL'
    """
    return f"{GCS_MOBILITY_BASE_PATH.rstrip('/')}/{country.upper()}"


def get_ingestion_spec_path(country: str) -> Path:
    """
    Locates the ingestion_spec.json for a given country.
    First checks the GitSync/Kubernetes path under AIRFLOW_HOME.
    Falls back to the local repository path if running in a local environment.
    """
    country_upper = country.upper()
    gitsync_path = (
        Path(AIRFLOW_HOME)
        / "dags"
        / "repo"
        / "scripts"
        / country_upper
        / f"{country_upper}_druid_ingestion"
        / "ingestion_spec.json"
    )
    if gitsync_path.is_file():
        return gitsync_path

    # Local development / direct repo fallback
    local_path = (
        Path(__file__).resolve().parent.parent
        / "scripts"
        / country_upper
        / f"{country_upper}_druid_ingestion"
        / "ingestion_spec.json"
    )
    if local_path.is_file():
        return local_path

    # Return default gitsync path if neither directly exists
    return gitsync_path


def get_blocklist_candidates(country: str, date_str: str) -> List[str]:
    """
    Returns candidate GCS object paths for the blocklist CSV.
    Supports both single-digit (e.g. 2026/9/10) and two-digit (e.g. 2026/09/10)
    month and day date formats.
    
    Example candidates returned:
    - control/druid/ingestion/blocklist/geohash/TUR/2026/9/10/ignore.csv
    - control/druid/ingestion/blocklist/geohash/TUR/2026/09/10/ignore.csv
    """
    country_upper = country.upper()
    prefix = GCS_BLOCKLIST_BASE_PATH.strip("/")
    filename = BLOCKLIST_FILENAME.strip("/")

    # Normalize date_str (can be '2026-09-10' or '2026/09/10' or '2026/9/10')
    cleaned = date_str.replace("-", "/").strip("/")
    parts = cleaned.split("/")

    # Find the year, month, day in the path
    year, month, day = None, None, None
    for i in range(len(parts) - 2):
        y, m, d = parts[i], parts[i + 1], parts[i + 2]
        if len(y) == 4 and y.isdigit() and m.isdigit() and d.isdigit():
            year, month, day = int(y), int(m), int(d)
            break

    if year is None:
        # Fallback if cannot extract 3 numbers
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

