import csv
import io
import logging
from typing import List, Optional

from config import GCS_BUCKET_NAME, GCS_CONN_ID, get_blocklist_candidates

logger = logging.getLogger(__name__)


def parse_csv_geohashes(
    csv_text: str,
    column_name: str = "geohash",
) -> List[str]:
    """
    Parses CSV text and returns a clean list of geohashes.
    
    Expected format:
        geohash
        u173zx0n
        u15xfy98
        ...
    """
    if not csv_text or not csv_text.strip():
        return []

    stream = io.StringIO(csv_text.strip())
    reader = csv.reader(stream)

    first_row = next(reader, None)
    if not first_row:
        return []

    # Determine if first row is a header
    target_idx = 0
    header_found = False
    for idx, col in enumerate(first_row):
        if col.strip().lower() == column_name.lower():
            target_idx = idx
            header_found = True
            break

    results: List[str] = []

    # If first row was not a header, process it as a data row
    if not header_found:
        first_val = first_row[0].strip()
        if first_val:
            results.append(first_val)

    for row in reader:
        if not row:
            continue
        if len(row) > target_idx:
            val = row[target_idx].strip()
            if val:
                results.append(val)

    # Deduplicate while preserving order
    seen = set()
    deduped = []
    for item in results:
        if item not in seen:
            seen.add(item)
            deduped.append(item)

    return deduped


def read_csv_from_gcs(
    bucket_name: str,
    object_name: str,
    conn_id: Optional[str] = None,
) -> str:
    """
    Downloads text content of a file from GCS.
    Uses Airflow's GCSHook when available, falls back to gcsfs if needed.
    """
    actual_conn_id = conn_id or GCS_CONN_ID

    # 1. Try Airflow GCSHook
    try:
        from airflow.providers.google.cloud.hooks.gcs import GCSHook

        hook = GCSHook(gcp_conn_id=actual_conn_id)
        content_bytes = hook.download(
            bucket_name=bucket_name,
            object_name=object_name,
        )
        return content_bytes.decode("utf-8")
    except Exception as hook_err:
        logger.debug(
            "Airflow GCSHook download failed for gs://%s/%s (%s), trying gcsfs...",
            bucket_name,
            object_name,
            hook_err,
        )

    # 2. Fallback to gcsfs
    try:
        import gcsfs

        fs = gcsfs.GCSFileSystem()
        with fs.open(f"gs://{bucket_name}/{object_name}", "r", encoding="utf-8") as f:
            return f.read()
    except Exception as gcsfs_err:
        logger.error(
            "Failed to read gs://%s/%s via gcsfs: %s",
            bucket_name,
            object_name,
            gcsfs_err,
        )
        raise FileNotFoundError(
            f"Could not read gs://{bucket_name}/{object_name}: {gcsfs_err}"
        ) from gcsfs_err


def read_csv(
    gcs_uri: str,
    column_name: str = "geohash",
    conn_id: Optional[str] = None,
) -> List[str]:
    """
    Reads a CSV from a full GCS URI (gs://bucket/path/to/file.csv)
    and returns a list of values for the given column.
    """
    clean_uri = gcs_uri.replace("gs://", "", 1)
    bucket_name, object_name = clean_uri.split("/", 1)

    csv_text = read_csv_from_gcs(
        bucket_name=bucket_name,
        object_name=object_name,
        conn_id=conn_id,
    )
    return parse_csv_geohashes(csv_text, column_name=column_name)


def read_blocklist_geohashes(
    country: str,
    date_str: str,
    bucket_name: Optional[str] = None,
    conn_id: Optional[str] = None,
) -> List[str]:
    """
    Locates and reads the ignore.csv blocklist for a given country and date.
    Checks date candidates (e.g. 2026/9/10 vs 2026/09/10).
    Returns a list of geohashes. If no blocklist file exists, returns [].
    """
    actual_bucket = bucket_name or GCS_BUCKET_NAME
    actual_conn_id = conn_id or GCS_CONN_ID

    candidates = get_blocklist_candidates(country=country, date_str=date_str)

    # Hook for checking existence
    hook = None
    try:
        from airflow.providers.google.cloud.hooks.gcs import GCSHook

        hook = GCSHook(gcp_conn_id=actual_conn_id)
    except Exception:
        hook = None

    for candidate_obj in candidates:
        logger.info(
            "Checking blocklist candidate: gs://%s/%s",
            actual_bucket,
            candidate_obj,
        )
        exists = False
        if hook:
            try:
                exists = hook.exists(
                    bucket_name=actual_bucket,
                    object_name=candidate_obj,
                )
            except Exception as e:
                logger.debug("hook.exists error for %s: %s", candidate_obj, e)

        if not exists:
            # Check via gcsfs fallback
            try:
                import gcsfs

                fs = gcsfs.GCSFileSystem()
                exists = fs.exists(f"gs://{actual_bucket}/{candidate_obj}")
            except Exception:
                pass

        if exists:
            logger.info(
                "Found blocklist file at gs://%s/%s",
                actual_bucket,
                candidate_obj,
            )
            csv_text = read_csv_from_gcs(
                bucket_name=actual_bucket,
                object_name=candidate_obj,
                conn_id=actual_conn_id,
            )
            geohashes = parse_csv_geohashes(csv_text, column_name="geohash")
            logger.info(
                "Loaded %d blocklisted geohash(es) for %s from %s",
                len(geohashes),
                country,
                candidate_obj,
            )
            return geohashes

    logger.info(
        "No ignore.csv blocklist found for %s (%s). Ingestion will proceed with empty blocklist.",
        country,
        date_str,
    )
    return []

