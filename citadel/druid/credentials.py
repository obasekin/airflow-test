from airflow.hooks.base import BaseHook


DEFAULT_CONN_ID = "druid_default"


def get_druid_credentials(
    conn_id: str | None = None,
) -> tuple[str, str, str]:
    """Load and validate the Druid URL and credentials from Airflow."""
    conn = BaseHook.get_connection(conn_id or DEFAULT_CONN_ID)
    druid_url = (conn.host or "").rstrip("/")
    username = conn.login or ""
    password = conn.password or ""

    if not druid_url:
        raise ValueError("Druid URL is not configured")
    if not username:
        raise ValueError("Druid username is not configured")
    if not password:
        raise ValueError("Druid password is not configured")

    return druid_url, username, password
