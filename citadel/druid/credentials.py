"""
Druid credentials management module.

This module provides functionality to load and validate Druid connection credentials
from Airflow.

Usage:
    url, username, password = get_druid_credentials(conn_id="my_druid_conn")

Returns:
    tuple[str, str, str]: A tuple containing the Druid URL, username, and password.
"""
from airflow.hooks.base import BaseHook
from citadel.config_loader import config


DEFAULT_CONN_ID = config.druid.get('conn_id', 'druid_default')


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
