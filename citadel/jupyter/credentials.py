import logging
import os
from typing import Any

from airflow.hooks.base import BaseHook


DEFAULT_CONN_ID = "jupyterhub_default"


def _read_extra_value(extra: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = extra.get(key)
        if value not in (None, ""):
            return str(value)
    return ""


def get_jupyterhub_config(
    conn_id: str = DEFAULT_CONN_ID,
) -> dict[str, str]:
    """Load JupyterHub credentials and Cloudflare headers from Airflow."""
    config = {
        "base_url": "",
        "username": "",
        "token": "",
        "cf_access_client_id": "",
        "cf_access_client_secret": "",
    }
    try:
        conn = BaseHook.get_connection(conn_id)
    except Exception as exc:
        logging.debug(
            "JupyterHub connection %s unavailable; using environment fallback: %s",
            conn_id,
            exc,
        )
    else:
        extra = conn.extra_dejson or {}
        config["base_url"] = (conn.host or "").rstrip("/")
        config["username"] = conn.login or ""
        config["token"] = conn.password or ""
        config["cf_access_client_id"] = _read_extra_value(
            extra,
            "cf_access_client_id",
            "CF-Access-Client-Id",
            "cloudflare_client_id",
        )
        config["cf_access_client_secret"] = _read_extra_value(
            extra,
            "cf_access_client_secret",
            "CF-Access-Client-Secret",
            "cloudflare_client_secret",
        )

    config["base_url"] = (
        config["base_url"]
        or os.getenv("JUPYTERHUB_BASE_URL")
        or os.getenv("JUPYTER_URL")
        or ""
    ).rstrip("/")
    config["username"] = (
        config["username"]
        or os.getenv("JUPYTERHUB_USERNAME")
        or os.getenv("JUPYTER_USERNAME")
        or ""
    )
    config["token"] = (
        config["token"]
        or os.getenv("JUPYTERHUB_TOKEN")
        or os.getenv("JUPYTER_TOKEN")
        or ""
    )
    config["cf_access_client_id"] = (
        config["cf_access_client_id"]
        or os.getenv("CF_ACCESS_CLIENT_ID")
        or os.getenv("JUPYTERHUB_CF_ACCESS_CLIENT_ID")
        or ""
    )
    config["cf_access_client_secret"] = (
        config["cf_access_client_secret"]
        or os.getenv("CF_ACCESS_CLIENT_SECRET")
        or os.getenv("JUPYTERHUB_CF_ACCESS_CLIENT_SECRET")
        or ""
    )

    for field, label in (
        ("base_url", "base URL"),
        ("username", "username"),
        ("token", "token"),
    ):
        if not config[field]:
            raise ValueError(f"JupyterHub {label} is not configured")
    return config
