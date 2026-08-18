import json
import os
from typing import Any, Dict

import requests

try:
    from airflow.hooks.base import BaseHook
except ImportError:  # pragma: no cover
    BaseHook = None


DEFAULT_CONN_ID = "jupyterhub_default"


def _read_extra_value(extra: Dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = extra.get(key)
        if value not in (None, ""):
            return str(value)
    return ""


def get_jupyterhub_config(conn_id: str = DEFAULT_CONN_ID) -> Dict[str, str]:
    """
    Resolve JupyterHub settings from an Airflow connection or environment variables.

    Expected Airflow connection values:
    - Host: https://server.arcanor.com
    - Login: username (for example: obasekin)
    - Password: JupyterHub API token
    - Extra JSON:
      {"cf_access_client_id": "...", "cf_access_client_secret": "..."}
    """
    config: Dict[str, str] = {
        "base_url": "",
        "username": "",
        "token": "",
        "cf_access_client_id": "",
        "cf_access_client_secret": "",
    }

    if BaseHook is not None:
        try:
            conn = BaseHook.get_connection(conn_id)
            config["base_url"] = (conn.host or "").rstrip("/")
            config["username"] = conn.login or ""
            config["token"] = conn.password or ""

            extra = conn.extra_dejson or {}
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
        except Exception:
            pass

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

    if not config["base_url"]:
        raise ValueError("JupyterHub base URL is not configured")
    if not config["username"]:
        raise ValueError("JupyterHub username is not configured")
    if not config["token"]:
        raise ValueError("JupyterHub token is not configured")

    return config


def get_jupyterhub_headers(conn_id: str = DEFAULT_CONN_ID) -> Dict[str, str]:
    config = get_jupyterhub_config(conn_id=conn_id)
    headers = {
        "Accept": "application/json",
        "Authorization": f"token {config['token']}",
    }

    if config.get("cf_access_client_id") and config.get("cf_access_client_secret"):
        headers["CF-Access-Client-Id"] = config["cf_access_client_id"]
        headers["CF-Access-Client-Secret"] = config["cf_access_client_secret"]

    return headers


def build_jupyterhub_session(conn_id: str = DEFAULT_CONN_ID) -> requests.Session:
    session = requests.Session()
    session.headers.update(get_jupyterhub_headers(conn_id=conn_id))
    return session


def check_jupyterhub_connection(conn_id: str = DEFAULT_CONN_ID) -> Dict[str, Any]:
    config = get_jupyterhub_config(conn_id=conn_id)
    session = build_jupyterhub_session(conn_id=conn_id)
    url = f"{config['base_url']}/hub/api"

    response = session.get(url, timeout=60, allow_redirects=False)
    response.raise_for_status()

    try:
        payload = response.json()
    except ValueError:
        payload = {"raw_response": response.text[:1000]}

    return {
        "status_code": response.status_code,
        "url": url,
        "username": config["username"],
        "base_url": config["base_url"],
        "payload": payload,
    }


if __name__ == "__main__":
    print(json.dumps(check_jupyterhub_connection(DEFAULT_CONN_ID), indent=2, ensure_ascii=False))
