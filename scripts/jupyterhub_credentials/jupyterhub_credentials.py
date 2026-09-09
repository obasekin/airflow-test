import json
import os
import time
import ssl
import uuid
from typing import Any, Dict, List, Optional

import requests
import websocket

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


def list_kernels(conn_id: str = DEFAULT_CONN_ID) -> List[Dict[str, Any]]:
    """List all kernels for the JupyterHub user."""
    config = get_jupyterhub_config(conn_id=conn_id)
    session = build_jupyterhub_session(conn_id=conn_id)
    url = f"{config['base_url']}/user/{config['username']}/api/kernels"

    response = session.get(url, timeout=60, allow_redirects=False)
    response.raise_for_status()

    return response.json()


def get_or_select_kernel(
    conn_id: str = DEFAULT_CONN_ID,
    prefer_idle: bool = True,
) -> str:
    """
    Get an existing kernel ID.
    Prefer idle python3 kernels; fallback to any python3 kernel.
    """
    kernels = list_kernels(conn_id=conn_id)

    if not kernels:
        raise RuntimeError("No existing kernels found.")

    if prefer_idle:
        idle_kernels = [
            k
            for k in kernels
            if k.get("name") == "python3"
            and k.get("execution_state") == "idle"
        ]
        if idle_kernels:
            return idle_kernels[0]["id"]

    python_kernels = [k for k in kernels if k.get("name") == "python3"]
    if python_kernels:
        return python_kernels[0]["id"]

    raise RuntimeError("No python3 kernel found.")


def create_notebook(
    conn_id: str = DEFAULT_CONN_ID,
    notebook_name: Optional[str] = None,
    code_cells: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Create a new notebook in JupyterHub."""
    config = get_jupyterhub_config(conn_id=conn_id)
    session = build_jupyterhub_session(conn_id=conn_id)

    if notebook_name is None:
        notebook_name = f"airflow_notebook_{int(time.time())}.ipynb"

    url = f"{config['base_url']}/user/{config['username']}/api/contents/{notebook_name}"

    cells = []
    if code_cells:
        for code in code_cells:
            cells.append({
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": code.split("\n") if "\n" in code else [code],
            })

    notebook = {
        "type": "notebook",
        "format": "json",
        "content": {
            "cells": cells if cells else [],
            "metadata": {
                "kernelspec": {
                    "display_name": "Python 3",
                    "language": "python",
                    "name": "python3",
                },
                "language_info": {
                    "name": "python",
                },
            },
            "nbformat": 4,
            "nbformat_minor": 4,
        },
    }

    response = session.put(url, json=notebook, timeout=60, allow_redirects=False)
    response.raise_for_status()

    return {
        "notebook_name": notebook_name,
        "notebook_path": response.json().get("path"),
        "url": f"{config['base_url']}/user/{config['username']}/notebooks/{notebook_name}",
    }


def execute_code_in_kernel(
    code: str,
    kernel_id: str,
    conn_id: str = DEFAULT_CONN_ID,
    timeout: int = 30,
) -> Dict[str, Any]:
    """Execute code in a Jupyter kernel via WebSocket."""
    config = get_jupyterhub_config(conn_id=conn_id)

    ws_url = (
        f"wss://{config['base_url'].replace('https://', '')}"
        f"/user/{config['username']}/api/kernels/{kernel_id}/channels"
    )

    ws_headers = [
        f"Authorization: token {config['token']}",
    ]
    if config.get("cf_access_client_id") and config.get("cf_access_client_secret"):
        ws_headers.append(f"CF-Access-Client-Id: {config['cf_access_client_id']}")
        ws_headers.append(f"CF-Access-Client-Secret: {config['cf_access_client_secret']}")

    try:
        ws = websocket.create_connection(
            ws_url,
            header=ws_headers,
            origin=config["base_url"],
            timeout=timeout,
            sslopt={
                "cert_reqs": ssl.CERT_NONE,
                "check_hostname": False,
            },
        )
    except Exception as e:
        raise RuntimeError(f"WebSocket connection failed: {type(e).__name__}: {e}")

    msg_id = str(uuid.uuid4())
    message = {
        "header": {
            "msg_id": msg_id,
            "username": config["username"],
            "session": str(uuid.uuid4()),
            "msg_type": "execute_request",
            "version": "5.3",
        },
        "parent_header": {},
        "metadata": {},
        "content": {
            "code": code,
            "silent": False,
            "store_history": True,
            "user_expressions": {},
            "allow_stdin": False,
            "stop_on_error": True,
        },
        "channel": "shell",
    }

    ws.send(json.dumps(message))

    outputs = []
    execution_status = None
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            raw = ws.recv()
            if not raw:
                continue

            msg = json.loads(raw)
            msg_type = msg.get("msg_type")

            if msg_type == "stream":
                text = msg.get("content", {}).get("text", "")
                outputs.append({
                    "type": "stream",
                    "text": text,
                })

            elif msg_type == "execute_result":
                data = msg.get("content", {}).get("data", {})
                outputs.append({
                    "type": "execute_result",
                    "data": data,
                })

            elif msg_type == "error":
                content = msg.get("content", {})
                outputs.append({
                    "type": "error",
                    "ename": content.get("ename"),
                    "evalue": content.get("evalue"),
                    "traceback": content.get("traceback", []),
                })

            elif msg_type == "execute_reply":
                content = msg.get("content", {})
                execution_status = content.get("status")
                break

        except Exception as e:
            outputs.append({
                "type": "error",
                "message": str(e),
            })
            break

    ws.close()

    return {
        "execution_status": execution_status,
        "outputs": outputs,
    }


def run_notebook_workflow(
    code: str,
    conn_id: str = DEFAULT_CONN_ID,
    notebook_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Complete workflow: create notebook, execute code, return results + notebook link.
    """
    config = get_jupyterhub_config(conn_id=conn_id)

    # 1. Create notebook
    notebook_info = create_notebook(
        conn_id=conn_id,
        notebook_name=notebook_name,
        code_cells=[code],
    )

    # 2. Get kernel
    kernel_id = get_or_select_kernel(conn_id=conn_id, prefer_idle=True)

    # 3. Execute code
    result = execute_code_in_kernel(
        code=code,
        kernel_id=kernel_id,
        conn_id=conn_id,
        timeout=30,
    )

    return {
        "status": "success",
        "notebook_name": notebook_info["notebook_name"],
        "notebook_path": notebook_info["notebook_path"],
        "notebook_url": notebook_info["url"],
        "kernel_id": kernel_id,
        "execution_status": result["execution_status"],
        "outputs": result["outputs"],
    }


if __name__ == "__main__":
    print(json.dumps(check_jupyterhub_connection(DEFAULT_CONN_ID), indent=2, ensure_ascii=False))
