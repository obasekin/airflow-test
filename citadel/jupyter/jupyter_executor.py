import json
import logging
import os
import ssl
import time
import uuid
from typing import Any

import requests
import websocket
from airflow.hooks.base import BaseHook


DEFAULT_CONN_ID = "jupyterhub_default"
DEFAULT_TIMEOUT = 60


def _read_extra_value(extra: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = extra.get(key)
        if value not in (None, ""):
            return str(value)
    return ""


def get_jupyterhub_config(
    conn_id: str = DEFAULT_CONN_ID,
) -> dict[str, str]:
    """Load and validate JupyterHub settings from an Airflow connection."""
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
            "JupyterHub Airflow connection %s unavailable; using environment fallback: %s",
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


def _session(conn_id: str) -> requests.Session:
    config = get_jupyterhub_config(conn_id)
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json",
            "Authorization": f"token {config['token']}",
        }
    )
    if config["cf_access_client_id"] and config["cf_access_client_secret"]:
        session.headers.update(
            {
                "CF-Access-Client-Id": config["cf_access_client_id"],
                "CF-Access-Client-Secret": config["cf_access_client_secret"],
            }
        )
    return session


def check_jupyterhub_connection(
    conn_id: str = DEFAULT_CONN_ID,
) -> dict[str, Any]:
    """Verify that the configured JupyterHub API can be reached."""
    config = get_jupyterhub_config(conn_id)
    response = _session(conn_id).get(
        f"{config['base_url']}/hub/api",
        timeout=DEFAULT_TIMEOUT,
        allow_redirects=False,
    )
    response.raise_for_status()
    try:
        payload = response.json()
    except ValueError:
        payload = {"raw_response": response.text[:1000]}
    return {
        "status_code": response.status_code,
        "url": f"{config['base_url']}/hub/api",
        "username": config["username"],
        "base_url": config["base_url"],
        "payload": payload,
    }


def _list_kernels(conn_id: str) -> list[dict[str, Any]]:
    config = get_jupyterhub_config(conn_id)
    response = _session(conn_id).get(
        f"{config['base_url']}/user/{config['username']}/api/kernels",
        timeout=DEFAULT_TIMEOUT,
        allow_redirects=False,
    )
    response.raise_for_status()
    return response.json()


def _create_kernel(conn_id: str) -> dict[str, Any]:
    config = get_jupyterhub_config(conn_id)
    response = _session(conn_id).post(
        f"{config['base_url']}/user/{config['username']}/api/kernels",
        json={"name": "python3"},
        timeout=DEFAULT_TIMEOUT,
        allow_redirects=False,
    )
    response.raise_for_status()
    return response.json()


def _select_kernel(conn_id: str) -> str:
    kernels = _list_kernels(conn_id)
    idle = [
        kernel
        for kernel in kernels
        if kernel.get("name") == "python3"
        and kernel.get("execution_state") == "idle"
    ]
    candidates = idle or [
        kernel for kernel in kernels if kernel.get("name") == "python3"
    ]
    if candidates:
        return candidates[0]["id"]
    return _create_kernel(conn_id)["id"]


def _notebook_payload(code: str) -> dict[str, Any]:
    return {
        "type": "notebook",
        "format": "json",
        "content": {
            "cells": [
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": code.splitlines(keepends=True),
                }
            ],
            "metadata": {
                "kernelspec": {
                    "display_name": "Python 3",
                    "language": "python",
                    "name": "python3",
                },
                "language_info": {"name": "python"},
            },
            "nbformat": 4,
            "nbformat_minor": 4,
        },
    }


def _save_notebook(
    notebook_name: str,
    notebook: dict[str, Any],
    conn_id: str,
) -> dict[str, str]:
    config = get_jupyterhub_config(conn_id)
    contents_url = (
        f"{config['base_url']}/user/{config['username']}/api/contents"
    )
    response = _session(conn_id).put(
        f"{contents_url}/{notebook_name}",
        json=notebook,
        timeout=DEFAULT_TIMEOUT,
        allow_redirects=False,
    )
    if response.status_code == 405:
        # Some JupyterHub deployments expose notebook creation as POST on the
        # contents directory, while the standard API uses PUT on the file path.
        response = _session(conn_id).post(
            contents_url,
            json={
                **notebook,
                "name": notebook_name,
            },
            timeout=DEFAULT_TIMEOUT,
            allow_redirects=False,
        )
    response.raise_for_status()
    created_path = response.json().get("path", notebook_name)
    created_name = created_path.rsplit("/", 1)[-1]
    return {
        "notebook_name": created_name,
        "notebook_path": created_path,
        "notebook_url": (
            f"{config['base_url']}/user/{config['username']}/notebooks/"
            f"{created_path}"
        ),
    }


def _create_notebook(
    code: str,
    conn_id: str,
    notebook_name: str,
) -> tuple[dict[str, str], dict[str, Any]]:
    notebook = _notebook_payload(code)
    return _save_notebook(notebook_name, notebook, conn_id), notebook


def _execute_in_kernel(
    code: str,
    kernel_id: str,
    conn_id: str,
    timeout: int,
) -> dict[str, Any]:
    config = get_jupyterhub_config(conn_id)
    ws_base = config["base_url"].replace("https://", "wss://").replace(
        "http://", "ws://"
    )
    ws_headers = [f"Authorization: token {config['token']}"]
    if config["cf_access_client_id"] and config["cf_access_client_secret"]:
        ws_headers.extend(
            [
                f"CF-Access-Client-Id: {config['cf_access_client_id']}",
                f"CF-Access-Client-Secret: {config['cf_access_client_secret']}",
            ]
        )

    ws = websocket.create_connection(
        f"{ws_base}/user/{config['username']}/api/kernels/"
        f"{kernel_id}/channels",
        header=ws_headers,
        origin=config["base_url"],
        timeout=timeout,
        sslopt={
            "cert_reqs": ssl.CERT_NONE,
            "check_hostname": False,
        },
    )
    try:
        ws.send(
            json.dumps(
                {
                    "header": {
                        "msg_id": str(uuid.uuid4()),
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
            )
        )

        outputs: list[dict[str, Any]] = []
        execution_status = None
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            message = json.loads(ws.recv())
            message_type = message.get("msg_type")
            content = message.get("content", {})
            if message_type == "stream":
                outputs.append({"type": "stream", "text": content.get("text", "")})
            elif message_type == "execute_result":
                outputs.append(
                    {
                        "type": "execute_result",
                        "data": content.get("data", {}),
                    }
                )
            elif message_type == "display_data":
                outputs.append(
                    {
                        "type": "display_data",
                        "data": content.get("data", {}),
                    }
                )
            elif message_type == "error":
                outputs.append(
                    {
                        "type": "error",
                        "ename": content.get("ename"),
                        "evalue": content.get("evalue"),
                        "traceback": content.get("traceback", []),
                    }
                )
            elif message_type == "execute_reply":
                execution_status = content.get("status")
                break
        else:
            raise TimeoutError(
                f"JupyterHub code execution timed out after {timeout} seconds"
            )
    finally:
        ws.close()

    return {
        "execution_status": execution_status,
        "outputs": outputs,
    }


def run_notebook_workflow(
    code: str,
    conn_id: str = DEFAULT_CONN_ID,
    notebook_name: str | None = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """Create a notebook, execute code in JupyterHub, and return its result."""
    if not code.strip():
        raise ValueError("code cannot be empty")
    name = notebook_name or f"airflow_notebook_{int(time.time())}.ipynb"
    notebook_info, notebook = _create_notebook(code, conn_id, name)
    kernel_id = _select_kernel(conn_id)
    execution = _execute_in_kernel(code, kernel_id, conn_id, timeout)
    notebook_cell = notebook["content"]["cells"][0]
    notebook_cell["execution_count"] = 1
    notebook_cell["outputs"] = [
        _as_notebook_output(output)
        for output in execution["outputs"]
    ]
    _save_notebook(
        notebook_info["notebook_name"],
        notebook,
        conn_id,
    )
    return {
        "status": "success",
        **notebook_info,
        "kernel_id": kernel_id,
        "execution_status": execution["execution_status"],
        "outputs": execution["outputs"],
    }


def execute_notebook_code(
    code: str,
    conn_id: str = DEFAULT_CONN_ID,
    notebook_name: str | None = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """Public executor entry point for DAG tasks and other callers."""
    return run_notebook_workflow(
        code=code,
        conn_id=conn_id,
        notebook_name=notebook_name,
        timeout=timeout,
    )


def _as_notebook_output(output: dict[str, Any]) -> dict[str, Any]:
    output_type = output.get("type")
    if output_type == "stream":
        return {
            "name": "stdout",
            "output_type": "stream",
            "text": output.get("text", ""),
        }
    if output_type in {"execute_result", "display_data"}:
        return {
            "output_type": output_type,
            "metadata": {},
            "data": output.get("data", {}),
            **(
                {"execution_count": 1}
                if output_type == "execute_result"
                else {}
            ),
        }
    if output_type == "error":
        return {
            "output_type": "error",
            "ename": output.get("ename", "Error"),
            "evalue": output.get("evalue", ""),
            "traceback": output.get("traceback", []),
        }
    return output


logger = logging.getLogger(__name__)
