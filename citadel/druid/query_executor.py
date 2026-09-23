"""
Druid query execution module.

This module provides functionality to execute SQL queries against a Druid cluster
using an Airflow connection for authentication.

Usage:
    results = execute_query(query="SELECT * FROM my_table", conn_id="my_druid_conn")

Parameters:
    query (str): The SQL query to execute.
    conn_id (str, optional): The Airflow connection ID to use. Defaults to None.
    timeout (int, optional): The request timeout in seconds. Defaults to DEFAULT_TIMEOUT.

Returns:
    Any: The JSON response from the Druid query API.
"""
from typing import Any

import requests
from requests import HTTPError

from citadel.druid.credentials import get_druid_credentials
from citadel.config_loader import config

DEFAULT_TIMEOUT = config.druid.get('status_timeout', 120)


def execute_query(
    query: str,
    conn_id: str | None = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> Any:
    """Execute a SQL query against Druid using an Airflow connection."""
    if not query or not query.strip():
        raise ValueError("query cannot be empty")

    druid_url, username, password = get_druid_credentials(conn_id)

    response = requests.post(
        f"{druid_url}/druid/v2/sql",
        headers={"Content-Type": "application/json"},
        json={"query": query},
        auth=(username, password),
        timeout=timeout,
    )
    try:
        response.raise_for_status()
    except HTTPError as exc:
        raise HTTPError(
            f"{exc} - Druid response: {response.text}",
            response=response,
        ) from exc

    return response.json()
