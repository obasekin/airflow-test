from typing import Any

import requests
from requests import HTTPError

from citadel.druid.credentials import get_druid_credentials

DEFAULT_TIMEOUT = 120


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
