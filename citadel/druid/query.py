from __future__ import annotations

import json
from typing import Any

import requests
from airflow.hooks.base import BaseHook


class DruidQueryService:
    """Small reusable Druid SQL client for IO-control count checks."""

    def __init__(self, conn_id: str = "druid_default") -> None:
        self.conn_id = conn_id

    def get_connection_config(self) -> dict[str, Any]:
        conn = BaseHook.get_connection(self.conn_id)
        if not conn.host:
            raise ValueError(f"Druid connection '{self.conn_id}' is missing host.")

        return {
            "host": conn.host.rstrip("/"),
            "login": conn.login,
            "password": conn.password,
            "extra": conn.extra_dejson or {},
        }

    @staticmethod
    def _serialise_sql_value(value: str | int) -> str:
        return str(int(value))

    def build_distinct_count_query(
        self,
        datasource: str,
        year: str | int,
        month: str | int,
        day: str | int,
        maid_field: str = "maid",
    ) -> str:
        year_value = self._serialise_sql_value(year)
        month_value = self._serialise_sql_value(month)
        day_value = self._serialise_sql_value(day)

        return (
            f'SELECT COUNT(DISTINCT "{maid_field}") AS count '
            f'FROM "{datasource}" '
            f'WHERE "year" = {year_value} '
            f'AND "month" = {month_value} '
            f'AND "day" = {day_value}'
        )

    def execute_query(self, query: str) -> dict[str, Any]:
        config = self.get_connection_config()
        url = config["host"]
        if not url.endswith("/druid/v2/sql"):
            url = f"{url}/druid/v2/sql"

        headers = {"Content-Type": "application/json"}
        payload = {"query": query}

        auth = None
        if config.get("login") and config.get("password"):
            auth = (config["login"], config["password"])

        response = requests.post(
            url,
            headers=headers,
            auth=auth,
            json=payload,
            timeout=120,
        )

        if response.status_code >= 400:
            raise RuntimeError(
                "Druid SQL query failed "
                f"(status={response.status_code}): {response.text}"
            )

        data = response.json()
        if not isinstance(data, list):
            raise ValueError(f"Unexpected Druid SQL response: {data!r}")

        if not data:
            return {
                "query": query,
                "rows": [],
                "count": 0,
            }

        row = data[0]
        count_value = (
            row.get("count")
            if "count" in row
            else row.get("EXPR$0")
            if "EXPR$0" in row
            else next(iter(row.values()), 0)
        )

        return {
            "query": query,
            "rows": data,
            "count": int(count_value),
        }

    def query_distinct_count_for_date(
        self,
        datasource: str,
        target_date: str,
        maid_field: str = "maid",
    ) -> dict[str, Any]:
        target_date = str(target_date)
        year, month, day = target_date.split("-")
        query = self.build_distinct_count_query(
            datasource=datasource,
            year=year,
            month=month,
            day=day,
            maid_field=maid_field,
        )
        result = self.execute_query(query)
        return {
            "datasource": datasource,
            "target_date": target_date,
            "year": int(year),
            "month": int(month),
            "day": int(day),
            "maid_field": maid_field,
            **result,
        }

    def query_distinct_count_for_date_parts(
        self,
        datasource: str,
        year: str | int,
        month: str | int,
        day: str | int,
        maid_field: str = "maid",
    ) -> dict[str, Any]:
        query = self.build_distinct_count_query(
            datasource=datasource,
            year=year,
            month=month,
            day=day,
            maid_field=maid_field,
        )
        result = self.execute_query(query)
        return {
            "datasource": datasource,
            "target_date": f"{int(year):04d}-{int(month):02d}-{int(day):02d}",
            "year": int(year),
            "month": int(month),
            "day": int(day),
            "maid_field": maid_field,
            **result,
        }


def query_distinct_count_for_date(
    datasource: str,
    target_date: str,
    conn_id: str = "druid_default",
    maid_field: str = "maid",
) -> dict[str, Any]:
    return DruidQueryService(conn_id=conn_id).query_distinct_count_for_date(
        datasource=datasource,
        target_date=target_date,
        maid_field=maid_field,
    )


def query_distinct_count_for_parts(
    datasource: str,
    year: str | int,
    month: str | int,
    day: str | int,
    conn_id: str = "druid_default",
    maid_field: str = "maid",
) -> dict[str, Any]:
    return DruidQueryService(conn_id=conn_id).query_distinct_count_for_date_parts(
        datasource=datasource,
        year=year,
        month=month,
        day=day,
        maid_field=maid_field,
    )
