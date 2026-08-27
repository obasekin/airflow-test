from __future__ import annotations

from datetime import date
from typing import Any

from citadel.druid.query import DruidQueryService
from citadel.notifications.io_control_email import DruidIOEmailService


class DruidIOController:
    """Coordinates before/after Druid count checks and the resulting mail."""

    def __init__(
        self,
        *,
        druid_conn_id: str = "druid_default",
        smtp_conn_id: str = "smtp_default",
    ) -> None:
        self.druid_service = DruidQueryService(conn_id=druid_conn_id)
        self.mail_service = DruidIOEmailService(conn_id=smtp_conn_id)

    @staticmethod
    def normalise_target_date(target_date: str | date) -> str:
        if isinstance(target_date, date):
            return target_date.isoformat()
        return str(target_date).strip()

    @staticmethod
    def _extract_count(result: dict[str, Any] | None) -> int:
        if not result:
            return 0
        if isinstance(result, dict):
            value = result.get("count")
            if value is None:
                value = result.get("result")
            if value is None:
                rows = result.get("rows") or []
                if rows:
                    row = rows[0]
                    value = row.get("count")
                    if value is None:
                        value = row.get("EXPR$0")
                    if value is None:
                        value = next(iter(row.values()), 0)
            return int(value or 0)
        return int(result)

    def run_before_control(
        self,
        *,
        datasource: str,
        target_date: str | date,
        maid_field: str = "maid",
    ) -> dict[str, Any]:
        target_date_str = self.normalise_target_date(target_date)
        return self.druid_service.query_distinct_count_for_date(
            datasource=datasource,
            target_date=target_date_str,
            maid_field=maid_field,
        )

    def run_after_control(
        self,
        *,
        datasource: str,
        target_date: str | date,
        before_result: dict[str, Any] | None = None,
        to_email: str | list[str] | None = None,
        ingestion_status: str = "SUCCESS",
        subject: str | None = None,
        include_delta: bool = True,
        extra_rows: list[dict[str, Any]] | None = None,
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        target_date_str = self.normalise_target_date(target_date)
        after_result = self.druid_service.query_distinct_count_for_date(
            datasource=datasource,
            target_date=target_date_str,
        )
        before_count = self._extract_count(before_result)
        after_count = self._extract_count(after_result)

        self.mail_service.send_report(
            to_email=to_email,
            target_date=target_date_str,
            before_count=before_count,
            after_count=after_count,
            datasource=datasource,
            ingestion_status=ingestion_status,
            subject=subject,
            include_delta=include_delta,
            extra_rows=extra_rows,
            context=context,
        )

        return {
            "before": before_result,
            "after": after_result,
            "before_count": before_count,
            "after_count": after_count,
            "delta": after_count - before_count,
            "change_pct": self.mail_service.percent_change(before_count, after_count),
            "status": ingestion_status,
        }

    def send_report_only(
        self,
        *,
        datasource: str,
        target_date: str | date,
        before_count: int,
        after_count: int,
        to_email: str | list[str] | None,
        ingestion_status: str = "SUCCESS",
        subject: str | None = None,
        include_delta: bool = True,
        extra_rows: list[dict[str, Any]] | None = None,
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        target_date_str = self.normalise_target_date(target_date)
        self.mail_service.send_report(
            to_email=to_email,
            target_date=target_date_str,
            before_count=before_count,
            after_count=after_count,
            datasource=datasource,
            ingestion_status=ingestion_status,
            subject=subject,
            include_delta=include_delta,
            extra_rows=extra_rows,
            context=context,
        )

        return {
            "before_count": before_count,
            "after_count": after_count,
            "delta": after_count - before_count,
            "change_pct": self.mail_service.percent_change(before_count, after_count),
            "status": ingestion_status,
        }


def run_before_druid_io_check(
    *,
    datasource: str,
    target_date: str | date,
    druid_conn_id: str = "druid_default",
    maid_field: str = "maid",
) -> dict[str, Any]:
    return DruidIOController(druid_conn_id=druid_conn_id).run_before_control(
        datasource=datasource,
        target_date=target_date,
        maid_field=maid_field,
    )


def run_after_druid_io_check_and_send_mail(
    *,
    datasource: str,
    target_date: str | date,
    before_result: dict[str, Any] | None,
    to_email: str | list[str] | None,
    druid_conn_id: str = "druid_default",
    smtp_conn_id: str = "smtp_default",
    ingestion_status: str = "SUCCESS",
    subject: str | None = None,
    include_delta: bool = True,
    extra_rows: list[dict[str, Any]] | None = None,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return DruidIOController(
        druid_conn_id=druid_conn_id,
        smtp_conn_id=smtp_conn_id,
    ).run_after_control(
        datasource=datasource,
        target_date=target_date,
        before_result=before_result,
        to_email=to_email,
        ingestion_status=ingestion_status,
        subject=subject,
        include_delta=include_delta,
        extra_rows=extra_rows,
        context=context,
    )
