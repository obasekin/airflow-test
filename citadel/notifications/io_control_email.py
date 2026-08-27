from __future__ import annotations

from datetime import datetime
from typing import Any

from citadel.notifications.email import EmailNotifier


class DruidIOEmailService:
    """Utility to format and send Druid IO control reports."""

    def __init__(self, conn_id: str = "smtp_default") -> None:
        self.conn_id = conn_id

    @staticmethod
    def format_number(value: int | float | None) -> str:
        if value is None:
            return "0"
        return f"{int(value):,}"

    @staticmethod
    def build_subject(datasource: str = "TURv2", target_date: str | None = None) -> str:
        if target_date is None:
            target_date = datetime.utcnow().strftime("%Y-%m-%d")
        return f"[{datasource}] Druid IO Control - {target_date}"

    @staticmethod
    def percent_change(before: int | float, after: int | float) -> float:
        if before in (None, 0):
            if after in (None, 0):
                return 0.0
            return 100.0
        return ((after - before) / before) * 100.0

    @staticmethod
    def delta(before: int | float, after: int | float) -> int:
        return int(after) - int(before)

    def build_body(
        self,
        *,
        target_date: str,
        before_count: int,
        after_count: int,
        datasource: str = "TURv2",
        ingestion_status: str = "SUCCESS",
        include_delta: bool = True,
        extra_rows: list[dict[str, Any]] | None = None,
    ) -> str:
        body_lines = [
            f"{datasource} Druid IO Control",
            "======================",
            "",
            "Target Date:",
            f"{target_date}",
            "",
            "BEFORE INGESTION",
            "----------------",
            f"{target_date}",
            f"Distinct MAID: {self.format_number(before_count)}",
            "",
            "AFTER INGESTION",
            "---------------",
            f"{target_date}",
            f"Distinct MAID: {self.format_number(after_count)}",
            "",
            "INGESTION",
            "---------",
            f"Status: {ingestion_status}",
        ]

        if include_delta:
            delta_value = self.delta(before_count, after_count)
            change_value = self.percent_change(before_count, after_count)
            sign = "+" if delta_value >= 0 else ""
            body_lines.extend(
                [
                    "",
                    "CHANGE",
                    "------",
                    f"{target_date}",
                    f"Before : {self.format_number(before_count)}",
                    f"After  : {self.format_number(after_count)}",
                    f"Delta  : {sign}{self.format_number(delta_value)}",
                    f"Change : {sign}{change_value:.2f}%",
                ]
            )

        if extra_rows:
            body_lines.extend(["", "DETAILS", "-------"])
            for row in extra_rows:
                label = row.get("label", "")
                value = row.get("value", "")
                body_lines.append(f"{label}: {value}")

        return "\n".join(body_lines)

    def create_message(
        self,
        *,
        to_email: str | list[str] | None,
        subject: str | None = None,
        target_date: str,
        before_count: int,
        after_count: int,
        datasource: str = "TURv2",
        ingestion_status: str = "SUCCESS",
        include_delta: bool = True,
        extra_rows: list[dict[str, Any]] | None = None,
    ) -> tuple[str, str]:
        effective_subject = subject or self.build_subject(
            datasource=datasource,
            target_date=target_date,
        )
        body = self.build_body(
            target_date=target_date,
            before_count=before_count,
            after_count=after_count,
            datasource=datasource,
            ingestion_status=ingestion_status,
            include_delta=include_delta,
            extra_rows=extra_rows,
        )
        return effective_subject, body

    def send_report(
        self,
        *,
        to_email: str | list[str] | None,
        target_date: str,
        before_count: int,
        after_count: int,
        datasource: str = "TURv2",
        ingestion_status: str = "SUCCESS",
        subject: str | None = None,
        include_delta: bool = True,
        extra_rows: list[dict[str, Any]] | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        effective_subject, body = self.create_message(
            to_email=to_email,
            subject=subject,
            target_date=target_date,
            before_count=before_count,
            after_count=after_count,
            datasource=datasource,
            ingestion_status=ingestion_status,
            include_delta=include_delta,
            extra_rows=extra_rows,
        )

        notifier = EmailNotifier(
            to_email=to_email,
            subject=effective_subject,
            html_content=body,
            conn_id=self.conn_id,
        )
        notifier.send_message(
            to_email=to_email,
            subject=effective_subject,
            html_content=body,
            context=context,
        )


def send_druid_io_control_email(
    *,
    to_email: str | list[str],
    target_date: str,
    before_count: int,
    after_count: int,
    datasource: str = "TURv2",
    ingestion_status: str = "SUCCESS",
    subject: str | None = None,
    conn_id: str = "smtp_default",
    include_delta: bool = True,
    extra_rows: list[dict[str, Any]] | None = None,
    context: dict[str, Any] | None = None,
) -> None:
    service = DruidIOEmailService(conn_id=conn_id)
    service.send_report(
        to_email=to_email,
        target_date=target_date,
        before_count=before_count,
        after_count=after_count,
        datasource=datasource,
        ingestion_status=ingestion_status,
        subject=subject,
        include_delta=include_delta,
        extra_rows=extra_rows,
        context=context,
    )
