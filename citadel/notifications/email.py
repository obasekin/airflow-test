from __future__ import annotations

import smtplib
import ssl

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate

from airflow.sdk import BaseHook, BaseNotifier, Context


class EmailNotifier(BaseNotifier):

    template_fields = (
        "to_email",
        "subject",
        "html_content",
    )

    def __init__(
        self,
        *,
        conn_id: str = "smtp_default",
        to_email: str | list[str] | None = None,
        subject: str | None = None,
        html_content: str | None = None,
    ):
        super().__init__()

        self.conn_id = conn_id
        self.to_email = to_email
        self.subject = subject
        self.html_content = html_content

    def notify(self, context: Context) -> None:

        # ============================================================
        # AIRFLOW CONTEXT
        # ============================================================

        dag = context.get("dag")
        dag_run = context.get("dag_run")
        task = context.get("task")
        exception = context.get("exception")

        dag_id = (
            dag.dag_id
            if dag
            else "unknown"
        )

        task_id = (
            task.task_id
            if task
            else "unknown"
        )

        run_id = (
            dag_run.run_id
            if dag_run
            else "unknown"
        )

        logical_date = (
            dag_run.logical_date
            if dag_run
            else None
        )

        # ============================================================
        # SMTP CONNECTION
        # ============================================================

        conn = BaseHook.get_connection(
            self.conn_id
        )

        smtp_host = conn.host

        if not smtp_host:
            raise ValueError(
                f"SMTP connection '{self.conn_id}' "
                "has no host."
            )

        smtp_user = conn.login
        smtp_password = conn.password

        extra = conn.extra_dejson

        disable_ssl = extra.get(
            "disable_ssl",
            True,
        )

        disable_tls = extra.get(
            "disable_tls",
            False,
        )

        smtp_port = conn.port

        if smtp_port is None:
            smtp_port = (
                587
                if disable_ssl
                else 465
            )

        timeout = int(
            extra.get(
                "timeout",
                30,
            )
        )

        retry_limit = int(
            extra.get(
                "retry_limit",
                5,
            )
        )

        from_email = (
            extra.get("from_email")
            or smtp_user
        )

        # ============================================================
        # RECIPIENTS
        # ============================================================

        if not self.to_email:
            raise ValueError(
                "EmailNotifier requires "
                "at least one recipient."
            )

        if isinstance(
            self.to_email,
            str,
        ):
            recipients = [
                self.to_email
            ]
        else:
            recipients = list(
                self.to_email
            )

        # ============================================================
        # SUBJECT
        # ============================================================

        subject = (
            self.subject
            or
            f"Airflow Task Failed: "
            f"{dag_id}.{task_id}"
        )

        # ============================================================
        # BODY
        # ============================================================

        if self.html_content:

            html_content = (
                self.html_content
            )

        else:

            html_content = f"""
<html>
<body>

<h2>Airflow Task Failed</h2>

<hr>

<p>
    <b>DAG:</b> {dag_id}
</p>

<p>
    <b>Task:</b> {task_id}
</p>

<p>
    <b>Run ID:</b> {run_id}
</p>

<p>
    <b>Execution Date:</b> {logical_date}
</p>

<h3>Exception</h3>

<pre>{exception}</pre>

</body>
</html>
"""

        # ============================================================
        # MESSAGE
        # ============================================================

        msg = MIMEMultipart(
            "alternative"
        )

        msg["From"] = from_email
        msg["To"] = ", ".join(
            recipients
        )
        msg["Subject"] = subject
        msg["Date"] = formatdate(
            localtime=True
        )

        msg.attach(
            MIMEText(
                html_content,
                "html",
                "utf-8",
            )
        )

        # ============================================================
        # SSL
        # ============================================================

        ssl_context = (
            ssl.create_default_context()
        )

        # ============================================================
        # SEND
        # ============================================================

        last_exception = None

        for attempt in range(
            retry_limit + 1
        ):

            try:

                # ----------------------------------------------------
                # SSL / 465
                # ----------------------------------------------------

                if not disable_ssl:

                    with smtplib.SMTP_SSL(
                        smtp_host,
                        smtp_port,
                        timeout=timeout,
                        context=ssl_context,
                    ) as server:

                        server.ehlo()

                        if (
                            smtp_user
                            and smtp_password
                        ):
                            server.login(
                                smtp_user,
                                smtp_password,
                            )

                        server.sendmail(
                            from_email,
                            recipients,
                            msg.as_string(),
                        )

                # ----------------------------------------------------
                # STARTTLS / 587
                # ----------------------------------------------------

                else:

                    with smtplib.SMTP(
                        smtp_host,
                        smtp_port,
                        timeout=timeout,
                    ) as server:

                        server.ehlo()

                        if not disable_tls:

                            server.starttls(
                                context=ssl_context
                            )

                            server.ehlo()

                        if (
                            smtp_user
                            and smtp_password
                        ):
                            server.login(
                                smtp_user,
                                smtp_password,
                            )

                        server.sendmail(
                            from_email,
                            recipients,
                            msg.as_string(),
                        )

                return

            except (
                smtplib.SMTPException,
                OSError,
                ssl.SSLError,
            ) as exc:

                last_exception = exc

                if (
                    attempt
                    >= retry_limit
                ):
                    raise

        if last_exception:
            raise last_exception