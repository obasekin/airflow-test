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
        to_email: str | list[str] | None = None,
        subject: str | None = None,
        html_content: str | None = None,
        conn_id: str = "smtp_default",
    ):
        super().__init__()

        self.conn_id = conn_id
        self.to_email = to_email
        self.subject = subject
        self.html_content = html_content

    def send_message(
        self,
        *,
        to_email: str | list[str] | None = None,
        subject: str | None = None,
        html_content: str | None = None,
        context: Context | dict | None = None,
    ) -> None:
        if to_email is not None:
            self.to_email = to_email
        if subject is not None:
            self.subject = subject
        if html_content is not None:
            self.html_content = html_content

        if context is None:
            context = {
                "dag": None,
                "dag_run": None,
                "task": None,
                "exception": None,
            }

        self.notify(context)

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
        # AIRFLOW SMTP CONNECTION
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

        smtp_port = conn.port
        smtp_user = conn.login
        smtp_password = conn.password

        extra = conn.extra_dejson or {}

        # ============================================================
        # SMTP SETTINGS
        # ============================================================

        # disable_ssl=True:
        #
        # SMTP SSL certificate verification is disabled.
        #
        # IMPORTANT:
        # This does NOT disable TLS.
        #
        # With:
        #
        # disable_ssl=True
        # disable_tls=False
        #
        # the connection is:
        #
        # SMTP -> EHLO -> STARTTLS -> TLS -> AUTH -> SEND
        #

        disable_ssl = extra.get(
            "disable_ssl",
            True,
        )

        # disable_tls=True:
        #
        # STARTTLS is completely disabled.
        #
        # Normally this should remain False for port 587.
        #

        disable_tls = extra.get(
            "disable_tls",
            False,
        )

        # ------------------------------------------------------------
        # PORT
        # ------------------------------------------------------------

        if smtp_port is None:

            if disable_ssl:
                smtp_port = 587
            else:
                smtp_port = 465

        # ------------------------------------------------------------
        # TIMEOUT
        # ------------------------------------------------------------

        timeout = int(
            extra.get(
                "timeout",
                30,
            )
        )

        # ------------------------------------------------------------
        # RETRY
        # ------------------------------------------------------------

        retry_limit = int(
            extra.get(
                "retry_limit",
                5,
            )
        )

        # ------------------------------------------------------------
        # FROM
        # ------------------------------------------------------------

        from_email = (
            extra.get("from_email")
            or smtp_user
        )

        if not from_email:
            raise ValueError(
                f"SMTP connection '{self.conn_id}' "
                "has no login/from_email."
            )

        # ============================================================
        # RECIPIENT
        # ============================================================

        if not self.to_email:
            raise ValueError(
                "EmailNotifier requires "
                "'to_email'."
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

        if not recipients:
            raise ValueError(
                "EmailNotifier requires "
                "at least one recipient."
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
        # HTML
        # ============================================================

        html_content = (
            self.html_content
            or
            f"""
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
        )

        # ============================================================
        # MIME MESSAGE
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
        # SSL CONTEXT
        # ============================================================

        if disable_ssl:

            # --------------------------------------------------------
            # TLS remains ENABLED.
            #
            # Only certificate verification is disabled.
            #
            # This is required for your Proofpoint endpoint because:
            #
            # smtp.domain.com
            #
            # receives a certificate for:
            #
            # *.smtp.a.cloudfilter.net
            # --------------------------------------------------------

            ssl_context = (
                ssl._create_unverified_context()
            )

        else:

            # --------------------------------------------------------
            # Normal secure certificate verification.
            # --------------------------------------------------------

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

                # ====================================================
                # PORT 465 / SMTPS
                # ====================================================

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

                # ====================================================
                # PORT 587 / STARTTLS
                # ====================================================

                else:

                    with smtplib.SMTP(
                        smtp_host,
                        smtp_port,
                        timeout=timeout,
                    ) as server:

                        # --------------------------------------------
                        # Initial EHLO
                        # --------------------------------------------

                        server.ehlo()

                        # --------------------------------------------
                        # STARTTLS
                        # --------------------------------------------

                        if not disable_tls:

                            server.starttls(
                                context=ssl_context
                            )

                            # ----------------------------------------
                            # EHLO after STARTTLS
                            # ----------------------------------------

                            server.ehlo()

                        # --------------------------------------------
                        # AUTH
                        # --------------------------------------------

                        if (
                            smtp_user
                            and smtp_password
                        ):

                            server.login(
                                smtp_user,
                                smtp_password,
                            )

                        # --------------------------------------------
                        # SEND
                        # --------------------------------------------

                        server.sendmail(
                            from_email,
                            recipients,
                            msg.as_string(),
                        )

                # ====================================================
                # SUCCESS
                # ====================================================

                return

            except (
                smtplib.SMTPException,
                OSError,
                ssl.SSLError,
            ) as exc:

                last_exception = exc

                if attempt >= retry_limit:
                    raise

        if last_exception:
            raise last_exception