import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from airflow.hooks.base import BaseHook


class EmailNotifier:

    def __init__(
        self,
        conn_id="smtp_default",
        to_email=None,
    ):
        self.conn_id = conn_id
        self.to_email = to_email

    def __call__(self, context):

        # ============================================================
        # AIRFLOW CONNECTION
        # ============================================================

        conn = BaseHook.get_connection(self.conn_id)

        smtp_host = conn.host
        smtp_port = conn.port or 587
        smtp_user = conn.login
        smtp_password = conn.password

        # ============================================================
        # TASK INFORMATION
        # ============================================================

        ti = context["task_instance"]

        dag_id = ti.dag_id
        task_id = ti.task_id
        run_id = ti.run_id
        logical_date = ti.logical_date

        exception = context.get("exception")

        # ============================================================
        # EMAIL
        # ============================================================

        subject = (
            f"Airflow Task Failed: "
            f"{dag_id}.{task_id}"
        )

        body = f"""
        <html>
        <body>

        <h2>Airflow Task Failed</h2>

        <hr>

        <p><b>DAG:</b> {dag_id}</p>

        <p><b>Task:</b> {task_id}</p>

        <p><b>Run ID:</b> {run_id}</p>

        <p><b>Execution Date:</b> {logical_date}</p>

        <p><b>Exception:</b></p>

        <pre>{exception}</pre>

        </body>
        </html>
        """

        msg = MIMEMultipart("alternative")

        msg["From"] = smtp_user
        msg["To"] = self.to_email
        msg["Subject"] = subject

        msg.attach(
            MIMEText(body, "html")
        )

        # ============================================================
        # SMTP
        # ============================================================

        with smtplib.SMTP(
            smtp_host,
            smtp_port,
            timeout=30,
        ) as server:

            server.ehlo()

            server.starttls()

            server.ehlo()

            server.login(
                smtp_user,
                smtp_password,
            )

            server.sendmail(
                smtp_user,
                self.to_email,
                msg.as_string(),
            )