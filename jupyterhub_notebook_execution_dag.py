import re
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

from citadel.jupyter.jupyter_executor import (
    execute_druid_query_in_notebook,
)
from citadel.notifications.email import EmailService
from config.settings import NOTIFICATION_EMAILS, SMTP_CONN_ID


DAG_ID = "jupyterhub_notebook_execution"
FAIL_ON_NOTEBOOK_ERROR = True
QUERY = """
select COUNT(DISTINCT "maid"), "day" from "TUR"
WHERE __time >= TIMESTAMP '{start_time}'
  AND __time < TIMESTAMP '{end_time}'
GROUP BY "day"
"""

NOTEBOOK_CODE = """
from pydruid.db import connect

druid_connection = connect(
    host={host!r},
    port={port!r},
    path={path!r},
    scheme={scheme!r},
    user={username!r},
    password={password!r},
)

druid_cursor = druid_connection.cursor()

query = \"\"\"
{query}
\"\"\"

druid_cursor.execute(query)
result = druid_cursor.fetchall()
print("Result:", result)
"""


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Execute code in JupyterHub and return notebook results via XCom",
    start_date=datetime(2026, 8, 18),
    schedule=None,
    catchup=False,
    tags=["jupyterhub", "notebook", "airflow"],
) as dag:

    @task
    def format_logical_date(**kwargs) -> dict:
        logical_date = kwargs["logical_date"]
        start_time = (logical_date - timedelta(days=5)).strftime("%Y-%m-%d 00:00:00")
        end_time = logical_date.strftime("%Y-%m-%d 00:00:00")
        return {"start_time": start_time, "end_time": end_time}

    @task
    def execute_notebook_druid_query(date_range: dict) -> list:
        formatted_query = QUERY.format(
            start_time=date_range["start_time"],
            end_time=date_range["end_time"]
        )
        result = execute_druid_query_in_notebook(
            code=NOTEBOOK_CODE.replace("{query}", formatted_query.strip()),
            fail_on_execution_error=FAIL_ON_NOTEBOOK_ERROR,
        )
        return result

    @task
    def send_email_report(result_list: list):
        html = "<h3>Druid Query Results</h3>\n"
        if not result_list:
            html += "<p>No results returned.</p>"
        else:
            html += "<ul>\n"
            for row in result_list:
                html += f"  <li>{row}</li>\n"
            html += "</ul>\n"

        email_service = EmailService(conn_id=SMTP_CONN_ID)
        email_service.send_email(
            to=NOTIFICATION_EMAILS,
            subject="JupyterHub Druid Query Result",
            html_content=html,
        )

    # DAG Task Dependencies
    date_range_xcom = format_logical_date()
    query_result = execute_notebook_druid_query(date_range_xcom)
    send_email_report(query_result)

