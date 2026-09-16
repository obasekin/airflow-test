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

    @task(
        task_id="sonuclari_mail_at",
        retries=1
    )
    def send_email_report(query_result: dict):
        html = "<h3>Druid Query Results</h3>\n"
        
        notebook_url = query_result.get("notebook_url", "")
        if notebook_url:
            html += f'<p><b>Notebook:</b> <a href="{notebook_url}">{notebook_url}</a></p>\n'
            
        outputs = query_result.get("outputs", [])
        
        if not outputs:
            html += "<p>No outputs returned from notebook.</p>"
        else:
            for item in outputs:
                if isinstance(item, dict) and item.get("type") == "stream" and "text" in item:
                    text = item["text"]
                    
                    # Parse rows
                    match = re.search(r'Result:\s*\[(.*?)\]', text, re.DOTALL)
                    if match:
                        rows_str = match.group(1)
                        row_matches = re.findall(r'Row\((.*?)\)', rows_str)
                        if row_matches:
                            html += "<table border='1' cellpadding='8' cellspacing='0' style='border-collapse: collapse;'>\n"
                            # Headers
                            first_row_items = row_matches[0].split(',')
                            headers = [i.split('=')[0].strip() for i in first_row_items]
                            html += "<tr style='background-color: #f2f2f2;'>" + "".join(f"<th>{h}</th>" for h in headers) + "</tr>\n"
                            
                            # Rows
                            for row_str in row_matches:
                                html += "<tr>"
                                items = row_str.split(',')
                                for i in items:
                                    val = i.split('=', 1)[1].strip() if '=' in i else ""
                                    html += f"<td>{val}</td>"
                                html += "</tr>\n"
                                
                            html += "</table>\n<br>\n"
                    
                    # Extract time metrics
                    time_matches = re.findall(r'Time:\s*(.*)', text)
                    if time_matches:
                        html += "<h4>Execution Time</h4>\n<ul>\n"
                        for tm in time_matches:
                            html += f"<li>{tm}</li>\n"
                        html += "</ul>\n"

        email_service = EmailService(conn_id=SMTP_CONN_ID)
        email_service.send_email(
            to="obasekin@arcanor.com",
            cc="omerfarukbasekin@gmail.com"
            subject="JupyterHub Druid Query Result",
            html_content=html,
        )

    # DAG Task Dependencies
    date_range_xcom = format_logical_date()
    query_result = execute_notebook_druid_query(date_range_xcom)
    send_email_report(query_result)

