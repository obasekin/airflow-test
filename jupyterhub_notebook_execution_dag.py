from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

from citadel.jupyter.jupyter_executor import (
    execute_druid_query_in_notebook,
)


DAG_ID = "jupyterhub_notebook_execution"
FAIL_ON_NOTEBOOK_ERROR = True
QUERY = """
select COUNT(DISTINCT "maid"), "day" from "TUR"
WHERE __time >= TIMESTAMP '2026-09-01 00:00:00'
  AND __time < TIMESTAMP '2026-09-06 00:00:00'
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
    def execute_notebook_druid_query() -> dict:
        result = execute_druid_query_in_notebook(
            code=NOTEBOOK_CODE.replace("{query}", QUERY.strip()),
            fail_on_execution_error=FAIL_ON_NOTEBOOK_ERROR,
        )
        return result

    execute_notebook_druid_query()
