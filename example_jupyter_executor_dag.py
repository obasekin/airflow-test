"""
Example DAG: JupyterHub Dynamic Notebook Executor
=================================================
Bu DAG, `citadel.jupyter.jupyter_executor` modülündeki:
- `execute_druid_query_in_notebook`
- `execute_notebook_code`
fonksiyonlarının kullanımını gösterir.

Ne zaman kullanılır?
--------------------
- JupyterHub üzerinde dinamik olarak yeni bir notebook oluşturup kod çalıştırmak istediğinizde
- Druid üzerinde pydruid ile sorgu çalıştırıp çıktıyı Jupyter notebook formatında saklamak istediğinizde
- Ağır hesaplama veya analiz kodlarını Airflow worker yerine JupyterHub pod/kernel'ında koşturmak için

Kullanılan Servis:
------------------
- `citadel.jupyter.jupyter_executor.execute_druid_query_in_notebook`
- `citadel.jupyter.jupyter_executor.execute_notebook_code`
"""

from datetime import timedelta
import pendulum
from airflow.decorators import dag, task

from citadel.config_loader import config
from citadel.jupyter.jupyter_executor import (
    execute_druid_query_in_notebook,
    execute_notebook_code,
)

default_args = {
    "owner": "obasekin",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="example_citadel_jupyter_executor",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Istanbul"),
    schedule=None,  # Manuel tetiklemeli örnek DAG
    catchup=False,
    tags=["example", "citadel", "jupyter", "notebook"],
    description="JupyterHub üzerinde dinamik notebook oluşturup kod koşturma örneği",
)
def example_jupyter_executor_workflow():

    @task(task_id="run_custom_python_in_notebook")
    def run_arbitrary_code() -> dict:
        """
        Örnek 1: JupyterHub üzerinde dinamik yeni bir notebook oluşturur
        ve verilen Python kodunu çalıştırıp çıktıları döner.
        """
        python_code = """
import sys
import platform

print("Hello from JupyterHub Kernel!")
print(f"Python Version: {platform.python_version()}")
print(f"Platform: {platform.platform()}")

# Örnek hesaplama
total = sum(i ** 2 for i in range(100))
print(f"Sum of squares (0..99): {total}")
"""

        print("Executing Python code on JupyterHub...")
        result = execute_notebook_code(
            code=python_code,
            conn_id=config.jupyter.get("conn_id", "jupyterhub_default"),
            timeout=config.jupyter.get("timeout", 1800),
            fail_on_execution_error=True,
        )

        print(f"Notebook created: {result.get('notebook_url')}")
        print(f"Outputs received: {len(result.get('outputs', []))}")
        return result

    @task(task_id="run_druid_query_in_notebook")
    def run_druid_notebook() -> dict:
        """
        Örnek 2: Druid credentials'larını otomatik inject ederek
        pydruid ile sorgu çalıştıran bir notebook oluşturur.
        """
        # {query}, {host}, {port}, {username}, {password} gibi alanlar otomatik bağlanabilir
        druid_code = """
from pydruid.db import connect

# Bağlantı bilgileri execute_druid_query_in_notebook tarafından sağlanır
print("Executing Druid query via notebook...")
query = "SELECT 'BEL' AS country, CURRENT_TIMESTAMP AS query_time"
print(f"Query: {query}")
"""

        print("Running Druid query in notebook...")
        result = execute_druid_query_in_notebook(
            code=druid_code,
            conn_id=config.jupyter.get("conn_id", "jupyterhub_default"),
            druid_conn_id=config.druid.get("conn_id", "druid_default"),
            timeout=config.jupyter.get("timeout", 1800),
            fail_on_execution_error=False,
        )

        return result

    @task(task_id="inspect_outputs")
    def inspect_outputs(code_result: dict, druid_result: dict):
        """
        Her iki çalıştırmadan dönen notebook linklerini ve çıktılarını gösterir.
        """
        print("=== Execution 1 Results ===")
        print(f"URL: {code_result.get('notebook_url')}")
        for out in code_result.get("outputs", []):
            if "text" in out:
                print(f"Text output: {out['text'].strip()}")

        print("\n=== Execution 2 Results ===")
        print(f"URL: {druid_result.get('notebook_url')}")

    r1 = run_arbitrary_code()
    r2 = run_druid_notebook()
    inspect_outputs(r1, r2)


example_jupyter_executor_workflow()

