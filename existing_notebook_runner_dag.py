import pendulum
from datetime import timedelta

from airflow.decorators import dag, task
from citadel.jupyter.jupyter_runner import run_existing_notebook

default_args = {
    "owner": "obasekin",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="existing_jupyter_notebook_runner",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 8, 18, tz="Europe/Istanbul"),
    schedule=None,
    catchup=False,
    tags=["jupyterhub", "notebook", "airflow", "runner"],
    description="Example DAG to run an existing Jupyter notebook on the server",
)
def existing_notebook_runner_workflow():

    @task(task_id="run_my_existing_notebook")
    def run_notebook() -> dict:
        # Notebook dosyasinin JupyterHub ana dizinindeki (ya da ilgili klasordeki) yolu.
        # Ornegin: "analysis/my_daily_analysis.ipynb"
        notebook_path = "airflow_notebook_example.ipynb"
        
        # run_existing_notebook fonksiyonunu cagiriyoruz
        result = run_existing_notebook(
            notebook_path=notebook_path,
            timeout=600, # 10 dakika timeout
            fail_on_execution_error=True
        )
        
        return result
        
    @task(task_id="print_results")
    def print_results(notebook_result: dict):
        print(f"Notebook executed successfully!")
        print(f"URL: {notebook_result.get('notebook_url')}")
        
        outputs = notebook_result.get("outputs", [])
        print(f"Total outputs captured: {len(outputs)}")
        
        for i, out in enumerate(outputs):
            if "text" in out:
                print(f"Output [{i}]: {out['text']}")
            elif "data" in out and "text/plain" in out["data"]:
                print(f"Output [{i}]: {out['data']['text/plain']}")

    # Task bagimliliklari (XCom ile)
    result = run_notebook()
    print_results(result)

# DAG tanimlamasi
existing_notebook_runner_workflow()
