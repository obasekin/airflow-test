from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "obasekin",
    "retry_delay": 100,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": ["obasekin@arcanor.com"],
}

def test_kubernetes():
    import socket
    raise ("error")
    print("Hello from KubernetesExecutor")
    print("Hostname:", socket.gethostname())


with DAG(
    dag_id="test_kubernetes_executor",
    start_date=datetime(2026, 1, 1),
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:

    test = PythonOperator(
        task_id="test",
        python_callable=test_kubernetes,
    )