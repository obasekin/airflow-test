from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def test_kubernetes():
    import socket

    print("Hello from KubernetesExecutor")
    print("Hostname:", socket.gethostname())


with DAG(
    dag_id="test_kubernetes_executor",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    test = PythonOperator(
        task_id="test",
        python_callable=test_kubernetes,
    )
