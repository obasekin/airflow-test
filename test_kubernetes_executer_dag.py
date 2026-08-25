from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from citadel.notifications.email import EmailNotifier


failure_email = EmailNotifier(
    conn_id="smtp_default",
    to_email="obasekin@arcanor.com",
)


def test_kubernetes():

    import socket

    raise Exception("SMTP FAILURE TEST")

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
        on_failure_callback=failure_email,
    )