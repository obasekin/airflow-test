from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s


def start_task():
    """İlk task: Hello World yazan Python kodu"""
    print("=" * 50)
    print("START TASK")
    print("=" * 50)
    print("Hello World!")
    print("=" * 50)


def dummy_task(task_name):
    """Dummy task'ları"""
    print(f"Dummy Task: {task_name} çalışıyor...")
    import time
    time.sleep(2)
    print(f"Dummy Task: {task_name} tamamlandı!")


def end_task():
    """Son task"""
    print("=" * 50)
    print("END TASK - Tüm görevler tamamlandı!")
    print("=" * 50)


# Pod'da çalışacak Python kodu
pod_commands = [
    "python",
    "-c",
    """
import socket
import os
print('='*50)
print(f'POD TASK')
print(f'Hostname: {socket.gethostname()}')
print(f'Pod IP: {os.environ.get("HOSTNAME", "N/A")}')
print(f'Python Version: {__import__("sys").version.split()[0]}')
print('='*50)
""",
]


with DAG(
    dag_id="test_printer_5min",
    start_date=datetime(2026, 8, 14),
    schedule_interval="*/5 * * * *",  # 5 dakikada bir çalışır
    catchup=False,
    tags=["test", "kubernetes"],
) as dag:

    # START TASK
    start = PythonOperator(
        task_id="start",
        python_callable=start_task,
    )

    # PART 1 ve PART 2 - Paralel Dummy Tasks
    part_1 = PythonOperator(
        task_id="part_1",
        python_callable=dummy_task,
        op_kwargs={"task_name": "Part 1"},
    )

    part_2 = PythonOperator(
        task_id="part_2",
        python_callable=dummy_task,
        op_kwargs={"task_name": "Part 2"},
    )

    # 3 Tane Paralel Kubernetes Pod Operatörü
    pod_1 = KubernetesPodOperator(
        task_id="kubernetes_pod_1",
        name="test-pod-1",
        namespace="default",
        image="python:3.9",
        cmds=pod_commands[0:1],
        arguments=pod_commands[1:],
        is_delete_operator_pod=True,
        get_logs=True,
    )

    pod_2 = KubernetesPodOperator(
        task_id="kubernetes_pod_2",
        name="test-pod-2",
        namespace="default",
        image="python:3.9",
        cmds=pod_commands[0:1],
        arguments=pod_commands[1:],
        is_delete_operator_pod=True,
        get_logs=True,
    )

    pod_3 = KubernetesPodOperator(
        task_id="kubernetes_pod_3",
        name="test-pod-3",
        namespace="default",
        image="python:3.9",
        cmds=pod_commands[0:1],
        arguments=pod_commands[1:],
        is_delete_operator_pod=True,
        get_logs=True,
    )

    # END TASK
    end = PythonOperator(
        task_id="end",
        python_callable=end_task,
    )

    # Bağlantılar
    start >> [part_1, part_2]  # start'tan part_1 ve part_2'ye paralel
    [part_1, part_2] >> [pod_1, pod_2, pod_3]  # part'lardan 3 pod'a paralel
    [pod_1, pod_2, pod_3] >> end  # tüm pod'lardan end'e
