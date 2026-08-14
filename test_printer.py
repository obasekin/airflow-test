from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


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


def pod_task(pod_id):
    """Kubernetes Pod'u simüle eden task"""
    import socket
    import os
    import platform
    
    print("=" * 60)
    print(f"POD TASK - {pod_id}")
    print("=" * 60)
    print(f"Hostname: {socket.gethostname()}")
    print(f"Pod ID: {pod_id}")
    print(f"IP Address: {socket.gethostbyname(socket.gethostname())}")
    print(f"Platform: {platform.platform()}")
    print(f"Python Version: {platform.python_version()}")
    print(f"Process ID: {os.getpid()}")
    print("=" * 60)
    import time
    time.sleep(1)


def end_task():
    """Son task"""
    print("=" * 50)
    print("END TASK - Tüm görevler tamamlandı!")
    print("=" * 50)


with DAG(
    dag_id="test_printer_5min",
    start_date=datetime(2026, 8, 14),
    schedule="*/5 * * * *",  # 5 dakikada bir çalışır
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

    # 3 Tane Paralel Kubernetes Pod Operatörleri (Python simülasyonu)
    pod_1 = PythonOperator(
        task_id="kubernetes_pod_1",
        python_callable=pod_task,
        op_kwargs={"pod_id": "pod-1"},
    )

    pod_2 = PythonOperator(
        task_id="kubernetes_pod_2",
        python_callable=pod_task,
        op_kwargs={"pod_id": "pod-2"},
    )

    pod_3 = PythonOperator(
        task_id="kubernetes_pod_3",
        python_callable=pod_task,
        op_kwargs={"pod_id": "pod-3"},
    )

    # END TASK
    end = PythonOperator(
        task_id="end",
        python_callable=end_task,
    )

    # Bağlantılar
    start >> [part_1, part_2]  # start'tan part_1 ve part_2'ye paralel
    
    # part'lardan 3 pod'a paralel bağlantı
    for upstream in [part_1, part_2]:
        upstream >> [pod_1, pod_2, pod_3]
    
    [pod_1, pod_2, pod_3] >> end  # tüm pod'lardan end'e
