import random
from datetime import datetime

from airflow import DAG
from airflow.models.baseoperator import cross_downstream
from airflow.providers.standard.operators.python import PythonOperator


def start_task(ti):
    """İlk task: Hello World yazan Python kodu ve XCom'a veri push et"""
    print("=" * 50)
    print("START TASK")
    print("=" * 50)
    print("Hello World!")

    start_data = {
        "message": "Başlangıç mesajı",
        "timestamp": str(datetime.now()),
    }
    ti.xcom_push(key="start_message", value=start_data)
    print(f"✅ XCom'a push edildi: {start_data}")
    print("=" * 50)
    return start_data


def generate_random_task(task_name, ti):
    """Part 1 / Part 2: random sayı üretir.

    İki farklı yolla downstream'e veri aktarılır:
    1) ti.xcom_push ile manuel, özel key altında (klasik yöntem)
    2) fonksiyonun return ettiği değer -> Airflow bunu otomatik olarak
       'return_value' key'i ile XCom'a yazar. Bu return değerine
       downstream task'larda op_kwargs içinde part_1.output / part_2.output
       (XComArg) şeklinde doğrudan erişebiliriz — manuel xcom_pull yazmaya
       gerek kalmaz.
    """
    previous_data = ti.xcom_pull(task_ids="start", key="start_message")
    print(f"📥 Start task'tan alınan veri: {previous_data}")

    random_number = random.randint(1, 100)
    print(f"🎲 {task_name} üretilen random sayı: {random_number}")

    # Yol 1: manuel xcom_push (özel key ile)
    ti.xcom_push(key=f"{task_name}_random", value=random_number)

    # Yol 2: return değeri otomatik XCom'a ('return_value' key'i)
    return random_number


def pod_multiply_task(pod_id, multiplier, ti, val1=None, val2=None):
    """Kubernetes Pod'u simüle eden task.

    val1 ve val2, DAG tanımında op_kwargs içinde part_1.output / part_2.output
    (XComArg) olarak verildiği için Airflow bunları task çalışmadan önce
    otomatik çözüp doğrudan fonksiyona argüman olarak geçirir (Yol 2).
    Karşılaştırma amacıyla aynı veriler klasik ti.xcom_pull ile de okunuyor
    (Yol 1).
    """
    import os
    import platform
    import socket

    print("=" * 60)
    print(f"POD TASK - {pod_id}")
    print("=" * 60)

    # --- Yol 1: klasik xcom_pull ---
    part_1_random = ti.xcom_pull(task_ids="part_1", key="Part 1_random")
    part_2_random = ti.xcom_pull(task_ids="part_2", key="Part 2_random")
    print(f"📥 (xcom_pull) Part 1 random: {part_1_random}")
    print(f"📥 (xcom_pull) Part 2 random: {part_2_random}")

    # --- Yol 2: doğrudan op_kwargs / XComArg üzerinden gelen değerler ---
    print(f"📥 (op_kwargs/XComArg) val1: {val1}")
    print(f"📥 (op_kwargs/XComArg) val2: {val2}")

    v1 = val1 if val1 is not None else part_1_random
    v2 = val2 if val2 is not None else part_2_random
    result = (v1 + v2) * multiplier

    print(f"🧮 Hesaplama: ({v1} + {v2}) * {multiplier} = {result}")

    pod_info = {
        "hostname": socket.gethostname(),
        "pod_id": pod_id,
        "ip_address": socket.gethostbyname(socket.gethostname()),
        "platform": platform.platform(),
        "python_version": platform.python_version(),
        "process_id": os.getpid(),
        "part_1_random": v1,
        "part_2_random": v2,
        "multiplier": multiplier,
        "result": result,
    }

    ti.xcom_push(key=f"{pod_id}_info", value=pod_info)
    print(f"✅ XCom'a push edildi: {pod_info}")
    print("=" * 60)

    return result  # return_value XCom'u da otomatik push edilir, end task bunu op_kwargs ile alacak


def end_task(ti, pod_1_result=None, pod_2_result=None, pod_3_result=None):
    """Son task - hem xcom_pull hem op_kwargs/XComArg ile tüm pod sonuçlarını toplar."""
    print("=" * 60)
    print("END TASK - Tüm görevler tamamlandı!")
    print("=" * 60)

    # --- Yol 1: klasik xcom_pull ---
    pod_1_info = ti.xcom_pull(task_ids="kubernetes_pod_1", key="pod-1_info")
    pod_2_info = ti.xcom_pull(task_ids="kubernetes_pod_2", key="pod-2_info")
    pod_3_info = ti.xcom_pull(task_ids="kubernetes_pod_3", key="pod-3_info")

    print("\n📊 XCOM_PULL İLE ALINAN POD BİLGİLERİ:\n")
    print(f"Pod 1: {pod_1_info}")
    print(f"Pod 2: {pod_2_info}")
    print(f"Pod 3: {pod_3_info}")

    # --- Yol 2: op_kwargs / XComArg ile alınan return_value'lar ---
    print("\n📊 OP_KWARGS/XComArg İLE ALINAN SONUÇLAR:\n")
    print(f"Pod 1 result: {pod_1_result}")
    print(f"Pod 2 result: {pod_2_result}")
    print(f"Pod 3 result: {pod_3_result}")

    all_results = {
        "pod_1": pod_1_info,
        "pod_2": pod_2_info,
        "pod_3": pod_3_info,
        "pod_1_result": pod_1_result,
        "pod_2_result": pod_2_result,
        "pod_3_result": pod_3_result,
        "timestamp": str(datetime.now()),
    }

    ti.xcom_push(key="final_results", value=all_results)
    print("\n✅ Final sonuçlar XCom'a push edildi!")
    print("=" * 60)

    return all_results


with DAG(
    dag_id="test_printer_5min",
    start_date=datetime(2026, 8, 14),
    schedule="*/30 * * * *",  # 30 dakikada bir çalışır
    catchup=False,
    tags=["test", "kubernetes"],
) as dag:

    # START TASK
    start = PythonOperator(
        task_id="start",
        python_callable=start_task,
    )

    # PART 1 ve PART 2 - Paralel, random sayı üreten task'lar
    part_1 = PythonOperator(
        task_id="part_1",
        python_callable=generate_random_task,
        op_kwargs={"task_name": "Part 1"},
    )

    part_2 = PythonOperator(
        task_id="part_2",
        python_callable=generate_random_task,
        op_kwargs={"task_name": "Part 2"},
    )

    # 3 Paralel Kubernetes Pod task'ı - part_1 ve part_2'nin random sayılarını
    # doğrudan op_kwargs içinde XComArg (.output) olarak alıyor
    pod_1 = PythonOperator(
        task_id="kubernetes_pod_1",
        python_callable=pod_multiply_task,
        op_kwargs={
            "pod_id": "pod-1",
            "multiplier": 2,
            "val1": part_1.output,
            "val2": part_2.output,
        },
    )

    pod_2 = PythonOperator(
        task_id="kubernetes_pod_2",
        python_callable=pod_multiply_task,
        op_kwargs={
            "pod_id": "pod-2",
            "multiplier": 3,
            "val1": part_1.output,
            "val2": part_2.output,
        },
    )

    pod_3 = PythonOperator(
        task_id="kubernetes_pod_3",
        python_callable=pod_multiply_task,
        op_kwargs={
            "pod_id": "pod-3",
            "multiplier": 4,
            "val1": part_1.output,
            "val2": part_2.output,
        },
    )

    # END TASK - 3 pod'un sonuçlarını op_kwargs/XComArg ile direkt alıyor
    end = PythonOperator(
        task_id="end",
        python_callable=end_task,
        op_kwargs={
            "pod_1_result": pod_1.output,
            "pod_2_result": pod_2.output,
            "pod_3_result": pod_3.output,
        },
    )

    # Bağlantılar
    start >> [part_1, part_2]
    cross_downstream([part_1, part_2], [pod_1, pod_2, pod_3])
    [pod_1, pod_2, pod_3] >> end