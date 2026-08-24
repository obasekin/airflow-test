"""
Test DAG: smtp_default connection'ini okuyup gercek SMTP baglantisini
dener. Sertifika/hostname sorununu teshis etmek icin verbose loglar basar.

Kullanim:
1) Bu dosyayi dags/repo altina koyun.
2) Airflow UI -> DAGs -> test_smtp_default_conn -> Trigger.
3) Task log'unda ayrintili ciktiyi gorun (host, port, login, ssl_context,
   ve hangi asamada patladigi).
"""
from __future__ import annotations

import datetime
import smtplib
import ssl
from email.message import EmailMessage

from airflow.sdk import DAG, task  # Airflow 3.x SDK
from airflow.hooks.base import BaseHook

TEST_RECIPIENT = "obasekin@arcanor.com"  # ihtiyaca gore degistirin
CONN_ID = "smtp_default"


@task(task_id="test_smtp_default_connection")
def test_smtp_default_connection():
    conn = BaseHook.get_connection(CONN_ID)

    host = conn.host
    port = conn.port or 587
    login = conn.login
    password = conn.password
    extra = conn.extra_dejson or {}

    print("=== smtp_default connection bilgileri ===")
    print(f"Host: {host}")
    print(f"Port: {port}")
    print(f"Login: {login}")
    print(f"Extra: {extra}")

    disable_tls = extra.get("disable_tls", False)
    ssl_context_setting = extra.get("ssl_context")  # None | 'default' | 'none'
    disable_ssl = extra.get("disable_ssl", False)

    # Airflow SMTP provider mantigina benzer sekilde context sec
    if ssl_context_setting == "none":
        context = ssl._create_unverified_context()
        print("SSL context: DOGRULAMASIZ (unverified) kullaniliyor")
    else:
        context = ssl.create_default_context()
        print("SSL context: default (tam dogrulama) kullaniliyor")

    msg = EmailMessage()
    msg["Subject"] = "Airflow smtp_default test email"
    msg["From"] = login
    msg["To"] = TEST_RECIPIENT
    msg.set_content("Bu, smtp_default connection'i uzerinden gonderilen test mailidir.")

    server = None
    try:
        if port == 465:
            print(f"SMTP_SSL ile baglaniliyor: {host}:{port}")
            server = smtplib.SMTP_SSL(host, port, timeout=10, context=context)
        else:
            print(f"SMTP ile baglaniliyor: {host}:{port}")
            server = smtplib.SMTP(host, port, timeout=10)
            server.ehlo()
            if not disable_tls and not disable_ssl:
                print("STARTTLS baslatiliyor...")
                server.starttls(context=context)
                server.ehlo()
            else:
                print("TLS/SSL devre disi birakildi (extra'da disable_tls/disable_ssl)")

        if login:
            print("Authenticate ediliyor...")
            server.login(login, password)
            print("Authentication basarili.")

        server.send_message(msg)
        print(f"Test maili gonderildi -> {TEST_RECIPIENT}")

    except ssl.SSLCertVerificationError as e:
        print(f"SSL SERTIFIKA HATASI: {e}")
        raise
    except smtplib.SMTPAuthenticationError as e:
        print(f"AUTH HATASI: {e}")
        raise
    except smtplib.SMTPException as e:
        print(f"SMTP HATASI: {e}")
        raise
    except Exception as e:
        print(f"BEKLENMEYEN HATA: {type(e).__name__}: {e}")
        raise
    finally:
        if server is not None:
            try:
                server.quit()
            except Exception:
                pass


with DAG(
    dag_id="test_smtp_default_conn",
    schedule=None,
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    tags=["test", "smtp"],
) as dag:
    test_smtp_default_connection()