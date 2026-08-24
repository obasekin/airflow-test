"""
Test DAG (v2 - duzeltilmis): smtp_default connection'ini okuyup, gercek
Airflow SMTP provider hook'unun (airflow/providers/smtp/hooks/smtp.py)
mantigini BIREBIR taklit ederek baglanmayi dener.

Onceki versiyondaki hata: disable_ssl=True oldugunda STARTTLS'i tamamen
atlamistik - bu yuzden "basarili" gorunmustu ama aslinda gercek hook
davranisini yansitmiyordu. Gercek hook, disable_tls=False oldugu surece
HER ZAMAN starttls cagirir (disable_ssl sadece port 465'te SMTP_SSL
sinifi kullanilip kullanilmayacagiyla ilgilidir, port 587/STARTTLS akisini
etkilemez).

Ayrica bu connection auth_type=oauth2 kullaniyor - yani sifre degil,
access_token ile XOAUTH2 mekanizmasi calisiyor. Bu da ayri bir fark noktasi.

Bu DAG once "as-is" (gercek hatayi reprodukte etmek icin) dener, sonra
ssl_context override (unverified) ile dener - boylece cozumun gercekten
ise yarayip yaramadigini ayni kosullarda test edebilirsiniz.
"""
from __future__ import annotations

import base64
import datetime
import smtplib
import ssl
from email.message import EmailMessage

from airflow.sdk import DAG, task
from airflow.hooks.base import BaseHook

TEST_RECIPIENT = "obasekin@arcanor.com"  # ihtiyaca gore degistirin
CONN_ID = "smtp_default"


def _build_context(verify: bool) -> ssl.SSLContext:
    if verify:
        return ssl.create_default_context()
    ctx = ssl._create_unverified_context()
    return ctx


def _try_send(conn, verify_ssl: bool, label: str):
    host = conn.host
    port = conn.port or 587
    login = conn.login
    extra = conn.extra_dejson or {}

    disable_tls = extra.get("disable_tls", False)
    auth_type = extra.get("auth_type")
    access_token = extra.get("access_token")

    print(f"\n=== Deneme: {label} (verify_ssl={verify_ssl}) ===")
    print(f"Host: {host}  Port: {port}  Login: {login}  auth_type: {auth_type}")
    print(f"disable_tls: {disable_tls}")

    msg = EmailMessage()
    msg["Subject"] = f"Airflow smtp_default test email ({label})"
    msg["From"] = extra.get("from_email") or login
    msg["To"] = TEST_RECIPIENT
    msg.set_content(f"Bu, smtp_default connection'i uzerinden gonderilen test mailidir. Mod: {label}")

    server = None
    try:
        print(f"SMTP ile baglaniliyor: {host}:{port}")
        server = smtplib.SMTP(host, port, timeout=extra.get("timeout", 30) or 30)
        server.ehlo()

        if not disable_tls:
            print("STARTTLS baslatiliyor... (gercek hook default'ta bunu yapar)")
            context = _build_context(verify_ssl)
            server.starttls(context=context)
            server.ehlo()
            print("STARTTLS basarili.")
        else:
            print("disable_tls=True -> STARTTLS atlaniyor")

        # Auth
        if auth_type == "oauth2" and access_token:
            print("XOAUTH2 ile authenticate ediliyor...")
            auth_string = f"user={login}\x01auth=Bearer {access_token}\x01\x01"
            code, resp = server.docmd(
                "AUTH", "XOAUTH2 " + base64.b64encode(auth_string.encode()).decode()
            )
            print(f"AUTH XOAUTH2 sonucu: {code} {resp}")
            if code not in (235, 503):
                raise smtplib.SMTPAuthenticationError(code, resp)
        elif login:
            print("Standart login ile authenticate ediliyor...")
            server.login(login, conn.password)
            print("Authentication basarili.")

        server.send_message(msg)
        print(f"BASARILI: Test maili gonderildi -> {TEST_RECIPIENT} [{label}]")
        return True

    except ssl.SSLCertVerificationError as e:
        print(f"SSL SERTIFIKA HATASI [{label}]: {e}")
    except smtplib.SMTPAuthenticationError as e:
        print(f"AUTH HATASI [{label}]: {e}")
    except smtplib.SMTPException as e:
        print(f"SMTP HATASI [{label}]: {e}")
    except Exception as e:
        print(f"BEKLENMEYEN HATA [{label}]: {type(e).__name__}: {e}")
    finally:
        if server is not None:
            try:
                server.quit()
            except Exception:
                pass
    return False


@task(task_id="test_smtp_default_connection_v2")
def test_smtp_default_connection_v2():
    conn = BaseHook.get_connection(CONN_ID)

    print("=== smtp_default connection bilgileri ===")
    print(f"Host: {conn.host}")
    print(f"Port: {conn.port}")
    print(f"Login: {conn.login}")
    print(f"Extra: {conn.extra_dejson}")

    # 1) Gercek production davranisini reprodukte et (tam dogrulamali TLS)
    ok = _try_send(conn, verify_ssl=True, label="verified-ssl (production ile ayni)")

    if not ok:
        # 2) Cozum adayi: dogrulamasiz context
        print("\n>>> Verified SSL basarisiz oldu, unverified context ile deneniyor...")
        ok2 = _try_send(conn, verify_ssl=False, label="unverified-ssl (olasi cozum)")
        if ok2:
            print("\nSONUC: Sertifika dogrulamasi kapatilinca calisiyor -> "
                  "connection Extra'sina {'ssl_context': 'none'} eklenmeli.")
        else:
            print("\nSONUC: Unverified context ile de calismadi -> "
                  "sorun sertifikadan farkli bir sey olabilir (auth, network, vs).")
    else:
        print("\nSONUC: Verified SSL ile calisti -> sertifika sorunu yok, "
              "baska bir seyle karisiyor olabilirsiniz.")


with DAG(
    dag_id="test_smtp_default_conn_v2",
    schedule=None,
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    tags=["test", "smtp"],
) as dag:
    test_smtp_default_connection_v2()