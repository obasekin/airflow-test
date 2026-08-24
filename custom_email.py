from __future__ import annotations

import ssl
import smtplib

from email.message import EmailMessage

from airflow.hooks.base import BaseHook


def send_email_smtp(
    to,
    subject,
    html_content,
    files=None,
    dryrun=False,
    cc=None,
    bcc=None,
    mime_subtype="mixed",
    mime_charset="utf-8",
    conn_id="smtp_default",
    from_email=None,
    custom_headers=None,
    **kwargs,
):

    conn = BaseHook.get_connection(conn_id)

    host = conn.host
    port = conn.port or 587
    login = conn.login
    password = conn.password

    extra = conn.extra_dejson or {}

    disable_tls = extra.get("disable_tls", False)
    auth_type = extra.get("auth_type")
    access_token = extra.get("access_token")

    timeout = extra.get("timeout", 30) or 30

    mail_from = (
        from_email
        or extra.get("from_email")
        or login
    )

    print(
        f"[CUSTOM SMTP] "
        f"host={host} "
        f"port={port} "
        f"login={login} "
        f"auth_type={auth_type} "
        f"disable_tls={disable_tls}"
    )

    msg = EmailMessage()

    msg["Subject"] = subject
    msg["From"] = mail_from
    msg["To"] = ", ".join(to) if isinstance(to, (list, tuple)) else to

    if cc:
        msg["Cc"] = ", ".join(cc) if isinstance(cc, (list, tuple)) else cc

    if bcc:
        msg["Bcc"] = ", ".join(bcc) if isinstance(bcc, (list, tuple)) else bcc

    if custom_headers:
        for key, value in custom_headers.items():
            msg[key] = value

    msg.set_content(html_content, subtype="html")

    if dryrun:
        print("[CUSTOM SMTP] dryrun=True -> mail gönderilmedi")
        return

    server = None

    try:

        print(f"[CUSTOM SMTP] Connecting {host}:{port}")

        server = smtplib.SMTP(
            host,
            port,
            timeout=timeout,
        )

        server.ehlo()

        if not disable_tls:

            print("[CUSTOM SMTP] STARTTLS")

            # BURASI KRİTİK
            context = ssl._create_unverified_context()

            server.starttls(
                context=context
            )

            server.ehlo()

        # OAuth2
        if auth_type == "oauth2" and access_token:

            print("[CUSTOM SMTP] XOAUTH2 authentication")

            import base64

            auth_string = (
                f"user={login}\x01"
                f"auth=Bearer {access_token}\x01\x01"
            )

            encoded = base64.b64encode(
                auth_string.encode()
            ).decode()

            code, response = server.docmd(
                "AUTH",
                "XOAUTH2 " + encoded,
            )

            print(
                f"[CUSTOM SMTP] XOAUTH2 -> "
                f"{code} {response}"
            )

            if code not in (235, 503):
                raise smtplib.SMTPAuthenticationError(
                    code,
                    response,
                )

        elif login and password:

            print("[CUSTOM SMTP] LOGIN authentication")

            server.login(
                login,
                password,
            )

        server.send_message(msg)

        print(
            f"[CUSTOM SMTP] "
            f"Mail successfully sent -> {msg['To']}"
        )

    finally:

        if server is not None:

            try:
                server.quit()
            except Exception:
                pass