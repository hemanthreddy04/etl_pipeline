"""Alert delivery. A channel only delivers when it has been configured through environment variables."""
import smtplib
from email.message import EmailMessage

import requests

from .config import settings


def deliver(title, detail, ctl):
    """Send an alert to every channel the pipeline selected. Returns (sent, not_configured)."""
    sent, missing = [], []
    available = settings.channels()
    text = f"{title}\n{detail}"
    for ch in ("email", "chat", "pager"):
        if not ctl.get(ch):
            continue
        if not available[ch]:
            missing.append(ch)
            continue
        try:
            if ch == "chat":
                requests.post(settings.chat_webhook, json={"text": text}, timeout=15).raise_for_status()
            elif ch == "pager":
                requests.post(settings.pager_webhook, json={"summary": title, "detail": detail, "source": "medallion-control-plane"}, timeout=15).raise_for_status()
            else:
                msg = EmailMessage()
                msg["Subject"], msg["From"], msg["To"] = title, settings.smtp_from or settings.smtp_user, settings.alert_email_to
                msg.set_content(text)
                with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=20) as s:
                    s.starttls()
                    if settings.smtp_user:
                        s.login(settings.smtp_user, settings.smtp_password)
                    s.send_message(msg)
            sent.append(ch)
        except Exception as e:  # an alert that cannot be delivered must never break a run
            missing.append(f"{ch} ({e.__class__.__name__})")
    return sent, missing
