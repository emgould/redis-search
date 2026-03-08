"""
Email alerting utility for critical function failures.

Environment variables (matches redis-search pattern):
    WARMUP_NOTIFICATION_EMAIL: Recipient email address
    SENDGRID_PASSWORD: SendGrid API key
    SENDGRID_SERVER: SMTP server (default: smtp.sendgrid.net)
    SENDGRID_PORT: SMTP port (default: 587)
    SENDGRID_USERNAME: SMTP user (default: apikey)
    SENDGRID_FROM_EMAIL: From address (default: recipient)
    -- OR --
    SMTP_USER: Gmail address
    SMTP_APP_PASSWORD: Gmail app password

Usage:
    from utils.alerts import send_alert
    send_alert(
        subject="Warmup Failed",
        body="get_trending warmup failed after 30 retries",
        severity="critical"
    )
"""

import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from utils.get_logger import get_logger

logger = get_logger(__name__)

# Severity colors for HTML emails
SEVERITY_COLORS = {
    "critical": "#ef4444",  # Red
    "warning": "#f59e0b",  # Orange
    "info": "#3b82f6",  # Blue
}

SEVERITY_EMOJI = {
    "critical": "🚨",
    "warning": "⚠️",
    "info": "ℹ️",
}


def _build_html(subject: str, body: str, severity: str, metadata: dict | None = None) -> str:
    """Build HTML email body."""
    color = SEVERITY_COLORS.get(severity, "#6b7280")
    emoji = SEVERITY_EMOJI.get(severity, "")

    metadata_rows = ""
    if metadata:
        for key, value in metadata.items():
            metadata_rows += (
                f"<tr>"
                f'<td style="padding:8px;border-bottom:1px solid #374151;color:#9ca3af">{key}</td>'
                f'<td style="padding:8px;border-bottom:1px solid #374151">{value}</td>'
                f"</tr>"
            )

    metadata_section = ""
    if metadata_rows:
        metadata_section = f"""
        <div style="padding:20px">
            <h2 style="margin:0 0 15px;font-size:14px;color:#9ca3af">Details</h2>
            <table style="width:100%;border-collapse:collapse;font-size:13px">
                <tbody>{metadata_rows}</tbody>
            </table>
        </div>
        """

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>{subject}</title></head>
<body style="font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:#111827;color:#f3f4f6;padding:20px;margin:0">
<div style="max-width:600px;margin:0 auto;background:#1f2937;border-radius:8px;overflow:hidden;border:1px solid #374151">
    <div style="background:{color};padding:20px;text-align:center">
        <h1 style="margin:0;color:white;font-size:20px">{emoji} {subject}</h1>
    </div>
    <div style="padding:20px">
        <p style="margin:0;font-size:14px;line-height:1.6;white-space:pre-wrap">{body}</p>
    </div>
    {metadata_section}
    <div style="padding:15px 20px;background:#111827;border-top:1px solid #374151;font-size:11px;color:#6b7280">
        <p style="margin:0">MediaCircle Firebase Functions Alert</p>
    </div>
</div>
</body></html>"""


def send_alert(
    subject: str,
    body: str,
    severity: str = "warning",
    metadata: dict | None = None,
) -> bool:
    """
    Send an email alert for critical function failures.

    Args:
        subject: Email subject line
        body: Email body text
        severity: "critical", "warning", or "info"
        metadata: Optional dict of key-value pairs to include in email

    Returns:
        True if email sent successfully, False otherwise
    """
    recipient = os.getenv("WARMUP_NOTIFICATION_EMAIL")
    if not recipient:
        logger.warning("⚠️ WARMUP_NOTIFICATION_EMAIL not set - cannot send alert email")
        return False

    # Check for SendGrid first (matches redis-search pattern)
    sendgrid_password = os.getenv("SENDGRID_PASSWORD")
    if sendgrid_password:
        sg_server = os.getenv("SENDGRID_SERVER", "smtp.sendgrid.net")
        sg_port = int(os.getenv("SENDGRID_PORT", "587"))
        sg_user = os.getenv("SENDGRID_USERNAME", "apikey")
        sg_from = os.getenv("SENDGRID_FROM_EMAIL", recipient)
        return _send_via_sendgrid(
            subject,
            body,
            severity,
            metadata,
            recipient,
            sg_server,
            sg_port,
            sg_user,
            sendgrid_password,
            sg_from,
        )

    # Fallback to Gmail SMTP (matches redis-search pattern)
    gmail_user = os.getenv("SMTP_USER")
    gmail_password = os.getenv("SMTP_APP_PASSWORD") or os.getenv("SMTP_PASSWORD")
    if gmail_user and gmail_password:
        return _send_via_smtp(
            subject, body, severity, metadata, recipient, gmail_user, gmail_password
        )

    logger.warning(
        "⚠️ No email credentials configured (SENDGRID_PASSWORD or SMTP_USER/SMTP_APP_PASSWORD)"
    )
    return False


def _send_via_sendgrid(
    subject: str,
    body: str,
    severity: str,
    metadata: dict | None,
    recipient: str,
    smtp_server: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str,
    from_email: str,
) -> bool:
    """Send email via SendGrid SMTP relay."""
    try:
        emoji = SEVERITY_EMOJI.get(severity, "")
        full_subject = f"{emoji} [{severity.upper()}] {subject}"

        msg = MIMEMultipart("alternative")
        msg["From"] = from_email
        msg["To"] = recipient
        msg["Subject"] = full_subject
        msg.attach(MIMEText(_build_html(subject, body, severity, metadata), "html", "utf-8"))

        use_tls = smtp_port == 587
        if use_tls:
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(smtp_user, smtp_password)
                server.sendmail(from_email, recipient, msg.as_string())
        else:
            with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
                server.login(smtp_user, smtp_password)
                server.sendmail(from_email, recipient, msg.as_string())

        logger.info(f"✅ Alert email sent via SendGrid: {subject}")
        return True

    except Exception as e:
        logger.error(f"❌ Failed to send alert via SendGrid: {e}")
        return False


def _send_via_smtp(
    subject: str,
    body: str,
    severity: str,
    metadata: dict | None,
    recipient: str,
    smtp_user: str,
    smtp_password: str,
) -> bool:
    """Send email via Gmail SMTP."""
    try:
        smtp_server = "smtp.gmail.com"
        smtp_port = 465

        emoji = SEVERITY_EMOJI.get(severity, "")
        full_subject = f"{emoji} [{severity.upper()}] {subject}"

        msg = MIMEMultipart("alternative")
        msg["From"] = smtp_user
        msg["To"] = recipient
        msg["Subject"] = full_subject
        msg.attach(MIMEText(_build_html(subject, body, severity, metadata), "html", "utf-8"))

        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            server.login(smtp_user, smtp_password)
            server.sendmail(smtp_user, recipient, msg.as_string())

        logger.info(f"✅ Alert email sent via Gmail: {subject}")
        return True

    except Exception as e:
        logger.error(f"❌ Failed to send alert via Gmail: {e}")
        return False
