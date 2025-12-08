#!/usr/bin/env python3
"""Test email notification - standalone script."""

import os
import smtplib
from dataclasses import dataclass, field
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


@dataclass
class JobRunResult:
    """Simplified job result for testing."""

    job_name: str
    media_type: str
    status: str
    changes_found: int = 0
    documents_upserted: int = 0
    errors_count: int = 0
    duration_seconds: float | None = None


@dataclass
class ETLRunMetadata:
    """Simplified metadata for testing."""

    run_id: str
    run_date: str
    status: str
    duration_seconds: float | None = None
    total_changes_found: int = 0
    total_documents_upserted: int = 0
    total_errors: int = 0
    job_results: list[JobRunResult] = field(default_factory=list)


def format_duration(seconds: float | None) -> str:
    if seconds is None:
        return "N/A"
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    if minutes < 60:
        return f"{minutes}m {secs}s"
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours}h {mins}m {secs}s"


def format_number(n: int) -> str:
    return f"{n:,}"


def build_html(metadata: ETLRunMetadata) -> str:
    status_color = {
        "completed": "#22c55e",
        "partial": "#f59e0b",
        "failed": "#ef4444",
    }.get(metadata.status, "#6b7280")

    status_emoji = {"completed": "✅", "partial": "⚠️", "failed": "❌"}.get(
        metadata.status, "❓"
    )
    error_color = "#ef4444" if metadata.total_errors > 0 else "#22c55e"
    gcs_url = f"https://console.cloud.google.com/storage/browser/mc-redis-etl/redis-search/etl/runs/{metadata.run_date}"

    job_rows = ""
    for job in metadata.job_results:
        row_color = (
            "#22c55e"
            if job.status == "success"
            else "#ef4444" if job.status == "failed" else "#6b7280"
        )
        job_rows += (
            f"<tr>"
            f'<td style="padding:8px;border-bottom:1px solid #374151">{job.job_name}</td>'
            f'<td style="padding:8px;border-bottom:1px solid #374151;color:{row_color}">{job.status}</td>'
            f'<td style="padding:8px;border-bottom:1px solid #374151;text-align:right">{format_number(job.changes_found)}</td>'
            f'<td style="padding:8px;border-bottom:1px solid #374151;text-align:right">{format_number(job.documents_upserted)}</td>'
            f'<td style="padding:8px;border-bottom:1px solid #374151;text-align:right">{job.errors_count}</td>'
            f'<td style="padding:8px;border-bottom:1px solid #374151">{format_duration(job.duration_seconds)}</td>'
            f"</tr>"
        )

    return "".join([
        "<!DOCTYPE html>",
        '<html><head><meta charset="utf-8"><title>ETL Run Summary</title></head>',
        '<body style="font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:#111827;color:#f3f4f6;padding:20px;margin:0">',
        '<div style="max-width:700px;margin:0 auto;background:#1f2937;border-radius:8px;overflow:hidden;border:1px solid #374151">',
        f'<div style="background:{status_color};padding:20px;text-align:center">',
        f'<h1 style="margin:0;color:white;font-size:24px">{status_emoji} ETL Run {metadata.status.upper()}</h1>',
        f'<p style="margin:5px 0 0;color:rgba(255,255,255,0.9);font-size:14px">{metadata.run_date} • {metadata.run_id}</p>',
        "</div>",
        '<div style="padding:20px;display:flex;justify-content:space-around;background:#111827;border-bottom:1px solid #374151">',
        f'<div style="text-align:center"><div style="font-size:28px;font-weight:bold;color:#22c55e">{format_number(metadata.total_documents_upserted)}</div><div style="font-size:12px;color:#9ca3af">Documents Upserted</div></div>',
        f'<div style="text-align:center"><div style="font-size:28px;font-weight:bold;color:#3b82f6">{format_number(metadata.total_changes_found)}</div><div style="font-size:12px;color:#9ca3af">Changes Found</div></div>',
        f'<div style="text-align:center"><div style="font-size:28px;font-weight:bold;color:{error_color}">{format_number(metadata.total_errors)}</div><div style="font-size:12px;color:#9ca3af">Errors</div></div>',
        f'<div style="text-align:center"><div style="font-size:28px;font-weight:bold;color:#f3f4f6">{format_duration(metadata.duration_seconds)}</div><div style="font-size:12px;color:#9ca3af">Duration</div></div>',
        "</div>",
        '<div style="padding:20px">',
        '<h2 style="margin:0 0 15px;font-size:16px;color:#9ca3af">Job Breakdown</h2>',
        '<table style="width:100%;border-collapse:collapse;font-size:13px">',
        '<thead><tr style="background:#374151">',
        '<th style="padding:10px 8px;text-align:left">Job</th>',
        '<th style="padding:10px 8px;text-align:left">Status</th>',
        '<th style="padding:10px 8px;text-align:right">Changes</th>',
        '<th style="padding:10px 8px;text-align:right">Upserted</th>',
        '<th style="padding:10px 8px;text-align:right">Errors</th>',
        '<th style="padding:10px 8px;text-align:left">Duration</th>',
        "</tr></thead>",
        f"<tbody>{job_rows}</tbody>",
        "</table></div>",
        '<div style="padding:15px 20px;background:#111827;border-top:1px solid #374151;font-size:12px;color:#6b7280">',
        f'<p style="margin:0">Redis Search ETL • <a href="{gcs_url}" style="color:#60a5fa">View in GCS</a></p>',
        "</div>",
        "</div></body></html>",
    ])


def send_test_email(metadata: ETLRunMetadata) -> bool:
    recipient = os.getenv("ETL_NOTIFICATION_EMAIL")
    if not recipient:
        print("❌ ETL_NOTIFICATION_EMAIL not set")
        return False

    # Check for SendGrid first
    sendgrid_password = os.getenv("SENDGRID_PASSWORD")
    if sendgrid_password:
        smtp_server = os.getenv("SENDGRID_SERVER", "smtp.sendgrid.net")
        smtp_port = int(os.getenv("SENDGRID_PORT", "587"))
        smtp_user = os.getenv("SENDGRID_USERNAME", "apikey")
        smtp_password = sendgrid_password
        from_email = os.getenv("SENDGRID_FROM_EMAIL", recipient)
        use_tls = smtp_port == 587
        print(f"   Using SendGrid SMTP ({smtp_server}:{smtp_port})")
    else:
        smtp_server = "smtp.gmail.com"
        smtp_port = 465
        smtp_user = os.getenv("SMTP_USER")
        smtp_password = os.getenv("SMTP_APP_PASSWORD") or os.getenv("SMTP_PASSWORD")
        from_email = smtp_user
        use_tls = False
        print("   Using Gmail SMTP")

        if not smtp_user or not smtp_password:
            print("❌ Missing SMTP credentials")
            return False

    status_prefix = {"completed": "✅", "partial": "⚠️", "failed": "❌"}.get(
        metadata.status, ""
    )
    subject = (
        f"{status_prefix} ETL {metadata.status.upper()}: "
        f"{format_number(metadata.total_documents_upserted)} docs, "
        f"{metadata.total_errors} errors ({metadata.run_date})"
    )

    msg = MIMEMultipart("alternative")
    msg["From"] = from_email
    msg["To"] = recipient
    msg["Subject"] = subject
    msg.attach(MIMEText(build_html(metadata), "html", "utf-8"))

    try:
        print(f"   Connecting to {smtp_server}:{smtp_port}...")
        if use_tls:
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                print("   Authenticating...")
                server.login(smtp_user, smtp_password)
                print(f"   Sending to {recipient}...")
                server.sendmail(from_email, recipient, msg.as_string())
        else:
            with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
                print("   Authenticating...")
                server.login(smtp_user, smtp_password)
                print(f"   Sending to {recipient}...")
                server.sendmail(from_email, recipient, msg.as_string())
        return True
    except Exception as e:
        print(f"❌ SMTP error: {e}")
        return False


if __name__ == "__main__":
    print("=" * 50)
    print("ETL Email Notification Test")
    print("=" * 50)
    print()
    print("Environment variables:")
    print(f"  ETL_NOTIFICATION_EMAIL: {os.getenv('ETL_NOTIFICATION_EMAIL', 'NOT SET')}")
    if os.getenv("SENDGRID_PASSWORD"):
        print(f"  SENDGRID_SERVER: {os.getenv('SENDGRID_SERVER', 'smtp.sendgrid.net')}")
        print(f"  SENDGRID_PORT: {os.getenv('SENDGRID_PORT', '587')}")
        print(f"  SENDGRID_USERNAME: {os.getenv('SENDGRID_USERNAME', 'apikey')}")
        print(f"  SENDGRID_PASSWORD: {'*' * 10}...{os.getenv('SENDGRID_PASSWORD', '')[-4:]}")
    else:
        print(f"  SMTP_USER: {os.getenv('SMTP_USER', 'NOT SET')}")
        pw = os.getenv("SMTP_PASSWORD", "")
        print(f"  SMTP_PASSWORD: {'*' * len(pw) if pw else 'NOT SET'}")
    print()

    metadata = ETLRunMetadata(
        run_id="TEST_20251208_070000",
        run_date="2025-12-08",
        status="completed",
        duration_seconds=1847.5,
        total_changes_found=2446,
        total_documents_upserted=127,
        total_errors=5,
        job_results=[
            JobRunResult(
                job_name="tmdb_tv_changes",
                media_type="tv",
                status="success",
                duration_seconds=612.3,
                changes_found=892,
                documents_upserted=45,
                errors_count=2,
            ),
            JobRunResult(
                job_name="tmdb_movie_changes",
                media_type="movie",
                status="success",
                duration_seconds=534.1,
                changes_found=1204,
                documents_upserted=67,
                errors_count=3,
            ),
            JobRunResult(
                job_name="tmdb_person_changes",
                media_type="person",
                status="success",
                duration_seconds=701.1,
                changes_found=350,
                documents_upserted=15,
                errors_count=0,
            ),
        ],
    )

    print("Sending test email...")
    if send_test_email(metadata):
        print()
        print("✅ Test email sent successfully!")
        print("   Check your inbox for the ETL summary.")
    else:
        print()
        print("❌ Failed to send test email")
