"""
ETL Notifications - Send email summaries of ETL runs.

This module provides email notification functionality for ETL runs,
sending detailed summaries including job results, document counts,
and error information.
"""

import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import TYPE_CHECKING

from utils.get_logger import get_logger

if TYPE_CHECKING:
    from etl.etl_metadata import ETLRunMetadata

logger = get_logger(__name__)


def format_duration(seconds: float | None) -> str:
    """Format duration in human-readable format."""
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
    """Format number with commas."""
    return f"{n:,}"


def build_summary_text(metadata: "ETLRunMetadata") -> str:
    """Build plain text email body with ETL run summary."""
    status_emoji = {
        "completed": "✅",
        "partial": "⚠️",
        "failed": "❌",
        "running": "🔄",
    }.get(metadata.status, "❓")

    lines = [
        f"{'=' * 60}",
        f"  Redis Search ETL Run Summary - {metadata.run_date}",
        f"{'=' * 60}",
        "",
        f"  Status: {status_emoji} {metadata.status.upper()}",
        f"  Run ID: {metadata.run_id}",
        f"  Duration: {format_duration(metadata.duration_seconds)}",
        "",
        f"{'─' * 60}",
        "  JOB RESULTS",
        f"{'─' * 60}",
        f"  ✓ Completed: {metadata.jobs_completed}",
        f"  ✗ Failed:    {metadata.jobs_failed}",
        f"  ○ Skipped:   {metadata.jobs_skipped}",
        "",
        f"{'─' * 60}",
        "  DOCUMENT STATS",
        f"{'─' * 60}",
        f"  Changes Found:      {format_number(metadata.total_changes_found)}",
        f"  Documents Upserted: {format_number(metadata.total_documents_upserted)}",
        f"  Total Errors:       {format_number(metadata.total_errors)}",
        "",
        f"{'─' * 60}",
        "  MEDIA MANAGER",
        f"{'─' * 60}",
        f"  Docs Sent:          {format_number(metadata.total_mm_docs_sent)}",
        "",
    ]

    # Add per-job breakdown
    if metadata.job_results:
        lines.extend(
            [
                f"{'─' * 60}",
                "  JOB BREAKDOWN",
                f"{'─' * 60}",
            ]
        )
        for job in metadata.job_results:
            status_icon = "✓" if job.status == "success" else "✗" if job.status == "failed" else "○"
            lines.append(f"  {status_icon} {job.job_name}")
            detail = (
                f"      Changes: {format_number(job.changes_found)} → "
                f"Upserted: {format_number(job.documents_upserted)} | "
                f"Errors: {job.errors_count} | "
                f"Duration: {format_duration(job.duration_seconds)}"
            )
            if job.mm_docs_sent > 0:
                detail += f" | MM Sent: {format_number(job.mm_docs_sent)}"
            lines.append(detail)
            if job.error_message:
                lines.append(f"      Error: {job.error_message[:100]}")
        lines.append("")

    # Add Media Manager pipeline summary
    if metadata.mm_health_check is not None:
        lines.extend(
            [
                f"{'─' * 60}",
                "  MEDIA MANAGER PIPELINE",
                f"{'─' * 60}",
                f"  Health Check:     {'✓ ok' if metadata.mm_health_check == 'ok' else '✗ ' + (metadata.mm_health_check or 'N/A')}",
                f"  Docs Sent:        {format_number(metadata.total_mm_docs_sent)}",
            ]
        )
        if metadata.mm_queue_drained is not None:
            drain_status = "✓ drained" if metadata.mm_queue_drained else f"✗ {metadata.mm_queue_drain_error or 'failed'}"
            lines.append(f"  Queue Drain:      {drain_status}")
        if metadata.mm_indexes_rebuilt:
            for idx in metadata.mm_indexes_rebuilt:
                lines.append(
                    f"  Index Rebuilt:     {idx['index_name']} → "
                    f"{format_number(idx['total_documents'])} docs in {idx['duration_seconds']:.1f}s"
                )
        for err in metadata.mm_rebuild_errors:
            lines.append(f"  Rebuild Error:    ✗ {err[:80]}")
        if metadata.mm_finalize_publish:
            fp = metadata.mm_finalize_publish
            lines.append(
                f"  Finalize Publish: ✓ movies +{fp.get('movies_added', 0)}/~{fp.get('movies_updated', 0)}, "
                f"tv +{fp.get('tv_added', 0)}/~{fp.get('tv_updated', 0)}, "
                f"recycled={fp.get('readers_recycled', 0)}"
            )
        if metadata.mm_finalize_error:
            lines.append(f"  Finalize Error:   ✗ {metadata.mm_finalize_error[:80]}")
        lines.append("")

    # Add errors summary if any
    if metadata.total_errors > 0:
        lines.extend(
            [
                f"{'─' * 60}",
                "  ERRORS (first 10)",
                f"{'─' * 60}",
            ]
        )
        error_count = 0
        for job in metadata.job_results:
            for error in job.errors[:5]:  # Max 5 per job
                if error_count >= 10:
                    break
                lines.append(f"  • {error[:80]}...")
                error_count += 1
            if error_count >= 10:
                break
        if metadata.total_errors > 10:
            lines.append(f"  ... and {metadata.total_errors - 10} more errors")
        lines.append("")

    # Footer
    lines.extend(
        [
            f"{'─' * 60}",
            "  LINKS",
            f"{'─' * 60}",
            f"  GCS Metadata: gs://mc-redis-etl/redis-search/etl/runs/{metadata.run_date}/",
            "",
            f"{'=' * 60}",
            "  This is an automated message from the Redis Search ETL",
            f"{'=' * 60}",
        ]
    )

    return "\n".join(lines)


def _mm_step_html(ok: bool | None, label: str, detail: str = "") -> str:
    """Render a single MM pipeline step as an HTML badge."""
    if ok is True:
        return (
            f'<span style="display:inline-block;padding:4px 10px;border-radius:4px;'
            f'font-size:12px;font-weight:600;background:rgba(34,197,94,0.2);color:#4ade80">'
            f'{label} ✓</span>'
            f'{f" <span style=&quot;font-size:11px;color:#9ca3af&quot;>{detail}</span>" if detail else ""}'
        )
    if ok is False:
        return (
            f'<span style="display:inline-block;padding:4px 10px;border-radius:4px;'
            f'font-size:12px;font-weight:600;background:rgba(239,68,68,0.2);color:#f87171">'
            f'{label} ✗</span>'
            f'{f" <span style=&quot;font-size:11px;color:#f87171&quot;>{detail}</span>" if detail else ""}'
        )
    return (
        f'<span style="display:inline-block;padding:4px 10px;border-radius:4px;'
        f'font-size:12px;font-weight:600;background:rgba(107,114,128,0.2);color:#6b7280">'
        f'{label} —</span>'
    )


def _build_mm_pipeline_html(metadata: "ETLRunMetadata") -> str:
    """Build HTML for the Media Manager pipeline status section."""
    health_ok = metadata.mm_health_check == "ok"
    has_sent = metadata.total_mm_docs_sent > 0

    steps = [_mm_step_html(health_ok, "Health")]
    steps.append(_mm_step_html(has_sent if has_sent else None, f"Sent {format_number(metadata.total_mm_docs_sent)}"))

    if metadata.mm_queue_drained is not None:
        steps.append(_mm_step_html(metadata.mm_queue_drained, "Queue Drain"))

    rebuild_ok = len(metadata.mm_indexes_rebuilt) > 0 and len(metadata.mm_rebuild_errors) == 0
    has_rebuild = len(metadata.mm_indexes_rebuilt) > 0 or len(metadata.mm_rebuild_errors) > 0
    if has_rebuild:
        steps.append(_mm_step_html(rebuild_ok, "Rebuild"))

    finalize_ok = metadata.mm_finalize_publish is not None and "status" in metadata.mm_finalize_publish
    has_finalize = metadata.mm_finalize_publish is not None or metadata.mm_finalize_error is not None
    if has_finalize:
        steps.append(_mm_step_html(finalize_ok if not metadata.mm_finalize_error else False, "Finalize"))

    steps_html = " &nbsp; ".join(steps)

    detail_rows: list[str] = []
    for idx_info in metadata.mm_indexes_rebuilt:
        detail_rows.append(
            f'<tr><td style="padding:4px 8px;color:#9ca3af;font-size:12px">Rebuilt {idx_info["index_name"]}</td>'
            f'<td style="padding:4px 8px;font-size:12px">{format_number(idx_info["total_documents"])} docs in {idx_info["duration_seconds"]:.1f}s</td></tr>'
        )
    for err in metadata.mm_rebuild_errors:
        detail_rows.append(
            f'<tr><td style="padding:4px 8px;color:#f87171;font-size:12px">Rebuild Error</td>'
            f'<td style="padding:4px 8px;color:#f87171;font-size:12px">{err[:80]}</td></tr>'
        )
    if metadata.mm_finalize_publish:
        fp = metadata.mm_finalize_publish
        detail_rows.append(
            f'<tr><td style="padding:4px 8px;color:#9ca3af;font-size:12px">Finalize Publish</td>'
            f'<td style="padding:4px 8px;font-size:12px">'
            f'movies +{fp.get("movies_added", 0)}/~{fp.get("movies_updated", 0)}, '
            f'tv +{fp.get("tv_added", 0)}/~{fp.get("tv_updated", 0)}, '
            f'recycled={fp.get("readers_recycled", 0)}</td></tr>'
        )
    if metadata.mm_finalize_error:
        detail_rows.append(
            f'<tr><td style="padding:4px 8px;color:#f87171;font-size:12px">Finalize Error</td>'
            f'<td style="padding:4px 8px;color:#f87171;font-size:12px">{metadata.mm_finalize_error[:80]}</td></tr>'
        )

    details_table = ""
    if detail_rows:
        details_table = (
            '<table style="width:100%;border-collapse:collapse;margin-top:10px">'
            f'<tbody>{"".join(detail_rows)}</tbody></table>'
        )

    return (
        '<div style="padding:20px;border-top:1px solid #374151">'
        '<h2 style="margin:0 0 12px;font-size:16px;color:#9ca3af">Media Manager Pipeline</h2>'
        f'<div style="display:flex;flex-wrap:wrap;gap:8px">{steps_html}</div>'
        f'{details_table}'
        '</div>'
    )


def build_summary_html(metadata: "ETLRunMetadata") -> str:
    """Build HTML email body with ETL run summary."""
    status_color = {
        "completed": "#22c55e",
        "partial": "#f59e0b",
        "failed": "#ef4444",
        "running": "#3b82f6",
    }.get(metadata.status, "#6b7280")

    status_emoji = {
        "completed": "✅",
        "partial": "⚠️",
        "failed": "❌",
        "running": "🔄",
    }.get(metadata.status, "❓")

    error_color = "#ef4444" if metadata.total_errors > 0 else "#22c55e"
    gcs_url = f"https://console.cloud.google.com/storage/browser/mc-redis-etl/redis-search/etl/runs/{metadata.run_date}"

    # Build job rows
    job_rows_list = []
    for job in metadata.job_results:
        row_color = (
            "#22c55e"
            if job.status == "success"
            else "#ef4444"
            if job.status == "failed"
            else "#6b7280"
        )
        mm_sent_display = format_number(job.mm_docs_sent) if job.mm_docs_sent > 0 else "—"
        job_rows_list.append(
            f"<tr>"
            f'<td style="padding:8px;border-bottom:1px solid #374151">{job.job_name}</td>'
            f'<td style="padding:8px;border-bottom:1px solid #374151;color:{row_color}">{job.status}</td>'
            f'<td style="padding:8px;border-bottom:1px solid #374151;text-align:right">{format_number(job.changes_found)}</td>'
            f'<td style="padding:8px;border-bottom:1px solid #374151;text-align:right">{format_number(job.documents_upserted)}</td>'
            f'<td style="padding:8px;border-bottom:1px solid #374151;text-align:right">{job.errors_count}</td>'
            f'<td style="padding:8px;border-bottom:1px solid #374151;text-align:right">{mm_sent_display}</td>'
            f'<td style="padding:8px;border-bottom:1px solid #374151">{format_duration(job.duration_seconds)}</td>'
            f"</tr>"
        )
    job_rows = "".join(job_rows_list)

    # Build HTML using list to avoid trailing whitespace issues
    html_parts = [
        "<!DOCTYPE html>",
        '<html><head><meta charset="utf-8"><title>Redis Search ETL</title></head>',
        '<body style="font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:#111827;color:#f3f4f6;padding:20px;margin:0">',
        '<div style="max-width:700px;margin:0 auto;background:#1f2937;border-radius:8px;overflow:hidden;border:1px solid #374151">',
        # Header
        f'<div style="background:{status_color};padding:20px;text-align:center">',
        f'<h1 style="margin:0;color:white;font-size:24px">{status_emoji} Redis Search ETL Run {metadata.status.upper()}</h1>',
        f'<p style="margin:5px 0 0;color:rgba(255,255,255,0.9);font-size:14px">{metadata.run_date} • {metadata.run_id}</p>',
        "</div>",
        # Summary Stats
        '<div style="padding:20px;display:flex;justify-content:space-around;background:#111827;border-bottom:1px solid #374151">',
        f'<div style="text-align:center"><div style="font-size:28px;font-weight:bold;color:#22c55e">{format_number(metadata.total_documents_upserted)}</div><div style="font-size:12px;color:#9ca3af">Documents Upserted</div></div>',
        f'<div style="text-align:center"><div style="font-size:28px;font-weight:bold;color:#3b82f6">{format_number(metadata.total_changes_found)}</div><div style="font-size:12px;color:#9ca3af">Changes Found</div></div>',
        f'<div style="text-align:center"><div style="font-size:28px;font-weight:bold;color:{error_color}">{format_number(metadata.total_errors)}</div><div style="font-size:12px;color:#9ca3af">Errors</div></div>',
        f'<div style="text-align:center"><div style="font-size:28px;font-weight:bold;color:#a78bfa">{format_number(metadata.total_mm_docs_sent)}</div><div style="font-size:12px;color:#9ca3af">MM Docs Sent</div></div>',
        f'<div style="text-align:center"><div style="font-size:28px;font-weight:bold;color:#f3f4f6">{format_duration(metadata.duration_seconds)}</div><div style="font-size:12px;color:#9ca3af">Duration</div></div>',
        "</div>",
        # Job Details Table
        '<div style="padding:20px">',
        '<h2 style="margin:0 0 15px;font-size:16px;color:#9ca3af">Job Breakdown</h2>',
        '<table style="width:100%;border-collapse:collapse;font-size:13px">',
        '<thead><tr style="background:#374151">',
        '<th style="padding:10px 8px;text-align:left">Job</th>',
        '<th style="padding:10px 8px;text-align:left">Status</th>',
        '<th style="padding:10px 8px;text-align:right">Changes</th>',
        '<th style="padding:10px 8px;text-align:right">Upserted</th>',
        '<th style="padding:10px 8px;text-align:right">Errors</th>',
        '<th style="padding:10px 8px;text-align:right">MM Sent</th>',
        '<th style="padding:10px 8px;text-align:left">Duration</th>',
        "</tr></thead>",
        f"<tbody>{job_rows}</tbody>",
        "</table></div>",
    ]

    # MM Pipeline section (only if MM was configured)
    if metadata.mm_health_check is not None:
        mm_rows = _build_mm_pipeline_html(metadata)
        html_parts.append(mm_rows)

    html_parts.extend([
        # Footer
        '<div style="padding:15px 20px;background:#111827;border-top:1px solid #374151;font-size:12px;color:#6b7280">',
        f'<p style="margin:0">Redis Search ETL • <a href="{gcs_url}" style="color:#60a5fa">View in GCS</a></p>',
        "</div>",
        "</div></body></html>",
    ])

    return "".join(html_parts)


def send_etl_summary_email(metadata: "ETLRunMetadata") -> bool:
    """
    Send ETL run summary email.

    Supports SendGrid SMTP or standard Gmail SMTP.

    SendGrid env vars:
    - SENDGRID_PASSWORD: SendGrid API key
    - SENDGRID_USERNAME: Usually "apikey" (literal)
    - SENDGRID_SERVER: smtp.sendgrid.net (optional, default)
    - SENDGRID_PORT: 587 (optional, default)

    Gmail env vars (fallback):
    - SMTP_USER: Gmail address
    - SMTP_PASSWORD or SMTP_APP_PASSWORD: Gmail App Password

    Common:
    - ETL_NOTIFICATION_EMAIL: Recipient email address

    Args:
        metadata: ETL run metadata with job results

    Returns:
        True if email sent successfully, False otherwise
    """
    recipient = os.getenv("ETL_NOTIFICATION_EMAIL")
    if not recipient:
        logger.info("ETL_NOTIFICATION_EMAIL not set, skipping email notification")
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
    else:
        # Fall back to Gmail SMTP
        smtp_server = "smtp.gmail.com"
        smtp_port = 465
        gmail_user = os.getenv("SMTP_USER")
        gmail_password = os.getenv("SMTP_PASSWORD")
        use_tls = False

        if not gmail_user or not gmail_password:
            logger.warning(
                "No email credentials configured (SENDGRID_PASSWORD or SMTP_USER), "
                "skipping email notification"
            )
            return False

        smtp_user = gmail_user
        smtp_password = gmail_password
        from_email = gmail_user

    # Build email
    status_prefix = {
        "completed": "✅",
        "partial": "⚠️",
        "failed": "❌",
    }.get(metadata.status, "")

    subject = (
        f"{status_prefix} Redis Search ETL {metadata.status.upper()}: "
        f"{format_number(metadata.total_documents_upserted)} docs, "
        f"{metadata.total_errors} errors ({metadata.run_date})"
    )

    # Create multipart message with both plain text and HTML
    msg = MIMEMultipart("alternative")
    msg["From"] = from_email
    msg["To"] = recipient
    msg["Subject"] = subject

    # Attach both plain text and HTML versions
    text_part = MIMEText(build_summary_text(metadata), "plain", "utf-8")
    html_part = MIMEText(build_summary_html(metadata), "html", "utf-8")

    msg.attach(text_part)
    msg.attach(html_part)

    try:
        logger.info(f"Sending ETL summary email to {recipient} via {smtp_server}")
        if use_tls:
            # TLS on port 587
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(smtp_user, smtp_password)
                server.sendmail(from_email, recipient, msg.as_string())
        else:
            # SSL on port 465
            with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
                server.login(smtp_user, smtp_password)
                server.sendmail(from_email, recipient, msg.as_string())
        logger.info("ETL summary email sent successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to send ETL summary email: {e}")
        return False
