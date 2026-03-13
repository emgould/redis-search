"""
ETL Metadata Storage - Track ETL runs in GCS.

This module provides functionality to persist ETL run metadata to GCS,
including:
- Overall ETL run tracking
- Per-job run history and state
- Daily run logs and debugging information
"""

import gzip
import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from google.cloud import storage  # type: ignore[attr-defined]
from google.cloud.exceptions import NotFound

from adapters.config import load_env
from utils.get_logger import get_logger

logger = get_logger(__name__)


@dataclass
class JobRunResult:
    """Result of a single ETL job run."""

    job_name: str
    media_type: str
    status: str  # "success", "failed", "skipped"
    started_at: datetime | None = None
    completed_at: datetime | None = None
    duration_seconds: float | None = None
    effective_start_date: str | None = None  # YYYY-MM-DD resolved at runtime
    effective_end_date: str | None = None  # YYYY-MM-DD resolved at runtime

    # Job-specific stats
    changes_found: int = 0
    documents_upserted: int = 0
    documents_skipped: int = 0
    errors_count: int = 0
    mm_docs_sent: int = 0

    # Error tracking
    error_message: str | None = None
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "job_name": self.job_name,
            "media_type": self.media_type,
            "status": self.status,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": self.duration_seconds,
            "effective_start_date": self.effective_start_date,
            "effective_end_date": self.effective_end_date,
            "changes_found": self.changes_found,
            "documents_upserted": self.documents_upserted,
            "documents_skipped": self.documents_skipped,
            "errors_count": self.errors_count,
            "mm_docs_sent": self.mm_docs_sent,
            "error_message": self.error_message,
            "errors": self.errors[:20] if self.errors else [],  # Limit stored errors
        }


@dataclass
class ETLRunMetadata:
    """Metadata for a complete ETL run (all jobs)."""

    run_id: str  # Format: YYYY-MM-DD_HHMMSS
    run_date: str  # YYYY-MM-DD
    status: str  # "running", "completed", "failed", "partial"
    started_at: datetime | None = None
    completed_at: datetime | None = None
    duration_seconds: float | None = None

    # Job tracking
    total_jobs: int = 0
    jobs_completed: int = 0
    jobs_failed: int = 0
    jobs_skipped: int = 0

    # Aggregate stats
    total_changes_found: int = 0
    total_documents_upserted: int = 0
    total_errors: int = 0
    total_mm_docs_sent: int = 0

    # Media Manager pipeline events (structured, not just log strings)
    mm_health_check: str | None = None  # "ok", "unavailable", or None (not configured)
    mm_queue_drained: bool | None = None  # True/False/None (not attempted)
    mm_queue_drain_error: str | None = None
    mm_indexes_rebuilt: list[dict[str, Any]] = field(default_factory=list)
    mm_rebuild_errors: list[str] = field(default_factory=list)
    mm_finalize_publish: dict[str, Any] | None = None
    mm_finalize_error: str | None = None

    # Job results
    job_results: list[JobRunResult] = field(default_factory=list)

    # Configuration used
    config_snapshot: dict[str, Any] = field(default_factory=dict)

    # Logs (accumulated during run)
    logs: list[str] = field(default_factory=list)

    def add_log(self, message: str) -> None:
        """Add a timestamped log message."""
        timestamp = datetime.now().isoformat()
        self.logs.append(f"[{timestamp}] {message}")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "run_id": self.run_id,
            "run_date": self.run_date,
            "status": self.status,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": self.duration_seconds,
            "total_jobs": self.total_jobs,
            "jobs_completed": self.jobs_completed,
            "jobs_failed": self.jobs_failed,
            "jobs_skipped": self.jobs_skipped,
            "total_changes_found": self.total_changes_found,
            "total_documents_upserted": self.total_documents_upserted,
            "total_errors": self.total_errors,
            "total_mm_docs_sent": self.total_mm_docs_sent,
            "media_manager_pipeline": {
                "health_check": self.mm_health_check,
                "queue_drained": self.mm_queue_drained,
                "queue_drain_error": self.mm_queue_drain_error,
                "indexes_rebuilt": self.mm_indexes_rebuilt,
                "rebuild_errors": self.mm_rebuild_errors,
                "finalize_publish": self.mm_finalize_publish,
                "finalize_error": self.mm_finalize_error,
            },
            "job_results": [jr.to_dict() for jr in self.job_results],
            "config_snapshot": self.config_snapshot,
            "logs": self.logs[-500] if len(self.logs) > 500 else self.logs,  # Keep last 500 logs
        }


@dataclass
class JobState:
    """Persistent state for an ETL job (tracks last run time and coverage)."""

    job_name: str
    last_run_date: str | None = None  # YYYY-MM-DD
    last_run_time: str | None = None  # ISO format datetime
    last_status: str | None = None  # "success", "failed"
    last_changes_found: int = 0
    last_documents_upserted: int = 0
    mm_docs_sent: int = 0
    effective_start_date: str | None = None  # YYYY-MM-DD date range start
    effective_end_date: str | None = None  # YYYY-MM-DD date range end
    duration_seconds: float | None = None
    errors_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_name": self.job_name,
            "last_run_date": self.last_run_date,
            "last_run_time": self.last_run_time,
            "last_status": self.last_status,
            "last_changes_found": self.last_changes_found,
            "last_documents_upserted": self.last_documents_upserted,
            "mm_docs_sent": self.mm_docs_sent,
            "effective_start_date": self.effective_start_date,
            "effective_end_date": self.effective_end_date,
            "duration_seconds": self.duration_seconds,
            "errors_count": self.errors_count,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "JobState":
        return cls(
            job_name=data.get("job_name", ""),
            last_run_date=data.get("last_run_date"),
            last_run_time=data.get("last_run_time"),
            last_status=data.get("last_status"),
            last_changes_found=data.get("last_changes_found", 0),
            last_documents_upserted=data.get("last_documents_upserted", 0),
            mm_docs_sent=data.get("mm_docs_sent", 0),
            effective_start_date=data.get("effective_start_date"),
            effective_end_date=data.get("effective_end_date"),
            duration_seconds=data.get("duration_seconds"),
            errors_count=data.get("errors_count", 0),
        )


GCS_ETL_PREFIXES: dict[str, str] = {
    "local": "redis-search/etl/local",
    "public": "redis-search/etl/dev",
}


@dataclass
class ETLStateConfig:
    """Configuration for ETL metadata storage."""

    gcs_bucket: str | None
    gcs_prefix: str  # e.g., "redis-search/etl"

    @classmethod
    def from_env(cls) -> "ETLStateConfig":
        """Create config from environment variables."""
        load_env()
        return cls(
            gcs_bucket=os.getenv("GCS_BUCKET"),
            gcs_prefix=os.getenv("GCS_ETL_PREFIX", "redis-search/etl"),
        )

    @classmethod
    def for_environment(cls, env_name: str) -> "ETLStateConfig":
        """Create config for a specific environment (local or public)."""
        load_env()
        prefix = GCS_ETL_PREFIXES.get(env_name, os.getenv("GCS_ETL_PREFIX", "redis-search/etl"))
        return cls(
            gcs_bucket=os.getenv("GCS_BUCKET"),
            gcs_prefix=prefix,
        )


class ETLMetadataStore:
    """
    Store and retrieve ETL metadata from GCS.

    Directory structure:
        {prefix}/
            state/
                job_states.json.gz       # Persistent job states (last run times)
            runs/
                2025-12-08/
                    run_20251208_030000.json.gz  # Complete run metadata
                    run_20251208_030000_logs.txt.gz  # Detailed logs
    """

    def __init__(self, config: ETLStateConfig | None = None):
        self.config = config or ETLStateConfig.from_env()
        self._client: storage.Client | None = None
        self._log_configuration()

    def _log_configuration(self) -> None:
        """Log metadata store configuration for visibility."""
        # Detect if running in Cloud Run (K_SERVICE is auto-set by Cloud Run)
        is_cloud_run = os.getenv("K_SERVICE") is not None
        environment = "cloud_run" if is_cloud_run else os.getenv("ENVIRONMENT", "local")

        if self.config.gcs_bucket:
            logger.info(
                f"ETL metadata store initialized: "
                f"bucket={self.config.gcs_bucket}, "
                f"prefix={self.config.gcs_prefix}, "
                f"environment={environment}"
            )
        else:
            if is_cloud_run:
                logger.error(
                    "GCS_BUCKET not configured in Cloud Run! "
                    "ETL run metadata will NOT be persisted. "
                    "Set GCS_BUCKET environment variable."
                )
            else:
                logger.warning(
                    f"GCS_BUCKET not configured (environment={environment}). "
                    "ETL run metadata will not be persisted to GCS. "
                    "This is expected in local development."
                )

    @property
    def client(self) -> storage.Client:
        if self._client is None:
            # On GCE VMs, use default credentials (service account)
            # If GOOGLE_APPLICATION_CREDENTIALS is set to empty string, unset it
            # to allow default credentials to work
            creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            if creds_path == "":
                # Temporarily unset to allow default credentials
                os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)

            try:
                # Use default credentials (works on GCE VMs with service accounts)
                self._client = storage.Client()
            except Exception:
                # If default credentials fail, try with explicit project from env
                project = os.getenv("GCP_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")
                if project:
                    self._client = storage.Client(project=project)
                else:
                    # Re-raise the original error if we can't resolve it
                    raise
            finally:
                # Restore original value if we unset it
                if creds_path == "":
                    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ""
        return self._client

    @property
    def bucket(self) -> storage.Bucket | None:
        if not self.config.gcs_bucket:
            return None
        return self.client.bucket(self.config.gcs_bucket)

    def _gcs_path(self, *parts: str) -> str:
        """Construct a GCS path."""
        return "/".join([self.config.gcs_prefix, *parts])

    def _upload_json(self, data: dict[str, Any], gcs_path: str) -> bool:
        """Upload JSON data to GCS with gzip compression."""
        if not self.bucket:
            logger.warning("GCS bucket not configured, skipping upload")
            return False

        try:
            json_bytes = json.dumps(data, indent=2, default=str).encode("utf-8")
            compressed = gzip.compress(json_bytes)

            blob = self.bucket.blob(f"{gcs_path}.gz")
            blob.upload_from_string(compressed, content_type="application/gzip")

            logger.info(f"Uploaded metadata to gs://{self.config.gcs_bucket}/{gcs_path}.gz")
            return True
        except Exception as e:
            logger.error(f"Failed to upload metadata to GCS: {e}")
            return False

    def _download_json(self, gcs_path: str) -> dict[str, Any] | None:
        """Download and decompress JSON data from GCS."""
        if not self.bucket:
            return None

        try:
            blob = self.bucket.blob(f"{gcs_path}.gz")
            compressed = blob.download_as_bytes()
            json_bytes = gzip.decompress(compressed)
            result: dict[str, Any] = json.loads(json_bytes.decode("utf-8"))
            return result
        except NotFound:
            return None
        except Exception as e:
            logger.error(f"Failed to download metadata from GCS: {e}")
            return None

    def _upload_text(self, text: str, gcs_path: str) -> bool:
        """Upload text data to GCS with gzip compression."""
        if not self.bucket:
            return False

        try:
            text_bytes = text.encode("utf-8")
            compressed = gzip.compress(text_bytes)

            blob = self.bucket.blob(f"{gcs_path}.gz")
            blob.upload_from_string(compressed, content_type="application/gzip")
            return True
        except Exception as e:
            logger.error(f"Failed to upload text to GCS: {e}")
            return False

    # =========================================================================
    # Job State Management
    # =========================================================================

    def _load_state_file(self) -> dict[str, Any]:
        """Load the full state file (jobs + last_run_summary) from GCS."""
        gcs_path = self._gcs_path("state", "job_states.json")
        return self._download_json(gcs_path) or {}

    def _save_state_file(self, data: dict[str, Any]) -> bool:
        """Write the full state file back to GCS."""
        data["updated_at"] = datetime.now().isoformat()
        gcs_path = self._gcs_path("state", "job_states.json")
        return self._upload_json(data, gcs_path)

    def get_all_job_states(self) -> dict[str, JobState]:
        """Load all job states from GCS."""
        data = self._load_state_file()
        states: dict[str, JobState] = {}
        for job_name, state_data in data.get("jobs", {}).items():
            states[job_name] = JobState.from_dict(state_data)
        return states

    def get_job_state(self, job_name: str) -> JobState | None:
        """Get state for a specific job."""
        states = self.get_all_job_states()
        return states.get(job_name)

    def save_job_states(self, states: dict[str, JobState]) -> bool:
        """Save all job states to GCS, preserving other keys in the file."""
        data = self._load_state_file()
        data["jobs"] = {name: state.to_dict() for name, state in states.items()}
        return self._save_state_file(data)

    def update_job_state(self, job_name: str, result: JobRunResult) -> bool:
        """Update state for a single job after a run."""
        states = self.get_all_job_states()

        if job_name not in states:
            states[job_name] = JobState(job_name=job_name)

        state = states[job_name]
        state.last_run_time = result.completed_at.isoformat() if result.completed_at else None
        state.last_run_date = (
            result.completed_at.strftime("%Y-%m-%d") if result.completed_at else None
        )
        state.last_status = result.status
        state.last_changes_found = result.changes_found
        state.last_documents_upserted = result.documents_upserted
        state.mm_docs_sent = result.mm_docs_sent
        state.effective_start_date = result.effective_start_date
        state.effective_end_date = result.effective_end_date
        state.duration_seconds = result.duration_seconds
        state.errors_count = result.errors_count

        return self.save_job_states(states)

    def save_run_summary(self, metadata: ETLRunMetadata) -> bool:
        """Persist the latest run summary into the state file alongside job states."""
        data = self._load_state_file()
        data["last_run_summary"] = {
            "run_id": metadata.run_id,
            "run_date": metadata.run_date,
            "status": metadata.status,
            "started_at": metadata.started_at.isoformat() if metadata.started_at else None,
            "completed_at": metadata.completed_at.isoformat() if metadata.completed_at else None,
            "duration_seconds": metadata.duration_seconds,
            "total_jobs": metadata.total_jobs,
            "jobs_completed": metadata.jobs_completed,
            "jobs_failed": metadata.jobs_failed,
            "total_changes_found": metadata.total_changes_found,
            "total_documents_upserted": metadata.total_documents_upserted,
            "total_errors": metadata.total_errors,
            "total_mm_docs_sent": metadata.total_mm_docs_sent,
            "media_manager_pipeline": {
                "health_check": metadata.mm_health_check,
                "queue_drained": metadata.mm_queue_drained,
                "queue_drain_error": metadata.mm_queue_drain_error,
                "indexes_rebuilt": metadata.mm_indexes_rebuilt,
                "rebuild_errors": metadata.mm_rebuild_errors,
                "finalize_publish": metadata.mm_finalize_publish,
                "finalize_error": metadata.mm_finalize_error,
            },
        }
        return self._save_state_file(data)

    def get_last_run_summary(self) -> dict[str, Any] | None:
        """Load the last run summary from the state file."""
        data = self._load_state_file()
        summary: dict[str, Any] | None = data.get("last_run_summary")
        return summary

    def get_last_run_date(self, job_name: str) -> str | None:
        """
        Get the last successful run date for a job.
        Used to determine the start_date for the next run.
        """
        state = self.get_job_state(job_name)
        if state and state.last_status == "success":
            return state.last_run_date
        return None

    # =========================================================================
    # Run Metadata Management
    # =========================================================================

    def save_run_metadata(self, metadata: ETLRunMetadata) -> bool:
        """Save complete run metadata to GCS."""
        run_date = metadata.run_date
        run_id = metadata.run_id

        # Save main metadata
        gcs_path = self._gcs_path("runs", run_date, f"run_{run_id}.json")
        success = self._upload_json(metadata.to_dict(), gcs_path)

        # Save detailed logs separately
        if metadata.logs:
            logs_text = "\n".join(metadata.logs)
            logs_path = self._gcs_path("runs", run_date, f"run_{run_id}_logs.txt")
            self._upload_text(logs_text, logs_path)

        return success

    def get_run_metadata(self, run_date: str, run_id: str) -> ETLRunMetadata | None:
        """Load run metadata from GCS."""
        gcs_path = self._gcs_path("runs", run_date, f"run_{run_id}.json")
        data = self._download_json(gcs_path)

        if not data:
            return None

        # Reconstruct metadata from dict
        mm_pipeline = data.get("media_manager_pipeline", {})
        metadata = ETLRunMetadata(
            run_id=data.get("run_id", run_id),
            run_date=data.get("run_date", run_date),
            status=data.get("status", "unknown"),
            total_jobs=data.get("total_jobs", 0),
            jobs_completed=data.get("jobs_completed", 0),
            jobs_failed=data.get("jobs_failed", 0),
            jobs_skipped=data.get("jobs_skipped", 0),
            total_changes_found=data.get("total_changes_found", 0),
            total_documents_upserted=data.get("total_documents_upserted", 0),
            total_errors=data.get("total_errors", 0),
            total_mm_docs_sent=data.get("total_mm_docs_sent", 0),
            mm_health_check=mm_pipeline.get("health_check"),
            mm_queue_drained=mm_pipeline.get("queue_drained"),
            mm_queue_drain_error=mm_pipeline.get("queue_drain_error"),
            mm_indexes_rebuilt=mm_pipeline.get("indexes_rebuilt", []),
            mm_rebuild_errors=mm_pipeline.get("rebuild_errors", []),
            mm_finalize_publish=mm_pipeline.get("finalize_publish"),
            mm_finalize_error=mm_pipeline.get("finalize_error"),
            config_snapshot=data.get("config_snapshot", {}),
            logs=data.get("logs", []),
        )

        # Parse datetimes
        if data.get("started_at"):
            metadata.started_at = datetime.fromisoformat(data["started_at"])
        if data.get("completed_at"):
            metadata.completed_at = datetime.fromisoformat(data["completed_at"])
        metadata.duration_seconds = data.get("duration_seconds")

        # Reconstruct job results
        for jr_data in data.get("job_results", []):
            jr = JobRunResult(
                job_name=jr_data.get("job_name", ""),
                media_type=jr_data.get("media_type", ""),
                status=jr_data.get("status", "unknown"),
                effective_start_date=jr_data.get("effective_start_date"),
                effective_end_date=jr_data.get("effective_end_date"),
                changes_found=jr_data.get("changes_found", 0),
                documents_upserted=jr_data.get("documents_upserted", 0),
                documents_skipped=jr_data.get("documents_skipped", 0),
                errors_count=jr_data.get("errors_count", 0),
                mm_docs_sent=jr_data.get("mm_docs_sent", 0),
                error_message=jr_data.get("error_message"),
                errors=jr_data.get("errors", []),
            )
            if jr_data.get("started_at"):
                jr.started_at = datetime.fromisoformat(jr_data["started_at"])
            if jr_data.get("completed_at"):
                jr.completed_at = datetime.fromisoformat(jr_data["completed_at"])
            jr.duration_seconds = jr_data.get("duration_seconds")
            metadata.job_results.append(jr)

        return metadata

    def list_runs(self, run_date: str | None = None, limit: int = 10) -> list[dict[str, Any]]:
        """
        List recent ETL runs.

        Args:
            run_date: Optional specific date to list runs for
            limit: Maximum number of runs to return

        Returns:
            List of run summaries (run_id, run_date, status, etc.)
        """
        if not self.bucket:
            return []

        runs = []

        try:
            if run_date:
                prefix = self._gcs_path("runs", run_date) + "/"
            else:
                prefix = self._gcs_path("runs") + "/"

            blobs = self.bucket.list_blobs(prefix=prefix)

            for blob in blobs:
                if blob.name.endswith(".json.gz") and "_logs" not in blob.name:
                    # Extract run info from path
                    parts = blob.name.split("/")
                    if len(parts) >= 2:
                        filename = parts[-1]
                        date_part = parts[-2]

                        # Parse run_id from filename: run_YYYYMMDD_HHMMSS.json.gz
                        run_id = filename.replace("run_", "").replace(".json.gz", "")

                        runs.append(
                            {
                                "run_id": run_id,
                                "run_date": date_part,
                                "blob_path": blob.name,
                                "size_bytes": blob.size,
                                "created": blob.time_created.isoformat()
                                if blob.time_created
                                else None,
                            }
                        )

            # Sort by run_id (most recent first) and limit
            runs.sort(key=lambda x: x["run_id"], reverse=True)
            return runs[:limit]

        except Exception as e:
            logger.error(f"Failed to list runs: {e}")
            return []

    def get_latest_run(self) -> ETLRunMetadata | None:
        """Get the most recent ETL run metadata."""
        runs = self.list_runs(limit=1)
        if not runs:
            return None

        run = runs[0]
        return self.get_run_metadata(run["run_date"], run["run_id"])


def generate_run_id() -> str:
    """Generate a unique run ID based on current timestamp."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def create_run_metadata() -> ETLRunMetadata:
    """Create a new run metadata instance."""
    now = datetime.now()
    return ETLRunMetadata(
        run_id=now.strftime("%Y%m%d_%H%M%S"),
        run_date=now.strftime("%Y-%m-%d"),
        status="running",
        started_at=now,
    )
