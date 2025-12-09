"""
ETL Runner - Execute ETL jobs from YAML configuration.

This module provides:
- YAML configuration loading for ETL jobs
- Sequential job execution
- Progress tracking and metadata persistence
- HTTP trigger support for Cloud Run scheduling
"""

import os
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

import yaml  # type: ignore[import-untyped]

from adapters.config import load_env
from etl.etl_metadata import (
    ETLMetadataStore,
    ETLRunMetadata,
    JobRunResult,
    create_run_metadata,
)
from etl.pi_nightly_etl import PIETLStats, run_pi_nightly_etl
from etl.tmdb_nightly_etl import ChangesETLStats, run_nightly_etl
from utils.get_logger import get_logger

logger = get_logger(__name__)


@dataclass
class JobRunParams:
    """Parameters for a single job run."""

    media_type: str
    verbose: bool = False
    start_date: str | None = None
    end_date: str | None = None
    max_batches: int = 0  # 0 = no limit, >0 = limit for testing

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "JobRunParams":
        return cls(
            media_type=data.get("media_type", ""),
            verbose=data.get("verbose", False),
            start_date=data.get("start_date"),
            end_date=data.get("end_date"),
            max_batches=data.get("max_batches", 0),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "media_type": self.media_type,
            "verbose": self.verbose,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "max_batches": self.max_batches,
        }


@dataclass
class JobConfig:
    """Configuration for a single ETL job."""

    name: str
    target: str  # Function name to call
    enabled: bool = True
    runs: list[JobRunParams] = field(default_factory=list)

    @classmethod
    def from_dict(cls, name: str, data: dict[str, Any]) -> "JobConfig":
        runs = []
        for run_data in data.get("runs", []):
            params = run_data.get("params", run_data)  # Support both formats
            runs.append(JobRunParams.from_dict(params))

        return cls(
            name=name,
            target=data.get("target", ""),
            enabled=data.get("enabled", True),
            runs=runs,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "target": self.target,
            "enabled": self.enabled,
            "runs": [r.to_dict() for r in self.runs],
        }


@dataclass
class ETLConfig:
    """Complete ETL configuration loaded from YAML."""

    jobs: list[JobConfig] = field(default_factory=list)

    # Redis config
    redis_host: str = "localhost"
    redis_port: int = 6380
    redis_password: str | None = None

    @classmethod
    def from_yaml(cls, yaml_path: str | Path) -> "ETLConfig":
        """Load ETL configuration from YAML file."""
        with open(yaml_path) as f:
            data = yaml.safe_load(f)

        config = cls()

        # Load jobs
        for job_data in data.get("jobs", []):
            job_name = job_data.get("name", f"job_{len(config.jobs)}")
            job = JobConfig.from_dict(job_name, job_data)
            config.jobs.append(job)

        # Load Redis config
        redis_config = data.get("redis", {})
        config.redis_host = redis_config.get("host", os.getenv("REDIS_HOST", "localhost"))
        config.redis_port = redis_config.get("port", int(os.getenv("REDIS_PORT", "6380")))
        config.redis_password = redis_config.get("password") or os.getenv("REDIS_PASSWORD")

        return config

    @classmethod
    def from_env(cls, yaml_path: str | None = None) -> "ETLConfig":
        """Load ETL configuration from YAML file with environment defaults."""
        load_env()

        if yaml_path is None:
            yaml_path = os.getenv("ETL_CONFIG_PATH", "config/etl_jobs.yaml")

        return cls.from_yaml(yaml_path)

    def to_dict(self) -> dict[str, Any]:
        return {
            "jobs": [j.to_dict() for j in self.jobs],
            "redis_host": self.redis_host,
            "redis_port": self.redis_port,
        }


# Registry of available ETL functions
ETL_FUNCTIONS: dict[str, Any] = {
    "tmdb_nightly_etl": run_nightly_etl,
    "pi_nightly_etl": run_pi_nightly_etl,
}


class ETLRunner:
    """
    Run ETL jobs from configuration.

    The runner:
    1. Loads job configurations from YAML
    2. Determines start_date from last successful run (or uses provided override)
    3. Executes jobs sequentially
    4. Tracks progress and saves metadata to GCS
    """

    def __init__(
        self,
        config: ETLConfig | None = None,
        metadata_store: ETLMetadataStore | None = None,
    ):
        self.config = config or ETLConfig.from_env()
        self.metadata_store = metadata_store or ETLMetadataStore()
        self._run_metadata: ETLRunMetadata | None = None

    @property
    def run_metadata(self) -> ETLRunMetadata | None:
        return self._run_metadata

    def get_job_start_date(self, job_name: str, override: str | None = None) -> str:
        """
        Determine the start_date for a job run.

        Priority:
        1. Override if provided
        2. Last successful run date + 1 day
        3. Yesterday (default)
        """
        if override:
            return override

        # Check last successful run
        last_run = self.metadata_store.get_last_run_date(job_name)
        if last_run:
            # Start from day after last run
            try:
                last_date = datetime.strptime(last_run, "%Y-%m-%d").date()
                start_date = last_date + timedelta(days=1)
                return start_date.isoformat()
            except ValueError:
                pass

        # Default to yesterday
        yesterday = date.today() - timedelta(days=1)
        return yesterday.isoformat()

    async def run_job(
        self,
        job: JobConfig,
        params: JobRunParams,
        start_date_override: str | None = None,
        end_date_override: str | None = None,
    ) -> JobRunResult:
        """
        Run a single ETL job.

        Args:
            job: Job configuration
            params: Run parameters
            start_date_override: Override start date
            end_date_override: Override end date

        Returns:
            JobRunResult with run outcome
        """
        job_name = f"{job.name}_{params.media_type}"
        result = JobRunResult(
            job_name=job_name,
            media_type=params.media_type,
            status="running",
            started_at=datetime.now(),
        )

        # Log to run metadata if available
        if self._run_metadata:
            self._run_metadata.add_log(f"Starting job: {job_name}")

        try:
            # Get the ETL function
            etl_func = ETL_FUNCTIONS.get(job.target)
            if not etl_func:
                raise ValueError(f"Unknown ETL function: {job.target}")

            # Determine dates
            start_date = (
                start_date_override or params.start_date or self.get_job_start_date(job_name)
            )
            end_date = end_date_override or params.end_date or date.today().isoformat()

            logger.info(f"Running {job_name}: {start_date} to {end_date}")

            # Handle different ETL types
            if job.target == "pi_nightly_etl":
                # PodcastIndex ETL - doesn't use media_type/dates, uses since_hours
                stats: ChangesETLStats | PIETLStats = await etl_func(
                    media_type="podcast",
                    redis_host=self.config.redis_host,
                    redis_port=self.config.redis_port,
                    redis_password=self.config.redis_password,
                    max_batches=params.max_batches,
                )
            else:
                # TMDB ETL - validate media_type
                if params.media_type not in ("tv", "movie", "person"):
                    raise ValueError(f"Invalid media_type: {params.media_type}")

                # Cast media_type to the literal type
                media_type_literal: Literal["tv", "movie", "person"] = params.media_type  # type: ignore[assignment]
                stats = await etl_func(
                    media_type=media_type_literal,
                    start_date=start_date,
                    end_date=end_date,
                    redis_host=self.config.redis_host,
                    redis_port=self.config.redis_port,
                    redis_password=self.config.redis_password,
                    verbose=params.verbose,
                    max_batches=params.max_batches,
                )

            # Update result from stats (both ETL types have compatible interfaces)
            load_errors = stats.load_phase.items_failed
            result.status = "success" if load_errors == 0 else "partial"
            result.changes_found = stats.total_changes_found
            result.documents_upserted = stats.load_phase.items_success
            result.documents_skipped = stats.failed_filter
            all_errors = stats.fetch_phase.errors + stats.load_phase.errors
            result.errors_count = len(all_errors)
            result.errors = all_errors

            if self._run_metadata:
                self._run_metadata.add_log(
                    f"Completed {job_name}: {result.documents_upserted} upserted, "
                    f"{result.errors_count} errors"
                )

        except Exception as e:
            import traceback
            result.status = "failed"
            result.error_message = str(e)
            result.errors.append(str(e))
            error_traceback = traceback.format_exc()
            logger.error(f"Job {job_name} failed: {e}\n{error_traceback}")

            if self._run_metadata:
                self._run_metadata.add_log(f"FAILED {job_name}: {e}\n{error_traceback}")

        finally:
            result.completed_at = datetime.now()
            if result.started_at:
                result.duration_seconds = (result.completed_at - result.started_at).total_seconds()

        return result

    async def run_all(
        self,
        start_date_override: str | None = None,
        end_date_override: str | None = None,
        job_filter: list[str] | None = None,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> ETLRunMetadata:
        """
        Run all enabled ETL jobs sequentially.

        Args:
            start_date_override: Override start date for all jobs
            end_date_override: Override end date for all jobs
            job_filter: Optional list of job names to run (runs all if None)
            progress_callback: Optional callback called after each job with progress dict

        Returns:
            ETLRunMetadata with complete run results
        """
        self._run_metadata = create_run_metadata()
        self._run_metadata.config_snapshot = self.config.to_dict()

        # Count enabled jobs
        enabled_jobs = [j for j in self.config.jobs if j.enabled]
        if job_filter:
            enabled_jobs = [j for j in enabled_jobs if j.name in job_filter]

        total_runs = sum(len(j.runs) for j in enabled_jobs)
        self._run_metadata.total_jobs = total_runs
        self._run_metadata.add_log(f"Starting ETL run with {total_runs} job runs")

        print("=" * 60)
        print("🚀 ETL Runner - Starting Full Run")
        print("=" * 60)
        print(f"  Run ID: {self._run_metadata.run_id}")
        print(f"  Total jobs: {total_runs}")
        print(f"  Redis: {self.config.redis_host}:{self.config.redis_port}")
        print()

        # Run each job sequentially
        for job in enabled_jobs:
            print(f"\n📋 Job: {job.name}")
            print(f"   Target: {job.target}")

            for params in job.runs:
                print(f"\n   Running: {params.media_type}")

                result = await self.run_job(
                    job,
                    params,
                    start_date_override=start_date_override,
                    end_date_override=end_date_override,
                )

                # Track result
                self._run_metadata.job_results.append(result)

                if result.status == "success":
                    self._run_metadata.jobs_completed += 1
                elif result.status == "failed":
                    self._run_metadata.jobs_failed += 1
                elif result.status == "skipped":
                    self._run_metadata.jobs_skipped += 1

                # Aggregate stats
                self._run_metadata.total_changes_found += result.changes_found
                self._run_metadata.total_documents_upserted += result.documents_upserted
                self._run_metadata.total_errors += result.errors_count

                # Update job state (don't let GCS failures kill the whole run)
                try:
                    self.metadata_store.update_job_state(result.job_name, result)
                except Exception as e:
                    logger.warning(f"Failed to update job state for {result.job_name}: {e}")

                # Call progress callback if provided
                if progress_callback:
                    progress_callback(
                        {
                            "total_jobs": total_runs,
                            "jobs_completed": self._run_metadata.jobs_completed,
                            "jobs_failed": self._run_metadata.jobs_failed,
                            "current_job": f"{job.name}_{params.media_type}",
                            "last_result": {
                                "job_name": result.job_name,
                                "status": result.status,
                                "changes_found": result.changes_found,
                                "documents_upserted": result.documents_upserted,
                            },
                        }
                    )

        # Finalize run metadata
        self._run_metadata.completed_at = datetime.now()
        if self._run_metadata.started_at:
            self._run_metadata.duration_seconds = (
                self._run_metadata.completed_at - self._run_metadata.started_at
            ).total_seconds()

        # Determine overall status
        if self._run_metadata.jobs_failed == 0:
            self._run_metadata.status = "completed"
        elif self._run_metadata.jobs_completed > 0:
            self._run_metadata.status = "partial"
        else:
            self._run_metadata.status = "failed"

        self._run_metadata.add_log(f"ETL run completed: {self._run_metadata.status}")

        # Save metadata to GCS (don't let failure prevent email)
        try:
            self.metadata_store.save_run_metadata(self._run_metadata)
        except Exception as e:
            logger.warning(f"Failed to save run metadata to GCS: {e}")

        # Send email notification
        from etl.notifications import send_etl_summary_email

        try:
            email_sent = send_etl_summary_email(self._run_metadata)
            if email_sent:
                self._run_metadata.add_log("Email notification sent")
        except Exception as e:
            logger.warning(f"Failed to send email notification: {e}")

        # Print summary
        print()
        print("=" * 60)
        print("📊 ETL Run Summary")
        print("=" * 60)
        print(f"  Run ID: {self._run_metadata.run_id}")
        print(f"  Status: {self._run_metadata.status}")
        print(f"  Jobs completed: {self._run_metadata.jobs_completed}")
        print(f"  Jobs failed: {self._run_metadata.jobs_failed}")
        print(f"  Total changes found: {self._run_metadata.total_changes_found}")
        print(f"  Total documents upserted: {self._run_metadata.total_documents_upserted}")
        print(f"  Total errors: {self._run_metadata.total_errors}")
        if self._run_metadata.duration_seconds:
            print(f"  Duration: {self._run_metadata.duration_seconds:.1f}s")
        print()
        print("🎉 ETL Run Complete!")

        return self._run_metadata

    async def run_single_job(
        self,
        job_name: str,
        media_type: str,
        start_date: str | None = None,
        end_date: str | None = None,
        verbose: bool = False,
    ) -> JobRunResult:
        """
        Run a single job by name and media type.

        This is useful for manual triggering of specific jobs.

        Args:
            job_name: Name of the job from config
            media_type: Media type to process
            start_date: Optional start date override
            end_date: Optional end date override
            verbose: Enable verbose logging

        Returns:
            JobRunResult with outcome
        """
        # Find the job config
        job_config = None
        for job in self.config.jobs:
            if job.name == job_name:
                job_config = job
                break

        if not job_config:
            return JobRunResult(
                job_name=f"{job_name}_{media_type}",
                media_type=media_type,
                status="failed",
                error_message=f"Job not found: {job_name}",
                completed_at=datetime.now(),
            )

        # Create params
        params = JobRunParams(
            media_type=media_type,
            verbose=verbose,
            start_date=start_date,
            end_date=end_date,
        )

        return await self.run_job(job_config, params, start_date, end_date)


# Convenience functions for running ETL


async def run_full_etl(
    config_path: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> ETLRunMetadata:
    """
    Run the full ETL from YAML configuration.

    This is the main entry point for scheduled runs.

    Args:
        config_path: Path to YAML config file
        start_date: Optional start date override for all jobs
        end_date: Optional end date override for all jobs

    Returns:
        ETLRunMetadata with run results
    """
    load_env()
    config = ETLConfig.from_env(config_path)
    runner = ETLRunner(config)
    return await runner.run_all(start_date_override=start_date, end_date_override=end_date)


async def run_single_etl(
    media_type: str,
    start_date: str | None = None,
    end_date: str | None = None,
    redis_host: str | None = None,
    redis_port: int | None = None,
    redis_password: str | None = None,
    verbose: bool = False,
    stats: ChangesETLStats | None = None,
) -> ChangesETLStats:
    """
    Run a single ETL job directly without YAML config.

    Useful for testing and manual runs.

    Args:
        media_type: 'tv', 'movie', or 'person'
        start_date: Start date (defaults to yesterday)
        end_date: End date (defaults to today)
        redis_host: Redis host (defaults to env)
        redis_port: Redis port (defaults to env)
        redis_password: Redis password (defaults to env)
        verbose: Enable verbose logging
        stats: Optional pre-created stats object (for progress tracking)

    Returns:
        ChangesETLStats with results
    """
    load_env()

    host: str = redis_host if redis_host is not None else (os.getenv("REDIS_HOST") or "localhost")
    port = redis_port or int(os.getenv("REDIS_PORT", "6380"))
    password = redis_password or os.getenv("REDIS_PASSWORD")

    # Validate media_type
    if media_type not in ("tv", "movie", "person"):
        raise ValueError(f"Invalid media_type: {media_type}")
    media_type_literal: Literal["tv", "movie", "person"] = media_type  # type: ignore[assignment]

    return await run_nightly_etl(
        media_type=media_type_literal,
        start_date=start_date,
        end_date=end_date,
        redis_host=host,
        redis_port=port,
        redis_password=password,
        verbose=verbose,
        stats=stats,
    )
