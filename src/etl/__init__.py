"""
ETL Module - Extract, Transform, Load for TMDB data.

This module provides:
- TMDB Changes ETL: Process daily changes from TMDB API
- ETL Runner: Execute ETL jobs from YAML configuration
- ETL Metadata: Track run history and job state in GCS
- ETL API: HTTP endpoints for triggering and monitoring
"""

from etl.etl_metadata import (
    ETLMetadataStore,
    ETLRunMetadata,
    ETLStateConfig,
    JobRunResult,
    JobState,
    create_run_metadata,
)
from etl.etl_runner import (
    ETLConfig,
    ETLRunner,
    JobConfig,
    JobRunParams,
    run_full_etl,
    run_single_etl,
)
from etl.tmdb_changes_etl import (
    ChangesETLStats,
    TMDBChangesETL,
    run_nightly_etl,
)

# Note: etl_api is not imported here to avoid circular imports
# Import it directly when needed: from etl.etl_api import app

__all__ = [
    # Changes ETL
    "TMDBChangesETL",
    "ChangesETLStats",
    "run_nightly_etl",
    # Runner
    "ETLRunner",
    "ETLConfig",
    "JobConfig",
    "JobRunParams",
    "run_full_etl",
    "run_single_etl",
    # Metadata
    "ETLMetadataStore",
    "ETLRunMetadata",
    "ETLStateConfig",
    "JobRunResult",
    "JobState",
    "create_run_metadata",
]
