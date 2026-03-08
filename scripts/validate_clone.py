"""
Validate Clone - Verify Redis clone integrity.

Compares source and target Redis instances across multiple validation levels
to prove that a clone operation produced correct, uncorrupted results.

Validation levels:
  1. Structural  - index lists, key counts, num_docs
  2. Content     - DUMP comparison of sampled keys (raw byte equality)
  3. Search      - FT.SEARCH smoke queries on each index
  4. Failure     - detect partial transfer states

Usage:
    # Compare public vs scratch
    python scripts/validate_clone.py --source public --target scratch

    # Compare public vs local
    python scripts/validate_clone.py --source public --target local

    # Scope to specific prefixes
    python scripts/validate_clone.py --source public --target scratch --prefixes media: person:

    # Larger sample size
    python scripts/validate_clone.py --source public --target scratch --sample-size 500

    # Full key comparison (expensive)
    python scripts/validate_clone.py --source public --target scratch --full-compare

    # Machine-readable output
    python scripts/validate_clone.py --source public --target scratch --json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

from dotenv import load_dotenv
from redis.asyncio import Redis

env_file = os.getenv("ENV_FILE", "config/local.env")
load_dotenv(env_file)

_project_root = Path(__file__).resolve().parent.parent

PREFIX_TO_INDEX: dict[str, str] = {
    "media:": "idx:media",
    "person:": "idx:people",
    "podcast:": "idx:podcasts",
    "book:": "idx:book",
    "author:": "idx:author",
}

ALL_PREFIXES: list[str] = list(PREFIX_TO_INDEX.keys())

ENDPOINTS: dict[str, dict[str, object]] = {
    "public": {
        "host": os.getenv("PUBLIC_REDIS_HOST", "localhost"),
        "port": int(os.getenv("PUBLIC_REDIS_PORT", "6381")),
        "password": os.getenv("PUBLIC_REDIS_PASSWORD") or None,
        "label": "Public Redis",
    },
    "local": {
        "host": os.getenv("REDIS_HOST", "localhost"),
        "port": int(os.getenv("REDIS_PORT", "6380")),
        "password": os.getenv("REDIS_PASSWORD") or None,
        "label": "Local Redis",
    },
    "scratch": {
        "host": "localhost",
        "port": 6382,
        "password": None,
        "label": "Scratch Redis",
    },
}


@dataclass
class PrefixReport:
    prefix: str
    source_count: int = 0
    target_count: int = 0
    count_match: bool = False
    sampled: int = 0
    matched: int = 0
    mismatched: int = 0
    missing_on_target: int = 0
    extra_on_target: int = 0
    mismatched_keys: list[str] = field(default_factory=list)
    missing_keys: list[str] = field(default_factory=list)


@dataclass
class IndexReport:
    name: str
    source_num_docs: int = 0
    target_num_docs: int = 0
    num_docs_match: bool = False
    source_exists: bool = False
    target_exists: bool = False
    target_indexing: bool = False
    target_percent_indexed: float = 0.0
    smoke_queries_passed: int = 0
    smoke_queries_failed: int = 0
    smoke_failures: list[str] = field(default_factory=list)


@dataclass
class FailureReport:
    issues: list[str] = field(default_factory=list)


@dataclass
class ValidationReport:
    source: str
    target: str
    elapsed_seconds: float = 0.0
    source_dbsize: int = 0
    target_dbsize: int = 0
    prefixes: list[PrefixReport] = field(default_factory=list)
    indexes: list[IndexReport] = field(default_factory=list)
    failures: FailureReport = field(default_factory=FailureReport)

    @property
    def has_indexing(self) -> bool:
        return any(
            ir.target_indexing or ir.target_percent_indexed < 1.0
            for ir in self.indexes
        )

    @property
    def passed(self) -> bool:
        prefix_ok = all(
            r.count_match and r.mismatched == 0 and r.missing_on_target == 0
            for r in self.prefixes
        )
        index_ok = all(r.smoke_queries_failed == 0 for r in self.indexes)
        failure_ok = len(self.failures.issues) == 0
        return prefix_ok and index_ok and failure_ok


async def get_connections(
    name: str,
) -> tuple[Redis, Redis]:
    """Create decoded (meta) and raw (data) connections for an endpoint."""
    cfg = ENDPOINTS[name]
    host = str(cfg["host"])
    port = int(cfg["port"])  # type: ignore[arg-type]
    password = str(cfg["password"]) if cfg["password"] else None

    meta: Redis = Redis(host=host, port=port, password=password, decode_responses=True)
    raw: Redis = Redis(host=host, port=port, password=password, decode_responses=False)
    await meta.ping()  # type: ignore[misc]
    await raw.ping()  # type: ignore[misc]
    print(f"   Connected to {cfg['label']} ({host}:{port})")
    return meta, raw


async def scan_keys(redis: Redis, prefix: str) -> list[str]:
    """Scan all keys matching a prefix."""
    pattern = f"{prefix}*"
    keys: list[str] = []
    cursor: int = 0
    while True:
        cursor, batch = await redis.scan(cursor=cursor, match=pattern, count=1000)
        keys.extend(batch)
        if cursor == 0:
            break
    return keys


async def count_keys(redis: Redis, prefix: str) -> int:
    """Count keys matching a prefix."""
    pattern = f"{prefix}*"
    count = 0
    cursor: int = 0
    while True:
        cursor, batch = await redis.scan(cursor=cursor, match=pattern, count=1000)
        count += len(batch)
        if cursor == 0:
            break
    return count


# ---------------------------------------------------------------------------
# Level 1: Structural Validation
# ---------------------------------------------------------------------------


async def validate_structure(
    source_meta: Redis,
    target_meta: Redis,
    prefixes: list[str],
    report: ValidationReport,
) -> None:
    """Compare key counts and index metadata between source and target."""
    print("--- Level 1: Structural Validation ---")

    report.source_dbsize = await source_meta.dbsize()
    report.target_dbsize = await target_meta.dbsize()
    print(f"   DBSIZE  source={report.source_dbsize:,}  target={report.target_dbsize:,}")

    for prefix in prefixes:
        src_count = await count_keys(source_meta, prefix)
        tgt_count = await count_keys(target_meta, prefix)
        match = src_count == tgt_count
        marker = "OK" if match else "MISMATCH"

        pr = PrefixReport(
            prefix=prefix,
            source_count=src_count,
            target_count=tgt_count,
            count_match=match,
        )
        report.prefixes.append(pr)
        print(
            f"   {prefix:15s}  source={src_count:>8,}  "
            f"target={tgt_count:>8,}  [{marker}]"
        )

    # Index-level structural check
    try:
        source_indices: list[str] = await source_meta.execute_command("FT._LIST")
    except Exception:
        source_indices = []
    try:
        target_indices: list[str] = await target_meta.execute_command("FT._LIST")
    except Exception:
        target_indices = []

    relevant_indices = set()
    for prefix in prefixes:
        idx = PREFIX_TO_INDEX.get(prefix)
        if idx:
            relevant_indices.add(idx)

    for idx_name in relevant_indices:
        ir = IndexReport(name=idx_name)
        ir.source_exists = idx_name in source_indices
        ir.target_exists = idx_name in target_indices

        if ir.source_exists:
            try:
                info = await source_meta.ft(idx_name).info()
                ir.source_num_docs = int(info.get("num_docs", 0))
            except Exception:
                pass

        if ir.target_exists:
            try:
                info = await target_meta.ft(idx_name).info()
                ir.target_num_docs = int(info.get("num_docs", 0))
                ir.target_indexing = str(info.get("indexing", "0")) == "1"
                ir.target_percent_indexed = float(
                    info.get("percent_indexed", 1)
                )
            except Exception:
                pass

        ir.num_docs_match = ir.source_num_docs == ir.target_num_docs
        if ir.num_docs_match:
            marker = "OK"
        elif ir.target_indexing or ir.target_percent_indexed < 1.0:
            marker = "INDEXING"
        else:
            marker = "MISMATCH"
        exists = "OK" if ir.target_exists else "MISSING"

        indexing_note = ""
        if ir.target_indexing or ir.target_percent_indexed < 1.0:
            indexing_note = f"  ({ir.target_percent_indexed:.0%} indexed)"

        print(
            f"   {idx_name:15s}  source_docs={ir.source_num_docs:>8,}  "
            f"target_docs={ir.target_num_docs:>8,}  "
            f"exists=[{exists}]  docs=[{marker}]{indexing_note}"
        )
        report.indexes.append(ir)

    print()


# ---------------------------------------------------------------------------
# Level 2: Content Integrity
# ---------------------------------------------------------------------------


async def validate_content(
    source_meta: Redis,
    source_raw: Redis,
    target_raw: Redis,
    prefixes: list[str],
    report: ValidationReport,
    sample_size: int = 100,
    full_compare: bool = False,
) -> None:
    """Compare raw DUMP bytes for sampled keys."""
    print("--- Level 2: Content Integrity ---")

    for pr in report.prefixes:
        prefix = pr.prefix
        keys = await scan_keys(source_meta, prefix)

        if not keys:
            print(f"   {prefix}: no keys to sample")
            continue

        if full_compare:
            sample = keys
        else:
            sample = random.sample(keys, min(sample_size, len(keys)))

        pr.sampled = len(sample)

        batch_size = 200
        for i in range(0, len(sample), batch_size):
            batch = sample[i : i + batch_size]

            src_pipe = source_raw.pipeline()
            tgt_pipe = target_raw.pipeline()
            for key in batch:
                src_pipe.dump(key)
                tgt_pipe.dump(key)

            src_dumps = await src_pipe.execute()
            tgt_dumps = await tgt_pipe.execute()

            for j, key in enumerate(batch):
                src_val = src_dumps[j]
                tgt_val = tgt_dumps[j]

                if src_val is None and tgt_val is None:
                    pr.matched += 1
                elif tgt_val is None:
                    pr.missing_on_target += 1
                    if len(pr.missing_keys) < 10:
                        pr.missing_keys.append(key)
                elif src_val != tgt_val:
                    pr.mismatched += 1
                    if len(pr.mismatched_keys) < 10:
                        pr.mismatched_keys.append(key)
                else:
                    pr.matched += 1

        status = "PASS" if (pr.mismatched == 0 and pr.missing_on_target == 0) else "FAIL"
        print(
            f"   {prefix:15s}  sampled={pr.sampled:,}  "
            f"matched={pr.matched:,}  "
            f"mismatched={pr.mismatched}  "
            f"missing={pr.missing_on_target}  [{status}]"
        )
        if pr.mismatched_keys:
            print(f"      Mismatched keys (first 10): {pr.mismatched_keys}")
        if pr.missing_keys:
            print(f"      Missing keys (first 10): {pr.missing_keys}")

    print()


# ---------------------------------------------------------------------------
# Level 3: Search Smoke Tests
# ---------------------------------------------------------------------------


async def validate_search(
    source_meta: Redis,
    target_meta: Redis,
    report: ValidationReport,
) -> None:
    """Run FT.SEARCH smoke queries on each index."""
    print("--- Level 3: Search Smoke Tests ---")

    for ir in report.indexes:
        if not ir.source_exists or not ir.target_exists:
            ir.smoke_queries_failed += 1
            ir.smoke_failures.append("index missing on one side")
            print(
                f"   {ir.name}: SKIP "
                f"(missing on {'source' if not ir.source_exists else 'target'})"
            )
            continue

        if ir.target_indexing or ir.target_percent_indexed < 1.0:
            print(
                f"   {ir.name}: SKIP "
                f"(still indexing, {ir.target_percent_indexed:.0%} complete)"
            )
            continue

        queries = [
            ("wildcard", "*", "LIMIT", "0", "1", "TIMEOUT", "30000"),
            ("count_only", "*", "LIMIT", "0", "0", "TIMEOUT", "30000"),
        ]

        for label, *query_args in queries:
            try:
                src_result = await source_meta.execute_command(
                    "FT.SEARCH", ir.name, *query_args
                )
                tgt_result = await target_meta.execute_command(
                    "FT.SEARCH", ir.name, *query_args
                )

                src_total = src_result[0] if src_result else 0
                tgt_total = tgt_result[0] if tgt_result else 0

                if src_total == tgt_total:
                    ir.smoke_queries_passed += 1
                    status = "PASS"
                else:
                    ir.smoke_queries_failed += 1
                    ir.smoke_failures.append(
                        f"{label}: source={src_total} target={tgt_total}"
                    )
                    status = "FAIL"

                print(
                    f"   {ir.name} [{label}]  "
                    f"source={src_total}  target={tgt_total}  [{status}]"
                )

            except Exception as e:
                ir.smoke_queries_failed += 1
                ir.smoke_failures.append(f"{label}: {e}")
                print(f"   {ir.name} [{label}]  ERROR: {e}")

    print()


# ---------------------------------------------------------------------------
# Level 4: Failure Detection
# ---------------------------------------------------------------------------


async def validate_failures(
    target_meta: Redis,
    report: ValidationReport,
) -> None:
    """Detect partial transfer states on the target."""
    print("--- Level 4: Failure Detection ---")

    for ir in report.indexes:
        prefix = None
        for p, idx in PREFIX_TO_INDEX.items():
            if idx == ir.name:
                prefix = p
                break

        if not prefix:
            continue

        target_key_count = 0
        for pr in report.prefixes:
            if pr.prefix == prefix:
                target_key_count = pr.target_count
                break

        if target_key_count > 0 and not ir.target_exists:
            msg = f"{ir.name}: keys exist ({target_key_count:,}) but index is MISSING"
            report.failures.issues.append(msg)
            print(f"   ISSUE: {msg}")

        still_indexing = ir.target_indexing or ir.target_percent_indexed < 1.0

        if ir.target_exists and ir.target_num_docs == 0 and target_key_count > 0:
            if still_indexing:
                print(
                    f"   INFO: {ir.name}: indexing in progress "
                    f"({ir.target_percent_indexed:.0%}), "
                    f"{target_key_count:,} keys waiting"
                )
            else:
                msg = (
                    f"{ir.name}: index exists but num_docs=0 "
                    f"with {target_key_count:,} keys present"
                )
                report.failures.issues.append(msg)
                print(f"   ISSUE: {msg}")

        if ir.source_exists and ir.target_exists and not still_indexing and ir.source_num_docs > 0 and ir.target_num_docs > 0:
            ratio = ir.target_num_docs / ir.source_num_docs
            if ratio < 0.95:
                msg = (
                    f"{ir.name}: target has only {ratio:.0%} of source docs "
                    f"({ir.target_num_docs:,} vs {ir.source_num_docs:,})"
                )
                report.failures.issues.append(msg)
                print(f"   ISSUE: {msg}")

    if not report.failures.issues:
        print("   No issues detected")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def main(
    source_name: str = "public",
    target_name: str = "scratch",
    prefixes: list[str] | None = None,
    sample_size: int = 100,
    full_compare: bool = False,
    output_json: bool = False,
) -> int:
    """Run all validation levels."""
    if prefixes is None:
        prefixes = ALL_PREFIXES

    print("=" * 60)
    print("Validate Clone")
    print("=" * 60)
    print()
    print(f"   Source: {source_name}")
    print(f"   Target: {target_name}")
    print(f"   Prefixes: {', '.join(prefixes)}")
    print(f"   Sample size: {'FULL' if full_compare else sample_size}")
    print()

    start = time.monotonic()

    # --- Connect ---
    print("Connecting...")
    try:
        source_meta, source_raw = await get_connections(source_name)
    except Exception as e:
        print(f"   FAILED to connect to {source_name}: {e}")
        if source_name == "public":
            print("   Tip: Make sure the IAP tunnel is running (make tunnel)")
        return 1

    try:
        target_meta, target_raw = await get_connections(target_name)
    except Exception as e:
        print(f"   FAILED to connect to {target_name}: {e}")
        if target_name == "scratch":
            print("   Tip: Start scratch Redis with: make scratch-redis-up")
        await source_meta.aclose()
        await source_raw.aclose()
        return 1
    print()

    report = ValidationReport(source=source_name, target=target_name)

    # --- Level 1 ---
    await validate_structure(source_meta, target_meta, prefixes, report)

    # --- Level 2 ---
    await validate_content(
        source_meta, source_raw, target_raw,
        prefixes, report,
        sample_size=sample_size, full_compare=full_compare,
    )

    # --- Level 3 ---
    await validate_search(source_meta, target_meta, report)

    # --- Level 4 ---
    await validate_failures(target_meta, report)

    # --- Summary ---
    report.elapsed_seconds = time.monotonic() - start

    print("=" * 60)
    if report.passed:
        verdict = "PASS"
    elif report.has_indexing:
        verdict = "PASS (indexing still in progress -- re-run later for full smoke)"
    else:
        verdict = "FAIL"
    print(f"Validation Result: {verdict}")
    print("=" * 60)

    total_sampled = sum(pr.sampled for pr in report.prefixes)
    total_matched = sum(pr.matched for pr in report.prefixes)
    total_mismatched = sum(pr.mismatched for pr in report.prefixes)
    total_missing = sum(pr.missing_on_target for pr in report.prefixes)
    total_smoke_pass = sum(ir.smoke_queries_passed for ir in report.indexes)
    total_smoke_fail = sum(ir.smoke_queries_failed for ir in report.indexes)

    print(f"   Structural: {sum(1 for p in report.prefixes if p.count_match)}"
          f"/{len(report.prefixes)} prefixes match")
    print(f"   Content: {total_matched:,}/{total_sampled:,} keys match"
          f"  ({total_mismatched} mismatched, {total_missing} missing)")
    print(f"   Search: {total_smoke_pass}/{total_smoke_pass + total_smoke_fail}"
          f" smoke queries passed")
    print(f"   Failures: {len(report.failures.issues)} issues detected")
    print(f"   Time: {report.elapsed_seconds:.1f}s")
    print()

    if output_json:
        json_report = {
            "passed": report.passed,
            "source": report.source,
            "target": report.target,
            "source_dbsize": report.source_dbsize,
            "target_dbsize": report.target_dbsize,
            "elapsed_seconds": round(report.elapsed_seconds, 1),
            "prefixes": [asdict(pr) for pr in report.prefixes],
            "indexes": [asdict(ir) for ir in report.indexes],
            "failures": asdict(report.failures),
        }
        print(json.dumps(json_report, indent=2))

    # --- Cleanup ---
    await source_meta.aclose()
    await source_raw.aclose()
    await target_meta.aclose()
    await target_raw.aclose()

    if report.passed:
        return 0
    # Data is correct but indexes still building -- not a failure
    if report.has_indexing and len(report.failures.issues) == 0:
        prefix_ok = all(
            r.count_match and r.mismatched == 0 and r.missing_on_target == 0
            for r in report.prefixes
        )
        if prefix_ok:
            return 0
    return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Validate Redis clone integrity between source and target"
    )
    parser.add_argument(
        "--source",
        choices=["public", "local", "scratch"],
        default="public",
        help="Source Redis endpoint (default: public)",
    )
    parser.add_argument(
        "--target",
        choices=["scratch", "local"],
        default="scratch",
        help="Target Redis endpoint (default: scratch)",
    )
    parser.add_argument(
        "--prefixes",
        nargs="+",
        default=None,
        help="Key prefixes to validate (default: all known prefixes)",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=100,
        help="Keys to sample per prefix for content comparison (default: 100)",
    )
    parser.add_argument(
        "--full-compare",
        action="store_true",
        help="Compare every key, not just a sample (expensive)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output machine-readable JSON report",
    )

    args = parser.parse_args()

    exit_code = asyncio.run(
        main(
            source_name=args.source,
            target_name=args.target,
            prefixes=args.prefixes,
            sample_size=args.sample_size,
            full_compare=args.full_compare,
            output_json=args.json,
        )
    )
    sys.exit(exit_code)
