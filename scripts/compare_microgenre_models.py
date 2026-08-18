#!/usr/bin/env python3
"""Side-by-side microgenre scoring across providers/models.

Default lineup: Cerebras gpt-oss-120b, OpenAI gpt-5.6-terra, gpt-4o-mini.

Uses the same classifier prompt/contract as production. Web search is off.
AI Redis cache is disabled for this run so models are not cross-contaminated.

Usage:
    source venv/bin/activate
    REDIS=dev python scripts/compare_microgenre_models.py
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root / "src"))
sys.path.insert(0, str(_project_root))

from adapters.config import load_env  # noqa: E402

load_env()

from openai import AsyncOpenAI  # noqa: E402

from ai.prompts.microgenre_classifier import (  # noqa: E402
    LLM_MAX_TOKENS,
    LLM_TEMPERATURE,
    LLM_TIMEOUT_SECONDS,
    MicroGenreClassifyInput,
    _build_result,
    _extract_json_response,
    _format_title_context,
)
from ai.prompts.microgenre_prompts import MICROGENRE_SCORER_PROMPT  # noqa: E402
from ai.prompts.microgenre_taxonomy import TAXONOMY_BLOCK  # noqa: E402
from ai.providers.models import CerebrasModels, OpenAIModels  # noqa: E402
from utils.parse_json import parse_json  # noqa: E402
from utils.redis_cache import disable_cache  # noqa: E402

disable_cache()

ApiPath = Literal["responses", "chat"]


@dataclass(frozen=True)
class ModelSpec:
    label: str
    provider: Literal["openai", "cerebras"]
    model: str
    api_path: ApiPath


DEFAULT_CASES: list[MicroGenreClassifyInput] = [
    MicroGenreClassifyInput(
        title="Get Out",
        media_type="movie",
        year=2017,
        release_date="2017-02-24",
        summary=(
            "A young Black man uncovers disturbing secrets while visiting "
            "his white girlfriend's family estate."
        ),
        genres=["horror", "thriller", "comedy"],
        keywords=["social thriller", "satire", "race"],
        enable_web_search=False,
    ),
    MicroGenreClassifyInput(
        title="The Office",
        media_type="tv",
        year=2005,
        first_air_date="2005-03-24",
        summary=(
            "A mockumentary on a group of typical office workers, where the "
            "workday consists of ego clashes, inappropriate behavior, and tedium."
        ),
        genres=["comedy"],
        keywords=["workplace", "mockumentary", "ensemble"],
        enable_web_search=False,
    ),
    MicroGenreClassifyInput(
        title="Rafa",
        media_type="tv",
        year=2025,
        first_air_date="2025-01-01",
        summary=(
            "A documentary-style series following tennis champion Rafael Nadal "
            "through career milestones, training, and personal life."
        ),
        genres=["documentary", "sport"],
        keywords=["tennis", "biography", "sports"],
        enable_web_search=False,
    ),
]


MODELS: list[ModelSpec] = [
    ModelSpec(
        label="cerebras/gpt-oss-120b",
        provider="cerebras",
        model=CerebrasModels.GPT_OSS.value,
        api_path="chat",
    ),
    ModelSpec(
        label="openai/gpt-5.6-terra",
        provider="openai",
        model=OpenAIModels.GPT_5_6_TERRA.value,
        api_path="responses",
    ),
    ModelSpec(
        label="openai/gpt-4o-mini",
        provider="openai",
        model="gpt-4o-mini",
        api_path="chat",
    ),
]


def _client_for(spec: ModelSpec, timeout: int) -> AsyncOpenAI:
    if spec.provider == "openai":
        return AsyncOpenAI(
            api_key=os.getenv("OPENAI_API_KEY"),
            organization=os.getenv("OPENAI_ORGANIZATION"),
            timeout=timeout,
        )
    return AsyncOpenAI(
        api_key=os.getenv("CEREBRAS_API_KEY"),
        base_url="https://api.cerebras.ai/v1",
        timeout=timeout,
    )


def _build_prompt(case: MicroGenreClassifyInput) -> str:
    return MICROGENRE_SCORER_PROMPT.format(
        taxonomy_block=TAXONOMY_BLOCK,
        title_context=_format_title_context(case),
        score_threshold=case.score_threshold,
    )


async def _call_chat(
    client: AsyncOpenAI,
    model: str,
    prompt: str,
    timeout: int,
) -> tuple[dict[str, Any] | None, str | None, str | None]:
    raw = await client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        max_completion_tokens=LLM_MAX_TOKENS,
        temperature=LLM_TEMPERATURE,
        response_format={"type": "json_object"},
        timeout=float(timeout),
    )
    text = raw.choices[0].message.content if raw.choices else None
    if not text:
        return None, None, "empty chat completion content"
    try:
        parsed = parse_json(text)
    except (json.JSONDecodeError, AttributeError) as exc:
        return None, text, f"json parse failed: {exc}"
    if not isinstance(parsed, dict):
        return None, text, "parsed JSON was not an object"
    return parsed, text, None


async def _call_responses(
    client: AsyncOpenAI,
    model: str,
    prompt: str,
    timeout: int,
) -> tuple[dict[str, Any] | None, str | None, str | None]:
    raw = await client.responses.create(
        model=model,
        input=prompt,
        max_output_tokens=LLM_MAX_TOKENS,
        temperature=LLM_TEMPERATURE,
        reasoning={"effort": "low"},
        text={"format": {"type": "json_object"}},
        timeout=float(timeout),
    )
    text = getattr(raw, "output_text", None)
    if not isinstance(text, str) or not text:
        return None, None, "Responses API returned no output text"
    try:
        parsed = parse_json(text)
    except (json.JSONDecodeError, AttributeError) as exc:
        return None, text, f"json parse failed: {exc}"
    if not isinstance(parsed, dict):
        return None, text, "parsed JSON was not an object"
    return parsed, text, None


async def score_one(
    spec: ModelSpec,
    case: MicroGenreClassifyInput,
    timeout: int,
) -> dict[str, Any]:
    prompt = _build_prompt(case)
    client = _client_for(spec, timeout)
    t0 = time.perf_counter()
    try:
        if spec.api_path == "responses":
            parsed, text, api_error = await _call_responses(client, spec.model, prompt, timeout)
        else:
            parsed, text, api_error = await _call_chat(client, spec.model, prompt, timeout)
    except Exception as exc:  # noqa: BLE001 — comparison harness should surface provider errors
        return {
            "model": spec.label,
            "elapsed_s": round(time.perf_counter() - t0, 3),
            "error": str(exc),
            "top_ids": [],
            "confidence": None,
            "unknown": None,
            "rationale": None,
            "scores": {},
        }
    finally:
        await client.close()

    elapsed = round(time.perf_counter() - t0, 3)
    if api_error or parsed is None:
        return {
            "model": spec.label,
            "elapsed_s": elapsed,
            "error": api_error or "no parsed payload",
            "top_ids": [],
            "confidence": None,
            "unknown": None,
            "rationale": None,
            "scores": {},
            "raw_excerpt": (text or "")[:400],
        }

    extracted = _extract_json_response(parsed, text or "")
    if extracted is None:
        return {
            "model": spec.label,
            "elapsed_s": elapsed,
            "error": "could not extract JSON object",
            "top_ids": [],
            "confidence": None,
            "unknown": None,
            "rationale": None,
            "scores": {},
        }

    result, contract_error = _build_result(extracted, case.score_threshold)
    if contract_error is not None or result is None:
        return {
            "model": spec.label,
            "elapsed_s": elapsed,
            "error": contract_error or "contract build failed",
            "top_ids": [],
            "confidence": None,
            "unknown": None,
            "rationale": None,
            "scores": {},
        }

    return {
        "model": spec.label,
        "elapsed_s": elapsed,
        "error": None,
        "top_ids": result.top_ids,
        "confidence": result.confidence,
        "unknown": result.unknown,
        "unknown_reason": result.unknown_reason,
        "rationale": result.rationale,
        "scores": dict(
            sorted(result.microgenre_scores.items(), key=lambda item: item[1], reverse=True)
        ),
    }


def _print_case(title: str, media_type: str, rows: list[dict[str, Any]]) -> None:
    print("\n" + "=" * 88)
    print(f"{title} ({media_type})")
    print("=" * 88)
    for row in rows:
        print(f"\n--- {row['model']}  ({row['elapsed_s']}s) ---")
        if row.get("error"):
            print(f"ERROR: {row['error']}")
            if row.get("raw_excerpt"):
                print(f"raw: {row['raw_excerpt']}")
            continue
        print(f"unknown={row['unknown']}  confidence={row['confidence']}")
        print(f"top_ids={row['top_ids']}")
        print(f"scores={json.dumps(row['scores'], ensure_ascii=False)}")
        rationale = row.get("rationale") or ""
        if rationale:
            print(f"rationale={rationale[:500]}")


async def run(cases: list[MicroGenreClassifyInput], timeout: int) -> list[dict[str, Any]]:
    report: list[dict[str, Any]] = []
    for case in cases:
        print(f"\nScoring: {case.title} ({case.media_type}) ...", flush=True)
        # Sequential per model so latency is comparable and rate limits stay sane.
        rows: list[dict[str, Any]] = []
        for spec in MODELS:
            print(f"  -> {spec.label}", flush=True)
            row = await score_one(spec, case, timeout)
            rows.append(row)
        _print_case(case.title, case.media_type, rows)
        report.append(
            {
                "title": case.title,
                "media_type": case.media_type,
                "results": rows,
            }
        )
    return report


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare microgenre models side-by-side")
    parser.add_argument(
        "--timeout",
        type=int,
        default=LLM_TIMEOUT_SECONDS,
        help="Per-request timeout seconds",
    )
    parser.add_argument(
        "--json-out",
        type=Path,
        default=None,
        help="Optional path to write full comparison JSON",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    missing = [
        name
        for name, present in (
            ("OPENAI_API_KEY", bool(os.getenv("OPENAI_API_KEY"))),
            ("CEREBRAS_API_KEY", bool(os.getenv("CEREBRAS_API_KEY"))),
        )
        if not present
    ]
    if missing:
        raise SystemExit(f"Missing required env: {', '.join(missing)}")

    report = asyncio.run(run(DEFAULT_CASES, args.timeout))
    if args.json_out is not None:
        args.json_out.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
        print(f"\nWrote {args.json_out}")


if __name__ == "__main__":
    main()
