import asyncio
import json
import os
from pathlib import Path
from uuid import uuid4

import pytest
from redis.asyncio import Redis

from adapters.config import load_env
from ai.microgenre_batch import build_microgenre_input_from_document, collect_batch_records
from ai.microgenre_batch_models import (
    MicroGenreBatchConfig,
    MicroGenreBatchSidecarRecord,
)
from ai.prompts.microgenre_classifier import (
    MicroGenreClassifyInput,
    MicroGenreScoreResult,
    score_microgenres,
)
from scripts.backfill_microgenres import BackfillStats, backfill_rows

load_env()

pytestmark = pytest.mark.integration


def test_live_microgenre_classifier_movie_contract() -> None:
    _require_openai()

    response = asyncio.run(
        score_microgenres(
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
            )
        )
    )

    _assert_successful_contract(response.result, response.error)


def test_live_microgenre_classifier_tv_contract() -> None:
    _require_openai()

    response = asyncio.run(
        score_microgenres(
            MicroGenreClassifyInput(
                title="The Office",
                media_type="tv",
                year=2005,
                first_air_date="2005-03-24",
                summary=(
                    "A mockumentary workplace sitcom about employees at the "
                    "Scranton branch of a paper company."
                ),
                genres=["comedy"],
                keywords=["mockumentary", "workplace", "sitcom"],
                enable_web_search=False,
            )
        )
    )

    _assert_successful_contract(response.result, response.error)


def test_collect_batch_records_from_redis_media_doc() -> None:
    asyncio.run(_test_collect_batch_records_from_redis_media_doc())


def test_backfill_rows_patches_compact_microgenre_shape() -> None:
    asyncio.run(_test_backfill_rows_patches_compact_microgenre_shape())


async def _test_collect_batch_records_from_redis_media_doc() -> None:
    redis = _redis()
    key_suffix = uuid4().hex
    mc_id = f"tmdb_movie_000000_{key_suffix}"
    key = f"media:{mc_id}"
    doc = _media_doc(mc_id)
    try:
        await _require_redis(redis)
        await redis.json().set(key, "$", doc)
        await redis.expire(key, 300)

        config = MicroGenreBatchConfig(media_type="movie", take=10, rt_threshold=None)
        records = await collect_batch_records(redis, config)
        record = next((item for item in records if item.mc_id == mc_id), None)
        assert record is not None
        assert record.title == "Integration Test Movie"
        assert record.tmdb_id == doc["source_id"]

        classifier_input = build_microgenre_input_from_document(
            record.media_doc, record.media_type, 0.1
        )
        assert classifier_input.title == "Integration Test Movie"
        assert classifier_input.tmdb_id == doc["source_id"]
        assert classifier_input.genres == ["thriller", "mystery"]
    finally:
        await redis.aclose()


async def _test_backfill_rows_patches_compact_microgenre_shape() -> None:
    redis = _redis()
    key_suffix = uuid4().hex
    mc_id = f"tmdb_movie_000001_{key_suffix}"
    key = f"media:{mc_id}"
    try:
        await _require_redis(redis)
        await redis.json().set(key, "$", _media_doc(mc_id))
        await redis.expire(key, 300)

        row = MicroGenreBatchSidecarRecord(
            run_id="integration",
            input_position=0,
            internal_idx=0,
            mc_id=mc_id,
            media_type="movie",
            title="Integration Test Movie",
            tmdb_id="1000001",
            score_threshold=0.1,
            taxonomy_version="1.1",
            taxonomy_hash="taxonomy",
            prompt_version="prompt",
            prompt_hash="prompt_hash",
            classification=MicroGenreScoreResult(
                microgenre_scores={"thriller.mystery.noir": 0.8},
                top_ids=["thriller.mystery.noir"],
                confidence=0.9,
                rationale="Noir mystery shape.",
                unknown=False,
                unknown_reason=None,
                score_threshold=0.1,
                taxonomy_version="1.1",
                taxonomy_hash="taxonomy",
                prompt_version="prompt",
                prompt_hash="prompt_hash",
            ),
            error=None,
            error_type=None,
            error_detail=None,
            raw_response_excerpt=None,
            execution_time=1.0,
            classified_at=1.0,
        )

        stats = await backfill_rows(redis, [row], BackfillStats(), dry_run=False, force=True)
        assert stats.updated == 1

        stored = await redis.json().get(key, "$.microgenres")
        microgenres = stored[0] if isinstance(stored, list) else stored
        assert microgenres == {
            "confidence": 0.9,
            "microgenre_scores": {"thriller.mystery.noir": 0.8},
            "rationale": "Noir mystery shape.",
        }
    finally:
        await redis.aclose()


def _assert_successful_contract(
    result: MicroGenreScoreResult | None,
    error: str | None,
) -> None:
    assert error is None
    assert result is not None
    assert result.unknown is False
    assert result.microgenre_scores
    assert result.top_ids
    assert set(result.top_ids).issubset(result.microgenre_scores)
    assert 0.0 <= result.confidence <= 1.0
    assert result.rationale
    assert result.taxonomy_version
    assert result.taxonomy_hash
    assert result.prompt_version
    assert result.prompt_hash


def _require_openai() -> None:
    if not os.getenv("OPENAI_API_KEY"):
        pytest.skip("OPENAI_API_KEY is required for live microgenre integration tests")


def _redis() -> Redis:  # type: ignore[type-arg]
    return Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6380")),
        password=os.getenv("REDIS_PASSWORD") or None,
        decode_responses=True,
    )


async def _require_redis(redis: Redis) -> None:  # type: ignore[type-arg]
    try:
        await redis.ping()  # type: ignore[misc]
    except Exception as exc:
        pytest.skip(f"Redis is required for microgenre Redis integration tests: {exc}")


def _media_doc(mc_id: str) -> dict[str, object]:
    return {
        "id": mc_id,
        "mc_id": mc_id,
        "title": "Integration Test Movie",
        "search_title": "Integration Test Movie",
        "mc_type": "movie",
        "source": "tmdb",
        "source_id": "1000001",
        "year": 2024,
        "release_date": "2024-01-01",
        "overview": "A detective follows a shadowy conspiracy through a coastal city.",
        "genres": ["thriller", "mystery"],
        "keywords": ["noir", "detective"],
        "external_ids": {"imdb_id": "tt0000001"},
        "rt_audience_score": 0.9,
    }


def test_example_entry_documents_expected_redis_shape() -> None:
    example_path = (
        Path(__file__).resolve().parents[3]
        / "data"
        / "microgenre-classifications"
        / "example_entry.json"
    )
    payload = json.loads(example_path.read_text(encoding="utf-8"))
    assert payload[1] == {
        "microgenres": {
            "confidence": 0.86,
            "microgenre_scores": {
                "comedy.dark.social_satire": 0.18,
                "comedy.dramedy.prestige": 0.78,
                "comedy.romcom.modern": 0.22,
                "comedy.sitcom.singlecamera": 0.62,
                "drama.character.comingofage": 0.38,
                "drama.character.workplace": 0.28,
                "drama.social.class": 0.12,
                "drama.social.lgbtq": 0.55,
            },
            "rationale": (
                "Contemporary half-hour comedy-drama structure is the strongest fit, "
                "centered on friendship, ambition, identity, and emotional self-discovery. "
                "Its single-camera, slice-of-life tone and focus on a queer Black protagonist "
                "give it meaningful LGBTQ+ drama overlap, with smaller workplace and romance "
                "elements."
            ),
        }
    }
