"""Hermetic tests for POST /api/flixpatrol."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import services.search_service as search_service
import web.app as web_app
from web.app import api_get_flixpatrol

@pytest.fixture()
def flixpatrol_app(monkeypatch: pytest.MonkeyPatch) -> FastAPI:
    app = FastAPI()
    app.post("/api/flixpatrol")(api_get_flixpatrol)
    return app


@pytest.fixture()
def client(flixpatrol_app: FastAPI) -> TestClient:
    return TestClient(flixpatrol_app)


async def _fake_batch(
    mc_ids: list[str],
    mc_type: str,
    mc_subtype: str | None = None,
) -> list[dict[str, Any]]:
    del mc_type, mc_subtype
    out: list[dict[str, Any]] = []
    for mid in mc_ids:
        if mid == "tmdb_movie_missing":
            out.append(
                {
                    "mc_id": mid,
                    "error": "Not found in index",
                    "status_code": 404,
                }
            )
        elif mid == "tmdb_movie_empty":
            out.append({"mc_id": mid, "flixpatrol": None})
        else:
            out.append(
                {
                    "mc_id": mid,
                    "flixpatrol": {
                        "peak_total": 516,
                        "peak_rank": 10,
                        "records": [],
                    },
                }
            )
    return out


class TestFlixPatrolEndpoint:
    def test_requires_mc_id(self, client: TestClient) -> None:
        response = client.post("/api/flixpatrol", params={"mc_type": "movie"})
        assert response.status_code == 400

    def test_rejects_non_media_type(self, client: TestClient) -> None:
        response = client.post(
            "/api/flixpatrol",
            params={"mc_id": "tmdb_movie_1", "mc_type": "person"},
        )
        assert response.status_code == 400

    def test_single_success(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(web_app, "get_flixpatrol_batch", _fake_batch)
        response = client.post(
            "/api/flixpatrol",
            params={"mc_id": "tmdb_movie_671", "mc_type": "movie"},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["mc_id"] == "tmdb_movie_671"
        assert body["flixpatrol"]["peak_total"] == 516

    def test_single_missing_history_returns_null(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(web_app, "get_flixpatrol_batch", _fake_batch)
        response = client.post(
            "/api/flixpatrol",
            params={"mc_id": "tmdb_movie_empty", "mc_type": "movie"},
        )
        assert response.status_code == 200
        assert response.json() == {"mc_id": "tmdb_movie_empty", "flixpatrol": None}

    def test_single_missing_media_returns_404(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(web_app, "get_flixpatrol_batch", _fake_batch)
        response = client.post(
            "/api/flixpatrol",
            params={"mc_id": "tmdb_movie_missing", "mc_type": "movie"},
        )
        assert response.status_code == 404

    def test_batch_preserves_order(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(web_app, "get_flixpatrol_batch", _fake_batch)
        response = client.post(
            "/api/flixpatrol",
            params={
                "mc_ids": "tmdb_movie_671,tmdb_movie_empty,tmdb_movie_missing",
                "mc_type": "movie",
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert len(body) == 3
        assert body[0]["mc_id"] == "tmdb_movie_671"
        assert body[1]["flixpatrol"] is None
        assert body[2]["status_code"] == 404


class TestFlixPatrolBatchService:
    @pytest.mark.asyncio
    async def test_reads_full_history_from_sidecar(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        full_history = {
            "peak_total": 100,
            "records": [{"ranking": 1}, {"ranking": 2}, {"ranking": 3}],
        }

        async def mget(keys: list[str], _path: str) -> list[object]:
            if keys[0].startswith("media:"):
                return [
                    [{"mc_id": "tmdb_movie_1"}],
                    [None],
                ]
            return [
                [full_history],
                [None],
            ]

        json_module = MagicMock()
        json_module.mget = AsyncMock(side_effect=mget)
        redis = MagicMock()
        redis.json.return_value = json_module
        monkeypatch.setattr(search_service, "get_redis", lambda: redis)

        results = await search_service.get_flixpatrol_batch(
            ["tmdb_movie_1", "tmdb_movie_2"],
            "movie",
        )
        assert results[0] == {
            "mc_id": "tmdb_movie_1",
            "flixpatrol": full_history,
        }
        assert results[1]["mc_id"] == "tmdb_movie_2"
        assert results[1]["status_code"] == 404

    @pytest.mark.asyncio
    async def test_media_without_sidecar_returns_null(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def mget(keys: list[str], _path: str) -> list[object]:
            if keys[0].startswith("media:"):
                return [[{"mc_id": "tmdb_movie_1", "flixpatrol": {"peak_total": 1}}]]
            return [[None]]

        json_module = MagicMock()
        json_module.mget = AsyncMock(side_effect=mget)
        redis = MagicMock()
        redis.json.return_value = json_module
        monkeypatch.setattr(search_service, "get_redis", lambda: redis)

        results = await search_service.get_flixpatrol_batch(["tmdb_movie_1"], "movie")
        assert results[0] == {"mc_id": "tmdb_movie_1", "flixpatrol": None}
