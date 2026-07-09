"""
Hermetic API tests for the public-lists routes.

Mounts the router on a minimal FastAPI app with the service layer
monkeypatched, so no Redis is required. Verifies:
- write endpoints (upsert/delete/index-create) reject unauthenticated
  callers in a Cloud Run environment
- write endpoints accept the shared secret
- read endpoints stay public
- request validation (path/payload list_id mismatch, bad sort)
"""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import services.public_list_service as service_module
import web.routes.public_lists as routes_module
from core.public_lists import PublicListUpsertRequest
from web.auth import verify_public_list_write_key
from web.routes.public_lists import router

pytestmark = pytest.mark.unit

WRITE_KEY = "test-public-lists-key"


@pytest.fixture()
def cloud_run_env(monkeypatch: pytest.MonkeyPatch):
    """Simulate Cloud Run so local-dev auth bypass is inactive."""
    monkeypatch.setenv("K_SERVICE", "media-circle-search")
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.setenv("PUBLIC_LISTS_API_KEY", WRITE_KEY)
    monkeypatch.delenv("ETL_API_KEY", raising=False)


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """TestClient over a minimal app with the service layer stubbed."""
    stored: dict[str, dict] = {}

    async def fake_upsert(request: PublicListUpsertRequest, redis=None) -> dict:
        doc = {"list_id": request.list_id, "name": request.name}
        stored[request.list_id] = doc
        return doc

    async def fake_delete(list_id: str, redis=None) -> bool:
        return stored.pop(list_id, None) is not None

    async def fake_get(list_id: str, redis=None) -> dict | None:
        return stored.get(list_id)

    async def fake_search(**kwargs) -> dict:
        return {
            "results": list(stored.values()),
            "total": len(stored),
            "offset": kwargs.get("offset", 0),
            "limit": kwargs.get("limit", 20),
            "sort": kwargs.get("sort") or "relevance",
            "query": "*",
        }

    async def fake_ensure_index(redis=None) -> bool:
        return True

    # Patch both the service module and the route module's imported symbols
    for module in (service_module, routes_module):
        monkeypatch.setattr(module, "upsert_public_list", fake_upsert)
        monkeypatch.setattr(module, "delete_public_list", fake_delete)
        monkeypatch.setattr(module, "get_public_list", fake_get)
        monkeypatch.setattr(module, "ensure_public_list_index", fake_ensure_index)

    async def fake_search_wrapper(
        q=None,
        owner_username=None,
        owner_id=None,
        topic=None,
        item_mc_id=None,
        item_title=None,
        sort=None,
        limit=20,
        offset=0,
        include_system_owned_in_popularity=False,
        redis=None,
    ):
        # Preserve real sort validation so bad sorts still 400
        from core.public_list_queries import resolve_sort

        resolve_sort(sort)
        return await fake_search(sort=sort, limit=limit, offset=offset)

    for module in (service_module, routes_module):
        monkeypatch.setattr(module, "search_public_lists", fake_search_wrapper)

    app = FastAPI()
    app.include_router(router)
    stored["seeded_list"] = {"list_id": "seeded_list", "name": "Seeded"}
    return TestClient(app)


def _upsert_payload(list_id: str = "list_1") -> dict:
    return {
        "list_id": list_id,
        "name": "Test List",
        "owner_id": "user_1",
        "owner_username": "tester",
    }


class TestWriteAuth:
    def test_upsert_rejected_without_key(self, cloud_run_env, client: TestClient):
        response = client.put("/api/public-lists/list_1", json=_upsert_payload())
        assert response.status_code == 401

    def test_upsert_rejected_with_wrong_key(self, cloud_run_env, client: TestClient):
        response = client.put(
            "/api/public-lists/list_1",
            json=_upsert_payload(),
            headers={"X-API-Key": "wrong"},
        )
        assert response.status_code == 401

    def test_upsert_accepted_with_key(self, cloud_run_env, client: TestClient):
        response = client.put(
            "/api/public-lists/list_1",
            json=_upsert_payload(),
            headers={"X-API-Key": WRITE_KEY},
        )
        assert response.status_code == 200
        assert response.json()["success"] is True

    def test_delete_rejected_without_key(self, cloud_run_env, client: TestClient):
        response = client.delete("/api/public-lists/seeded_list")
        assert response.status_code == 401

    def test_delete_accepted_with_key(self, cloud_run_env, client: TestClient):
        response = client.delete(
            "/api/public-lists/seeded_list", headers={"X-API-Key": WRITE_KEY}
        )
        assert response.status_code == 200
        assert response.json()["deleted"] is True

    def test_index_create_rejected_without_key(self, cloud_run_env, client: TestClient):
        response = client.post("/api/public-lists/index")
        assert response.status_code == 401

    def test_etl_key_fallback_accepted(
        self, cloud_run_env, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.delenv("PUBLIC_LISTS_API_KEY", raising=False)
        monkeypatch.setenv("ETL_API_KEY", "etl-key")
        response = client.post(
            "/api/public-lists/index", headers={"X-API-Key": "etl-key"}
        )
        assert response.status_code == 200


class TestReadEndpoints:
    def test_search_is_public(self, cloud_run_env, client: TestClient):
        response = client.get("/api/public-lists/search")
        assert response.status_code == 200
        body = response.json()
        assert body["success"] is True
        assert body["total"] == 1

    def test_get_is_public(self, cloud_run_env, client: TestClient):
        response = client.get("/api/public-lists/seeded_list")
        assert response.status_code == 200
        assert response.json()["list"]["list_id"] == "seeded_list"

    def test_get_missing_returns_404(self, cloud_run_env, client: TestClient):
        response = client.get("/api/public-lists/does_not_exist")
        assert response.status_code == 404


class TestValidation:
    def test_upsert_list_id_mismatch(self, cloud_run_env, client: TestClient):
        response = client.put(
            "/api/public-lists/other_id",
            json=_upsert_payload("list_1"),
            headers={"X-API-Key": WRITE_KEY},
        )
        assert response.status_code == 400

    def test_invalid_sort_returns_400(self, cloud_run_env, client: TestClient):
        response = client.get("/api/public-lists/search", params={"sort": "bogus"})
        assert response.status_code == 400

    def test_limit_clamped_by_fastapi(self, cloud_run_env, client: TestClient):
        response = client.get("/api/public-lists/search", params={"limit": 500})
        assert response.status_code == 422


class TestVerifyWriteKey:
    def test_local_environment_bypass(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("K_SERVICE", raising=False)
        monkeypatch.delenv("PUBLIC_LISTS_API_KEY", raising=False)
        monkeypatch.delenv("ETL_API_KEY", raising=False)
        monkeypatch.setenv("ENVIRONMENT", "local")
        assert verify_public_list_write_key(None) is True

    def test_cloud_run_requires_key(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("K_SERVICE", "svc")
        monkeypatch.delenv("PUBLIC_LISTS_API_KEY", raising=False)
        monkeypatch.delenv("ETL_API_KEY", raising=False)
        monkeypatch.delenv("ENVIRONMENT", raising=False)
        assert verify_public_list_write_key(None) is False

    def test_dedicated_key_preferred_over_etl(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("K_SERVICE", "svc")
        monkeypatch.setenv("PUBLIC_LISTS_API_KEY", "dedicated")
        monkeypatch.setenv("ETL_API_KEY", "etl")
        assert verify_public_list_write_key("dedicated") is True
        # ETL key no longer valid once a dedicated key exists
        assert verify_public_list_write_key("etl") is False
