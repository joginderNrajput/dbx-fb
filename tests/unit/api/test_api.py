from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from app.core.security import hash_api_key
from app.main import create_app
from tests.conftest import FIXTURES, make_settings


def test_health(client: TestClient) -> None:
    assert client.get("/healthz").json() == {"status": "ok"}
    r = client.get("/readyz").json()
    assert r["status"] == "ok" and r["checks"]["storage"] == "ok"
    assert "dbx2fabric_jobs_started_total" in client.get("/metrics").text
    assert client.get("/healthz").headers["x-request-id"]


def test_api_key_required(tmp_path: Path) -> None:
    s = make_settings(tmp_path, AUTH_DISABLED=False, API_KEY_HASHES=hash_api_key("k1"))
    with TestClient(create_app(s)) as c:
        r = c.get("/api/v1/migrations")
        assert r.status_code == 401 and r.json()["error"]["code"] == "unauthorized"
        assert c.get("/api/v1/migrations", headers={"X-API-Key": "k1"}).status_code == 200
        assert c.get("/healthz").status_code == 200  # probes stay open


def test_convert_notebook_endpoint(client: TestClient) -> None:
    path = FIXTURES / "notebooks/etl/load_sales.py"
    r = client.post(
        "/api/v1/convert/notebook",
        files={"file": (path.name, path.read_bytes())},
        data={"mapping": json.dumps({"catalog_to_lakehouse": {"main": "lh_sales"}})},
    )
    body = r.json()
    assert r.status_code == 200 and body["outcome"] == "partial"
    assert {t["rule"] for t in body["todos"]} >= {
        "DBU-MOUNT",
        "MAGIC-SH-FS",
    }  # dbfs: path hits DBFS-PATH first without a mount_map
    assert "lh_sales.bronze.sales_raw" in json.dumps(body["notebook"])
    r = client.post(
        "/api/v1/convert/notebook",
        files={"file": (path.name, path.read_bytes())},
        data={"download": "true"},
    )
    assert (
        r.headers["content-disposition"].endswith('load_sales.ipynb"') and r.json()["nbformat"] == 4
    )


def test_convert_job_endpoint_and_errors(client: TestClient) -> None:
    settings = json.loads((FIXTURES / "job_settings.json").read_text())
    r = client.post("/api/v1/convert/job", json={"name": "nightly", "settings": settings})
    assert r.status_code == 200 and len(r.json()["pipeline"]["properties"]["activities"]) == 3
    r = client.post("/api/v1/convert/job", json={"name": "x", "settings": {}, "typo": 1})
    assert r.status_code == 400 and r.json()["error"]["field"] == "typo"
    r = client.post("/api/v1/convert/job", json={"settings": {}, "mapping": {"nope": 1}})
    assert r.status_code == 422 and r.json()["error"]["field"] == "mapping.nope"
    r = client.post("/api/v1/convert/notebook", files={"file": ("a.exe", b"x")})
    assert r.status_code == 422


def test_unknown_job_and_bad_ids(client: TestClient) -> None:
    assert client.get("/api/v1/migrations/mig_00000000000000000000").status_code == 404
    assert client.get("/api/v1/migrations/..%2F..%2Fetc").status_code == 404


def test_body_size_limit(tmp_path: Path) -> None:
    with TestClient(create_app(make_settings(tmp_path, MAX_UPLOAD_MB=1))) as c:
        r = c.post("/api/v1/migrations/upload", files={"files": ("a.py", b"x" * (2 * 1024 * 1024))})
        assert r.status_code == 413 and r.json()["error"]["code"] == "payload_too_large"


def test_llm_connection_check(tmp_path: Path) -> None:
    import httpx
    import respx

    s = make_settings(
        tmp_path,
        LLM_PROVIDER="azure_openai",
        LLM_BASE_URL="https://r.openai.azure.com",
        LLM_API_KEY="k",
        LLM_MODEL="dep",
    )
    with TestClient(create_app(s)) as c, respx.mock() as router:
        router.post(
            url__startswith="https://r.openai.azure.com/openai/deployments/dep/chat/completions"
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "choices": [
                        {
                            "finish_reason": "stop",
                            "message": {"role": "assistant", "content": "ready"},
                        }
                    ]
                },
            )
        )
        assert c.post("/api/v1/connections/llm/test").json() == {
            "ok": True,
            "provider": "azure_openai",
            "model": "dep",
            "reply": "ready",
        }
    with TestClient(create_app(make_settings(tmp_path))) as c:
        assert c.post("/api/v1/connections/llm/test").status_code == 503


def test_migration_form_is_served_without_auth(tmp_path: Path) -> None:
    """The static form holds no data; every API call it makes carries the user's key."""
    app = create_app(make_settings(tmp_path, AUTH_DISABLED=False, API_KEY_HASHES=hash_api_key("k")))
    with TestClient(app) as c:
        r = c.get("/ui")
        assert r.status_code == 200 and r.headers["content-type"].startswith("text/html")
        assert r.headers["cache-control"] == "no-store"
        page = r.text
        assert 'id="connectForm"' in page and "/connections/databricks/test" in page
        assert "localStorage.setItem" in page and "client_secret" in page
        assert "async function mockApi" in page and 'id="modeSwitch"' in page  # demo mode + switch
        root = c.get("/", follow_redirects=False)
        assert root.status_code == 307 and root.headers["location"] == "/ui"
        assert c.get("/api/v1/migrations").status_code == 401  # the API itself stays protected
