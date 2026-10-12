from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import SecretStr

from app.core.exceptions import ValidationFailed
from app.core.logging import redact
from app.core.secrets import resolve_secret
from app.core.security import ensure_safe_url, generate_api_key, hash_api_key, verify_api_key
from tests.conftest import make_settings


def test_api_key_hashing() -> None:
    key = generate_api_key()
    h = hash_api_key(key)
    assert verify_api_key(key, ["garbage", h]) and not verify_api_key("nope", [h])


@pytest.mark.parametrize(
    "url", ["http://adb.example.com", "https://127.0.0.1", "https://10.1.2.3:8443", "ftp://x", ""]
)
def test_unsafe_urls_refused(url: str) -> None:
    with pytest.raises(ValidationFailed):
        ensure_safe_url(url, field="host", allow_private=False)


def test_safe_url_normalised() -> None:
    assert ensure_safe_url("8.8.8.8/", field="h", allow_private=False) == "https://8.8.8.8"
    assert (
        ensure_safe_url("http://localhost:9000", field="h", allow_private=True)
        == "http://localhost:9000"
    )


def test_env_references_gated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MY_PAT", "dapi-1")
    with pytest.raises(ValidationFailed):
        resolve_secret(SecretStr("env://MY_PAT"), make_settings(tmp_path), field="token")
    assert (
        resolve_secret(
            SecretStr("env://MY_PAT"),
            make_settings(tmp_path, ALLOW_ENV_CREDENTIALS=True),
            field="token",
        )
        == "dapi-1"
    )
    assert resolve_secret(SecretStr(""), make_settings(tmp_path), field="token") is None


def test_redaction_by_key_name() -> None:
    ev = redact(
        None,
        "",
        {
            "event": "x",
            "token": "t",
            "nested": {"client_secret": "s", "ok": 1},
            "items": [{"authorization": "b"}],
        },
    )
    assert (
        ev["token"] == "***"
        and ev["nested"] == {"client_secret": "***", "ok": 1}
        and ev["items"] == [{"authorization": "***"}]
    )
