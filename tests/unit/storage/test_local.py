from __future__ import annotations

from pathlib import Path

import pytest

from app.core.exceptions import NotFound, ValidationFailed
from app.storage import keys
from app.storage.local import LocalStore


def test_roundtrip_and_traversal_guard(tmp_path: Path) -> None:
    s = LocalStore(tmp_path)
    s.write_json("jobs/a/job.json", {"x": 1})
    assert s.read_json("jobs/a/job.json") == {"x": 1} and s.list_prefixes("jobs") == ["a"]
    with pytest.raises(ValidationFailed):
        s.path("../outside")
    s.delete_prefix("jobs/a")
    assert not s.exists("jobs/a/job.json")


def test_keys_validate_job_ids() -> None:
    assert keys.job_out("mig_0123456789abcdef0123") == "jobs/mig_0123456789abcdef0123/out"
    with pytest.raises(NotFound):
        keys.job_record("../../etc")
