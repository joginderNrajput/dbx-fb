from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

import pytest

from app.core.exceptions import PayloadTooLarge, ValidationFailed
from app.sources.lookup import SourceResolver
from app.sources.offline import (
    IngestLimits,
    build_offline_assessment,
    detect_language,
    ingest_uploads,
)

LIMITS = IngestLimits(max_files=100, max_total_bytes=1_000_000)


def zip_bytes(members: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for n, d in members.items():
            zf.writestr(n, d)
    return buf.getvalue()


def test_zip_slip_and_unsupported_members_skipped(tmp_path: Path) -> None:
    data = zip_bytes(
        {
            "etl/a.py": b"x=1",
            "../../evil.py": b"x",
            "/abs.py": b"x",
            "readme.bin": b"?",
            "__MACOSX/a.py": b"x",
        }
    )
    res = ingest_uploads([("bundle.zip", data)], tmp_path / "src", LIMITS)
    assert res.notebooks == ["etl/a.py"]
    assert not (tmp_path / "evil.py").exists()
    assert "readme.bin" in res.skipped


def test_control_files(tmp_path: Path) -> None:
    res = ingest_uploads(
        [
            ("assessment.json", json.dumps({"assets": []}).encode()),
            ("mapping.json", b'{"warehouse_name": "w"}'),
            ("nb.ipynb", b"{}"),
        ],
        tmp_path / "src",
        LIMITS,
    )
    assert (
        res.assessment == {"assets": []}
        and res.mapping == {"warehouse_name": "w"}
        and res.notebooks == ["nb.ipynb"]
    )
    with pytest.raises(ValidationFailed):
        ingest_uploads([("assessment.json", b"{bad")], tmp_path / "src2", LIMITS)


def test_limits(tmp_path: Path) -> None:
    with pytest.raises(PayloadTooLarge):
        ingest_uploads(
            [("a.py", b"x" * 2000)],
            tmp_path / "s",
            IngestLimits(max_files=10, max_total_bytes=1000),
        )
    with pytest.raises(ValidationFailed):
        ingest_uploads([("bad.zip", b"not a zip")], tmp_path / "s2", LIMITS)


def test_detect_language(tmp_path: Path) -> None:
    p = tmp_path / "n.ipynb"
    p.write_text(
        json.dumps({"metadata": {"application/vnd.databricks.v1+notebook": {"language": "sql"}}})
    )
    assert detect_language(p) == "SQL"
    assert detect_language(tmp_path / "x.scala") == "SCALA"


def test_assessment_and_lookup(notebooks_dir: Path) -> None:
    rels = sorted(
        str(p.relative_to(notebooks_dir)) for p in notebooks_dir.rglob("*") if p.is_file()
    )
    assess = build_offline_assessment(notebooks_dir, rels)
    names = {a["name"]: a for a in assess["assets"]}
    assert set(names) == {"load_sales", "publish", "explore", "report"}
    assert (
        names["load_sales"]["location"] == "/etl/load_sales"
        and names["report"]["meta"]["language"] == "SQL"
    )
    resolver = SourceResolver(notebooks_dir, None)
    text, lang = resolver(names["report"]) or ("", "")
    assert "Revenue report" in text and lang == "SQL"
    assert resolver({"location": "/../../etc/passwd", "meta": {}}) is None
