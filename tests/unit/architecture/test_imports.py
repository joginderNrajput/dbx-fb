"""Structure enforced by tests: import isolation and single-owner modules (blueprint §9.4)."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

APP = Path(__file__).resolve().parents[3] / "app"


def test_api_import_does_not_load_the_llm_sdk() -> None:
    code = "import sys, app.main; bad=[m for m in ('anthropic',) if m in sys.modules]; print(','.join(bad))"
    out = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=True,
        env={"AUTH_DISABLED": "true", "PATH": ""},
    )
    assert out.stdout.strip() == ""


def _importers(pattern: str) -> set[str]:
    rx = re.compile(pattern, re.M)
    return {str(p.relative_to(APP)) for p in APP.rglob("*.py") if rx.search(p.read_text())}


def test_only_llm_module_imports_anthropic() -> None:
    assert _importers(r"^\s*(import anthropic|from anthropic)") == {"agents/llm.py"}


def test_no_os_environ_outside_secrets() -> None:
    assert _importers(r"os\.environ") <= {"core/secrets.py"}


def test_storage_keys_built_in_one_place() -> None:
    assert _importers(r"f\"jobs/|'jobs/") <= {"storage/keys.py"}


def test_core_does_not_import_domain() -> None:
    for p in (APP / "core").glob("*.py"):
        for m in re.findall(r"^from app\.(\w+)", p.read_text(), re.M):
            assert m == "core", f"{p.name} imports app.{m}"
