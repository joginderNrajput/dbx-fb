from __future__ import annotations

import json
from typing import Any

from app.conversion.jobs import convert_job, fab_timeout, quartz_to_fabric
from app.migration.mapping import load_cfg
from tests.conftest import FIXTURES


def job() -> dict[str, Any]:
    return {
        "name": "nightly_sales",
        "meta": {"settings": json.loads((FIXTURES / "job_settings.json").read_text())},
    }


def test_notebook_tasks_become_activities_and_others_are_reported() -> None:
    pipe, sched, notes, omitted = convert_job(job(), load_cfg(None))
    assert pipe is not None
    acts = {a["name"]: a for a in pipe["properties"]["activities"]}
    assert set(acts) == {"load", "publish", "notify"}
    assert acts["load"]["typeProperties"]["notebookId"] == "@@NB:load_sales@@"
    assert (
        acts["load"]["policy"]["retry"] == 2
        and acts["load"]["policy"]["retryIntervalInSeconds"] == 60
    )
    assert acts["notify"]["dependsOn"] == [
        {"activity": "publish", "dependencyConditions": ["Succeeded"]}
    ]
    assert omitted == ["refresh_dash: SQL task: recreate as Script/Warehouse activity"]
    assert any("dependency on omitted task(s) ['refresh_dash']" in n for n in notes)
    assert (
        sched
        and sched["configuration"]["type"] == "Daily"
        and sched["configuration"]["localTimeZoneId"] == "India Standard Time"
    )


def test_quartz_conversion() -> None:
    assert quartz_to_fabric("0 0/15 * * * ?")[0] == {"type": "Cron", "interval": 15}
    assert quartz_to_fabric("0 0 7 ? * MON-FRI")[0] == {
        "type": "Weekly",
        "weekdays": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"],
        "times": ["07:00"],
    }
    assert quartz_to_fabric("0 0 1 1 * ?")[0] is None


def test_timeout_format() -> None:
    assert fab_timeout(3600) == "0.01:00:00"
    assert fab_timeout(None) == "0.12:00:00"
