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


def test_run_job_and_for_each_tasks_with_job_refs() -> None:
    """Service extension (ADR 0003): only active when job_refs is passed."""
    a = {
        "name": "orchestrator",
        "meta": {
            "settings": {
                "tasks": [
                    {
                        "task_key": "child",
                        "run_job_task": {"job_id": 7, "job_parameters": {"d": "1"}},
                    },
                    {
                        "task_key": "fan",
                        "depends_on": [{"task_key": "child"}],
                        "for_each_task": {
                            "inputs": '["us", "eu"]',
                            "concurrency": 2,
                            "task": {
                                "task_key": "fan_iter",
                                "notebook_task": {
                                    "notebook_path": "/x/load_region",
                                    "base_parameters": {"region": "{{input}}", "fixed": "y"},
                                },
                            },
                        },
                    },
                    {"task_key": "other", "run_job_task": {"job_id": 99}},
                    {
                        "task_key": "dyn",
                        "for_each_task": {"inputs": "{{job.parameters.list}}", "task": {}},
                    },
                ]
            }
        },
    }
    assert convert_job(a, load_cfg(None))[0] is None  # reference behaviour: all omitted
    pipe, _, notes, omitted = convert_job(a, load_cfg(None), {"7": "pl_child"})
    assert pipe is not None
    acts = {x["name"]: x for x in pipe["properties"]["activities"]}
    assert set(acts) == {"child", "fan"}
    ex = acts["child"]
    assert ex["type"] == "ExecutePipeline"
    assert ex["typeProperties"]["pipeline"] == {
        "referenceName": "@@PL:pl_child@@",
        "type": "PipelineReference",
    }
    assert ex["typeProperties"]["waitOnCompletion"] is True
    assert ex["typeProperties"]["parameters"] == {"d": "1"}
    fe = acts["fan"]
    assert fe["type"] == "ForEach" and "policy" not in fe
    assert fe["dependsOn"] == [{"activity": "child", "dependencyConditions": ["Succeeded"]}]
    tp = fe["typeProperties"]
    assert tp["items"] == {"value": '@json(\'["us", "eu"]\')', "type": "Expression"}
    assert tp["isSequential"] is False and tp["batchCount"] == 2
    inner = tp["activities"][0]
    assert inner["typeProperties"]["notebookId"] == "@@NB:load_region@@"
    assert inner["typeProperties"]["parameters"]["region"] == {
        "value": {"value": "@{item()}", "type": "Expression"},
        "type": "string",
    }
    assert inner["typeProperties"]["parameters"]["fixed"] == {"value": "y", "type": "string"}
    text = " | ".join(omitted)
    assert "other: Run Job task: job 99 is not part of this migration" in text
    assert "dyn: For-each task: inputs are not a literal JSON array" in text
