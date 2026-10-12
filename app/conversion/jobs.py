"""Databricks Jobs -> Fabric Data Pipeline definitions and schedules. Spec §3.3."""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from typing import Any

from app.conversion.common import nb_name

__all__ = ["SKIP_TASK", "convert_job", "fab_timeout", "pipeline_refs", "quartz_to_fabric"]

WIN_TZ = {
    "Asia/Kolkata": "India Standard Time",
    "UTC": "UTC",
    "America/New_York": "Eastern Standard Time",
    "America/Chicago": "Central Standard Time",
    "America/Los_Angeles": "Pacific Standard Time",
    "Europe/London": "GMT Standard Time",
    "Europe/Berlin": "W. Europe Standard Time",
}
DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
DOW3 = {d[:3].upper(): d for d in DAYS}
Q_NUM = {
    "1": "Sunday",
    "2": "Monday",
    "3": "Tuesday",
    "4": "Wednesday",
    "5": "Thursday",
    "6": "Friday",
    "7": "Saturday",
}


def _dow(expr: str) -> list[str] | None:
    out: list[str] = []
    for part in expr.upper().split(","):
        if "-" in part:
            a, b = part.split("-")
            names = [DOW3.get(a[:3]) or Q_NUM.get(a), DOW3.get(b[:3]) or Q_NUM.get(b)]
            if names[0] is None or names[1] is None:
                return None
            i, j = DAYS.index(names[0]), DAYS.index(names[1])
            out += DAYS[i : j + 1] if i <= j else DAYS[i:] + DAYS[: j + 1]
        else:
            d = DOW3.get(part[:3]) or Q_NUM.get(part)
            if not d:
                return None
            out.append(d)
    return sorted(set(out), key=DAYS.index)


def quartz_to_fabric(expr: str | None) -> tuple[dict[str, Any] | None, str]:
    p = (expr or "").split()
    if len(p) < 6:
        return None, "cron not parseable"
    _, mi, hr, dom, mon, dow = p[:6]

    def anyd(x: str) -> bool:
        return x in ("*", "?")

    if mon != "*" or not anyd(dom) and not anyd(dow):
        return None, "monthly/complex cron needs a manual schedule"
    m = re.fullmatch(r"(?:0/|\*/)(\d+)", mi)
    if m and hr == "*" and anyd(dom) and anyd(dow):
        return {"type": "Cron", "interval": int(m.group(1))}, ""
    m = re.fullmatch(r"(?:0/|\*/)(\d+)", hr)
    if mi.isdigit() and m and anyd(dom) and anyd(dow):
        return {
            "type": "Cron",
            "interval": int(m.group(1)) * 60,
        }, "hourly interval approximated (start offset not kept)"
    if mi.isdigit() and re.fullmatch(r"\d+(,\d+)*", hr):
        times = [f"{int(h):02d}:{int(mi):02d}" for h in hr.split(",")]
        if anyd(dom) and anyd(dow):
            return {"type": "Daily", "times": times}, ""
        if anyd(dom):
            days = _dow(dow)
            if days:
                return {"type": "Weekly", "weekdays": days, "times": times}, ""
    return None, "cron pattern not supported by automatic conversion"


def fab_timeout(sec: Any) -> str:
    sec = int(sec or 0) or 12 * 3600
    d, r = divmod(sec, 86400)
    return f"{d}.{r // 3600:02d}:{r % 3600 // 60:02d}:{r % 60:02d}"


SKIP_TASK = {
    "pipeline_task": "DLT pipeline: no Fabric equivalent (redesign)",
    "dbt_task": "dbt task: handled as separate dbt Project item",
    "sql_task": "SQL task: recreate as Script/Warehouse activity",
    "spark_python_task": "Spark Python task: create a Spark Job Definition",
    "python_wheel_task": "Wheel task: Spark Job Definition + Environment",
    "spark_jar_task": "JAR task: Spark Job Definition",
    "spark_submit_task": "spark-submit task: rewrite as notebook / Spark Job Definition",
    "run_job_task": "Run Job task: wire an Invoke/Execute Pipeline activity manually",
    "condition_task": "Condition task: add an If Condition activity manually",
    "for_each_task": "For-each task: add a ForEach activity manually",
}


# Placeholder for another pipeline's item id, resolved at deploy (pipelines are deployed in
# dependency order so the referenced one exists first).
PL_REF = re.compile(r"@@PL:([^@]+)@@")
_INPUT = re.compile(r"\{\{\s*input((?:\.\w+)*)\s*\}\}")


def pipeline_refs(text: str) -> set[str]:
    """Names of the pipelines a pipeline definition invokes (``@@PL:name@@``)."""
    return set(PL_REF.findall(text))


def _policy(t: dict[str, Any]) -> dict[str, Any]:
    return {
        "timeout": fab_timeout(t.get("timeout_seconds")),
        "retry": int(t.get("max_retries") or 0),
        "retryIntervalInSeconds": max(
            30, int((t.get("min_retry_interval_millis") or 30000) / 1000)
        ),
        "secureInput": False,
        "secureOutput": False,
    }


def _activity(
    name: str, dep: list[dict[str, Any]], t: dict[str, Any], body: dict[str, Any]
) -> dict[str, Any]:
    """Container/invoke activities take no timeout/retry policy (ForEach none at all)."""
    act: dict[str, Any] = {"name": name, "type": body["type"], "dependsOn": dep}
    if body["type"] == "TridentNotebook":
        act["policy"] = _policy(t)
    elif body["type"] == "ExecutePipeline":
        act["policy"] = {"secureInput": False}
    act["typeProperties"] = body["typeProperties"]
    return act


def _param(v: Any, loop: bool) -> dict[str, Any]:
    """A notebook parameter; inside a ForEach ``{{input}}`` / ``{{input.key}}`` -> ``@{item()...}``."""
    text = str(v)
    if loop and _INPUT.search(text):
        return {
            "value": {
                "value": _INPUT.sub(lambda m: "@{item()" + m.group(1) + "}", text),
                "type": "Expression",
            },
            "type": "string",
        }
    return {"value": text, "type": "string"}


def _notebook_props(nt: dict[str, Any], loop: bool = False) -> dict[str, Any]:
    tp: dict[str, Any] = {
        "notebookId": f"@@NB:{nb_name(nt.get('notebook_path', ''))}@@",
        "workspaceId": "@@WS@@",
    }
    if nt.get("base_parameters"):
        tp["parameters"] = {pk: _param(pv, loop) for pk, pv in nt["base_parameters"].items()}
    return tp


def _run_job_activity(
    t: dict[str, Any], job_refs: dict[str, str], loop: bool = False
) -> dict[str, Any] | None:
    """run_job_task -> ExecutePipeline on the migrated pipeline of that job (if it is migrated)."""
    rj = t["run_job_task"]
    target = job_refs.get(str(rj.get("job_id")))
    if not target:
        return None
    params = {**(rj.get("notebook_params") or {}), **(rj.get("job_parameters") or {})}
    tp: dict[str, Any] = {
        "pipeline": {"referenceName": f"@@PL:{target}@@", "type": "PipelineReference"},
        "waitOnCompletion": True,
    }
    if params:
        tp["parameters"] = {k: (_param(v, loop)["value"]) for k, v in params.items()}
    return {"type": "ExecutePipeline", "typeProperties": tp}


def _json_items(inputs: Any) -> str | None:
    """for_each_task inputs as a pipeline expression; only literal JSON arrays are convertible."""
    if isinstance(inputs, list):
        arr = inputs
    else:
        try:
            arr = json.loads(str(inputs))
        except ValueError:
            return None
    if not isinstance(arr, list):
        return None
    return "@json('" + json.dumps(arr).replace("'", "''") + "')"


def _extended_activity(
    t: dict[str, Any], job_refs: dict[str, str]
) -> tuple[dict[str, Any] | None, str]:
    """run_job_task / for_each_task -> activity body (no name/dependsOn), or the reason it is omitted."""
    if "run_job_task" in t:
        act = _run_job_activity(t, job_refs)
        if act:
            return act, ""
        return None, (
            f"Run Job task: job {t['run_job_task'].get('job_id')} is not part of this migration; "
            "migrate it too (it becomes an Execute Pipeline activity) or wire the activity manually"
        )
    fe = t["for_each_task"]
    items = _json_items(fe.get("inputs"))
    if items is None:
        return (
            None,
            "For-each task: inputs are not a literal JSON array; add a ForEach activity manually",
        )
    inner = fe.get("task") or {}
    if "notebook_task" in inner:
        body: dict[str, Any] | None = {
            "type": "TridentNotebook",
            "typeProperties": _notebook_props(inner["notebook_task"], loop=True),
        }
    elif "run_job_task" in inner:
        body = _run_job_activity(inner, job_refs, loop=True)
    else:
        body = None
    if body is None:
        return (
            None,
            "For-each task: the nested task is not a notebook or migrated job; add a ForEach activity manually",
        )
    conc = int(fe.get("concurrency") or 1)
    inner_act = _activity(inner.get("task_key") or f"{t['task_key']}_iteration", [], inner, body)
    return {
        "type": "ForEach",
        "typeProperties": {
            "items": {"value": items, "type": "Expression"},
            "isSequential": conc <= 1,
            **({"batchCount": min(conc, 50)} if conc > 1 else {}),
            "activities": [inner_act],
        },
    }, ""


def convert_job(
    a: dict[str, Any], cfg: dict[str, Any], job_refs: dict[str, str] | None = None
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, list[str], list[str]]:
    """Notebook tasks become TridentNotebook activities; other task types are reported as omitted.

    ``job_refs`` (job id -> migrated pipeline name) turns on the service's extensions (ADR 0003):
    run_job_task -> ExecutePipeline and for_each_task over a literal list -> ForEach. Without it
    the result is exactly the reference tool's.
    """
    s = a["meta"].get("settings", {})
    acts: list[dict[str, Any]] = []
    kept: set[str] = set()
    notes: list[str] = []
    omitted: list[str] = []
    tasks = s.get("tasks", [])
    extended: dict[str, dict[str, Any]] = {}
    skipped: dict[str, str] = {}
    for t in tasks:
        if "notebook_task" in t:
            kept.add(t["task_key"])
        elif job_refs is not None and ("run_job_task" in t or "for_each_task" in t):
            act, why = _extended_activity(t, job_refs)
            if act:
                kept.add(t["task_key"])
                extended[t["task_key"]] = act
            else:
                skipped[t["task_key"]] = why
    for t in tasks:
        k = t["task_key"]
        if k not in kept:
            kind = next((x for x in SKIP_TASK if x in t), "unknown")
            omitted.append(f"{k}: {skipped.get(k) or SKIP_TASK.get(kind, 'unsupported task type')}")
            continue
        dep: list[str] = []
        lost: list[str] = []
        for d in t.get("depends_on", []):
            (dep if d["task_key"] in kept else lost).append(d["task_key"])
        if lost:
            notes.append(f"{k}: dependency on omitted task(s) {lost} was dropped")
        body = extended.get(k) or {
            "type": "TridentNotebook",
            "typeProperties": _notebook_props(t["notebook_task"]),
        }
        acts.append(
            _activity(
                k,
                [{"activity": d, "dependencyConditions": ["Succeeded"]} for d in dep],
                t,
                body,
            )
        )
    pipe = (
        {
            "properties": {
                "description": f"Migrated from Databricks job '{a['name']}'",
                "activities": acts,
            }
        }
        if acts
        else None
    )
    if s.get("continuous"):
        notes.append(
            "continuous job: Fabric pipelines are not continuous; use a streaming notebook or frequent schedule"
        )
    if s.get("email_notifications") or s.get("webhook_notifications"):
        notes.append("notifications: add Teams/Outlook activities on the failure path")
    if s.get("job_clusters") or any(t.get("libraries") for t in tasks):
        notes.append("cluster/library settings: attach a Fabric Environment to the notebooks")
    sched = None
    if s.get("schedule") and pipe:
        conf, note = quartz_to_fabric(s["schedule"].get("quartz_cron_expression"))
        if note:
            notes.append("schedule: " + note)
        if conf:
            tz = WIN_TZ.get(s["schedule"].get("timezone_id"), cfg["timezone"])
            start = datetime.now().replace(microsecond=0, second=0)
            conf.update(
                {
                    "startDateTime": start.strftime("%Y-%m-%dT%H:%M:%S"),
                    "endDateTime": (start + timedelta(days=3650)).strftime("%Y-%m-%dT%H:%M:%S"),
                    "localTimeZoneId": tz,
                }
            )
            sched = {
                "enabled": s["schedule"].get("pause_status") != "PAUSED",
                "configuration": conf,
            }
    return pipe, sched, notes, omitted
