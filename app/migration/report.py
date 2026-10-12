"""Post-migration report (HTML + CSV). Spec §3.12."""

from __future__ import annotations

import csv
import html
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from app.conversion.common import ENGINE_VERSION, LABEL, MIG, NOT, PART, TEST, safe
from app.migration.ledger import load_ledger, summarize_ledger

__all__ = ["write_report"]


def write_report(out: str | Path) -> dict[str, Any]:
    out = Path(out)
    ledger = load_ledger(out)
    S = summarize_ledger(ledger)
    e = html.escape
    color = {MIG: "#1a7f37", TEST: "#b7791f", PART: "#d9730d", NOT: "#c0392b"}
    HDR = {
        MIG: ["Type", "Name", "Target", "Deployment", "What was done"],
        TEST: ["Type", "Name", "Target", "Deployment", "What was done", "Test before go-live"],
        PART: [
            "Type",
            "Name",
            "Target",
            "Deployment",
            "Still needs manual rework",
            "Test / next step",
        ],
        NOT: ["Type", "Name", "Target", "Deployment", "Why not migrated", "Next action"],
    }

    def table(outcome: str, cols: list[Callable[[dict[str, Any]], str]]) -> str:
        rows = [i for i in ledger["items"] if i["outcome"] == outcome]
        if not rows:
            return "<p><i>none</i></p>"
        body = "".join("<tr>" + "".join(f"<td>{c(i)}</td>" for c in cols) + "</tr>" for i in rows)
        return "<table><tr>" + "".join(f"<th>{h}" for h in HDR[outcome]) + f"</tr>{body}</table>"

    def ul(xs: list[str]) -> str:
        return "<ul>" + "".join(f"<li>{e(x)}</li>" for x in xs) + "</ul>" if xs else ""

    base: list[Callable[[dict[str, Any]], str]] = [
        lambda i: e(i["type"]),
        lambda i: e(i["name"]),
        lambda i: e(
            f"{i['target_type']} / {i['target_name']}" if i["target_name"] else i["target_type"]
        ),
        lambda i: e(i["deployment"]),
    ]
    sections = {
        MIG: table(MIG, base + [lambda i: ul(i["actions"])]),
        TEST: table(
            TEST, base + [lambda i: ul(i["actions"] + i["reasons"]), lambda i: ul(i["tests"])]
        ),
        PART: table(
            PART,
            base
            + [
                lambda i: ul(i["reasons"]),
                lambda i: ul(i["tests"] + ["AI work order: ai_tasks/" + safe(i["id"]) + ".md"]),
            ],
        ),
        NOT: table(
            NOT,
            base
            + [
                lambda i: ul(i["reasons"]),
                lambda i: ul(["AI work order: ai_tasks/" + safe(i["id"]) + ".md"]),
            ],
        ),
    }
    cards = "".join(
        f"<span class=c style='color:{color[k]}'><b>{S[k]}</b>{LABEL[k]}</span>"
        for k in (MIG, TEST, PART, NOT)
    )
    trows = "".join(
        f"<tr><td>{e(t)}<td>{sum(v.values())}"
        + "".join(f"<td>{v[k]}" for k in (MIG, TEST, PART, NOT))
        + "</tr>"
        for t, v in sorted(S["by_type"].items())
    )
    doc = f"""<!doctype html><meta charset=utf-8><title>Databricks to Fabric - Post-Migration Report</title>
<style>body{{font:14px system-ui;margin:32px;color:#222}}table{{border-collapse:collapse;margin:12px 0;width:100%}}td,th{{border:1px solid #ddd;padding:6px 9px;text-align:left;vertical-align:top}}
th{{background:#f3f4f6}}.c{{display:inline-block;margin:0 14px 14px 0;padding:14px 20px;border-radius:10px;background:#f3f4f6}}.c b{{font-size:28px;display:block}}ul{{margin:0;padding-left:18px}}</style>
<h1>Databricks &rarr; Fabric Post-Migration Report</h1><p>Generated {datetime.now().strftime("%Y-%m-%d %H:%M")} by dbx2fabric {ENGINE_VERSION}</p>
<div>{cards}<span class=c><b>{S["deployed"]}</b>Deployed to Fabric</span><span class=c><b>{S["generated_only"]}</b>Generated, not deployed</span><span class=c><b>{S["failed"]}</b>Deployment failed</span></div>
<p><b>Deployment column:</b> "deployed" means the generated artifact now exists in Fabric (a notebook, pipeline or shortcut). Data copies, view creation and Warehouse scripts still have to be run; see RUNBOOK.md. Nothing counts as verified until it is executed in Fabric. "Migrated" = no behaviour to test (infrastructure or code with nothing to convert). Run the tests listed for every other item.</p>
<h2>By asset type</h2><table><tr><th>Type<th>Total<th>{LABEL[MIG]}<th>{LABEL[TEST]}<th>{LABEL[PART]}<th>{LABEL[NOT]}</tr>{trows}</table>
<h2>Not migrated</h2>{sections[NOT]}<h2>Partially migrated - manual rework</h2>{sections[PART]}
<h2>Migrated - needs testing</h2>{sections[TEST]}<h2>Migrated</h2>{sections[MIG]}"""
    (out / "post_migration_report.html").write_text(doc, "utf-8")
    with open(out / "post_migration_report.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "type",
                "name",
                "outcome",
                "deployment",
                "target_type",
                "target_name",
                "target_id",
                "actions",
                "reasons",
                "tests",
            ]
        )
        for i in ledger["items"]:
            w.writerow(
                [
                    i["type"],
                    i["name"],
                    LABEL[i["outcome"]] if i["outcome"] in LABEL else i["outcome"],
                    i["deployment"],
                    i["target_type"],
                    i["target_name"],
                    i.get("target_id") or "",
                    " | ".join(i["actions"]),
                    " | ".join(i["reasons"]),
                    " | ".join(i["tests"]),
                ]
            )
    return S
