"""Notebook parsing (Databricks source format and .ipynb), cell conversion and .ipynb build. Spec §3.1."""

from __future__ import annotations

import json
import re
from typing import Any

from app.conversion.code import Ctx, convert_code
from app.conversion.common import MIG, PART, TEST, nb_name, norm_path
from app.conversion.rules import COMMENT

__all__ = ["build_ipynb", "convert_cells", "notes_from_ctx", "outcome_from_ctx", "parse_source"]

Cell = dict[str, Any]

HDR_RE = re.compile(r"^(#|--|//)\s*Databricks notebook source\s*$")
MAGIC_RE = re.compile(r"^(#|--|//)\s?MAGIC\b")


def parse_source(text: str, lang: str | None) -> list[Cell]:
    """Databricks source format (.py/.sql/.scala/.r) or .ipynb -> [{kind, lang, src}]"""
    lang = (lang or "PYTHON").lower()
    text = text.lstrip("﻿").replace("\r\n", "\n")
    cells: list[Cell] = []
    if text.lstrip().startswith("{") and '"cells"' in text[:2000]:
        for c in json.loads(text).get("cells", []):
            src = c.get("source", [])
            cells.append(
                _classify(
                    "".join(src) if isinstance(src, list) else src,
                    lang,
                    md=c.get("cell_type") == "markdown",
                )
            )
        return cells
    text = re.sub(
        r"^# /// script\n((?:#.*\n)*?)# ///\n?",
        lambda m: "" if "tool.databricks" in m.group(1) else m.group(0),
        text,
        flags=re.M,
    )
    lines = [ln for ln in text.split("\n") if not HDR_RE.match(ln.strip())]
    chunks: list[list[str]] = []
    chunk: list[str] = []
    for ln in lines:
        if re.match(r"^(#|--|//)\s*COMMAND\s*-+\s*$", ln):
            chunks.append(chunk)
            chunk = []
        else:
            chunk.append(ln)
    chunks.append(chunk)
    for ch in chunks:
        segs: list[
            tuple[bool, list[str]]
        ] = []  # consecutive MAGIC lines vs plain lines become separate cells
        for ln in ch:
            is_m = bool(MAGIC_RE.match(ln))
            if not segs or segs[-1][0] != is_m:
                segs.append((is_m, []))
            segs[-1][1].append(re.sub(r"^(#|--|//)\s?MAGIC ?", "", ln) if is_m else ln)
        for _is_m, ls in segs:
            while ls and not ls[0].strip():
                ls = ls[1:]
            while ls and not ls[-1].strip():
                ls = ls[:-1]
            if ls:
                cells.append(_classify("\n".join(ls), lang))
    return cells


def _classify(src: str, default: str, md: bool = False) -> Cell:
    if md:
        return dict(kind="md", lang="md", src=src)
    m = re.match(r"\s*%(\w+)\b[ \t]*\n?", src)
    if m and m.group(1) in ("sql", "md", "python", "scala", "r", "sh", "run", "pip", "fs"):
        k = m.group(1)
        if k == "md":
            return dict(kind="md", lang="md", src=src[m.end() :])
        if k in ("run", "pip"):
            return dict(kind="code", lang=k, src=src.strip())
        return dict(kind="code", lang=k, src=src[m.end() :])
    return dict(
        kind="code",
        lang=default if default in ("python", "sql", "scala", "r") else "python",
        src=src,
    )


def convert_cells(cells: list[Cell], ctx: Ctx, base_path: str = "") -> list[Cell]:
    out: list[Cell] = []
    for c in cells:
        if c["kind"] == "md":
            out.append(dict(kind="md", src=c["src"]))
            continue
        lang, src = c["lang"], c["src"]
        if lang == "run":
            m = re.match(r"%run\s+(\S+)", src)
            name = nb_name(norm_path(m.group(1), base_path)) if m else ""
            ctx.deps.add(name)
            ctx.change("%run path -> notebook name")
            out.append(dict(kind="code", src=f"%run {name}"))
        elif lang in ("sh", "fs"):
            ctx.todos.append(
                dict(
                    rule="MAGIC-SH-FS",
                    advice="Rewrite shell/%fs with notebookutils or Python",
                    line=src[:80],
                )
            )
            out.append(
                dict(
                    kind="code",
                    src="# TODO[dbx2fabric:MAGIC-SH-FS] Rewrite shell/%fs with notebookutils or Python\n"
                    + "\n".join("# " + ln for ln in src.split("\n")),
                )
            )
        elif lang == "pip":
            ctx.warns["PIP-INSTALL"] = (
                ctx.warns.get("PIP-INSTALL", (0, ""))[0] + 1,
                "Move libraries to a Fabric Environment for repeatability",
            )
            out.append(dict(kind="code", src=src))
        else:
            code = convert_code(src, lang, ctx)
            if re.search(r"^(#|--|//)\s?MAGIC\b", code, re.M):
                cm = COMMENT.get(lang, "#")
                ctx.todos.append(
                    dict(rule="MAGIC-LEFTOVER", advice="Unconverted MAGIC cell", line=code[:80])
                )
                code = (
                    f"{cm} TODO[dbx2fabric:MAGIC-LEFTOVER] Unconverted MAGIC line(s): convert manually\n"
                    + code
                )
            prefix = {"sql": "%%sql\n", "scala": "%%spark\n", "r": "%%sparkr\n"}.get(lang, "")
            out.append(dict(kind="code", src=prefix + code))
    if ctx.params:
        out.insert(
            0,
            dict(
                kind="code",
                tags=["parameters"],
                src="\n".join(f"{k} = {json.dumps(v)}" for k, v in ctx.params.items()),
            ),
        )
    return out


def build_ipynb(cells: list[Cell], header: str = "") -> dict[str, Any]:
    nbc: list[dict[str, Any]] = []
    if header:
        nbc.append({"cell_type": "markdown", "metadata": {}, "source": header.splitlines(True)})
    for c in cells:
        if c["kind"] == "md":
            nbc.append(
                {"cell_type": "markdown", "metadata": {}, "source": c["src"].splitlines(True)}
            )
        else:
            meta = {"tags": c["tags"]} if c.get("tags") else {}
            nbc.append(
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": meta,
                    "outputs": [],
                    "source": c["src"].splitlines(True),
                }
            )
    return {
        "nbformat": 4,
        "nbformat_minor": 5,
        "cells": nbc,
        "metadata": {
            "kernel_info": {"name": "synapse_pyspark"},
            "kernelspec": {"name": "synapse_pyspark", "display_name": "Synapse PySpark"},
            "language_info": {"name": "python"},
            "microsoft": {"language": "python", "language_group": "synapse_pyspark"},
        },
    }


def outcome_from_ctx(ctx: Ctx) -> str:
    if ctx.todos:
        return PART
    return TEST if (ctx.changes or ctx.warns) else MIG


def notes_from_ctx(item: dict[str, Any], ctx: Ctx) -> None:
    for k, n in ctx.changes.items():
        item["actions"].append(f"{k} ({n}x)")
    for t in ctx.todos:
        item["reasons"].append(f"TODO {t['rule']}: {t['advice']}  [{t['line']}]")
    for k, (n, adv) in ctx.warns.items():
        item["reasons"].append(f"Test {k} ({n}x): {adv}")
