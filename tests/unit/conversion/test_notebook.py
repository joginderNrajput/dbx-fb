from __future__ import annotations

import json

from app.conversion.code import Ctx
from app.conversion.common import MIG, PART, TEST, nb_name, norm_path, safe
from app.conversion.notebook import build_ipynb, convert_cells, outcome_from_ctx, parse_source
from app.migration.mapping import load_cfg


def test_source_format_split_into_cells() -> None:
    text = "# Databricks notebook source\nx = 1\n\n# COMMAND ----------\n\n# MAGIC %md\n# MAGIC # Title\n\n# COMMAND ----------\n\n# MAGIC %sql\n# MAGIC SELECT 1"
    cells = parse_source(text, "PYTHON")
    assert [(c["kind"], c["lang"]) for c in cells] == [
        ("code", "python"),
        ("md", "md"),
        ("code", "sql"),
    ]
    assert cells[1]["src"] == "# Title"


def test_ipynb_parsed() -> None:
    nb = {
        "cells": [
            {"cell_type": "markdown", "source": ["# a"]},
            {"cell_type": "code", "source": ["%run ./x"]},
        ]
    }
    cells = parse_source(json.dumps(nb), "PYTHON")
    assert cells[0]["kind"] == "md" and cells[1]["lang"] == "run"


def test_run_and_shell_cells() -> None:
    ctx = Ctx(load_cfg(None), [])
    out = convert_cells(
        [
            dict(kind="code", lang="run", src="%run ../lib/helpers"),
            dict(kind="code", lang="sh", src="ls"),
        ],
        ctx,
        "/Repos/a/etl/nb",
    )
    assert out[0]["src"] == "%run helpers"
    assert out[1]["src"].startswith("# TODO[dbx2fabric:MAGIC-SH-FS]")
    assert outcome_from_ctx(ctx) == PART


def test_parameter_cell_tagged_and_kernel_metadata() -> None:
    ctx = Ctx(load_cfg(None), [])
    cells = convert_cells(parse_source('dbutils.widgets.text("a", "1")', "PYTHON"), ctx)
    nb = build_ipynb(cells, "header")
    assert nb["cells"][1]["metadata"] == {"tags": ["parameters"]}
    assert nb["metadata"]["kernelspec"]["name"] == "synapse_pyspark"


def test_outcomes() -> None:
    ctx = Ctx(load_cfg(None), [])
    assert outcome_from_ctx(ctx) == MIG
    ctx.change("x")
    assert outcome_from_ctx(ctx) == TEST


def test_name_helpers() -> None:
    assert safe("a b/c") == "a_b_c" and safe("") == "item"
    assert norm_path("/Workspace/Repos/x") == "/Repos/x"
    assert norm_path("./lib", "/Repos/a/nb") == "/Repos/a/lib"
    assert nb_name("/Repos/team/etl/load sales") == "load_sales"
