"""Source-level rewrite of one notebook cell from Databricks to Fabric idioms. Spec §3.2."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

from app.conversion.common import ident, nb_name
from app.conversion.rules import (
    COMMENT,
    NB_RUN_RE,
    SECRET_RE,
    TEST_RULES,
    TODO_RULES,
    WIDGET_DEF_RE,
    WIDGET_GET_RE,
    WIDGET_RM_RE,
)

__all__ = ["Ctx", "convert_code"]


class Ctx:
    """Accumulates what conversion did to one asset: changes, TODO blockers, warnings, params."""

    def __init__(self, cfg: dict[str, Any], catalogs: list[str]) -> None:
        self.cfg, self.catalogs = cfg, catalogs
        self.changes: dict[str, int] = {}
        self.todos: list[dict[str, str]] = []
        self.warns: dict[str, tuple[int, str]] = {}
        self.params: dict[str, str] = {}
        self.deps: set[str] = set()
        self.lakehouses: list[str] = []
        self.secrets: set[tuple[str, str]] = set()
        self.wh_refs: set[str] = set()

    def change(self, label: str, n: int = 1) -> None:
        self.changes[label] = self.changes.get(label, 0) + n

    def use_lh(self, lh: str) -> None:
        if lh not in self.lakehouses:
            self.lakehouses.append(lh)


def convert_code(src: str, lang: str, ctx: Ctx) -> str:
    cfg, s, cm = ctx.cfg, src, COMMENT.get(lang, "#")

    def sub(
        label: str, rx: str, repl: str | Callable[[re.Match[str]], str], flags: int = 0
    ) -> None:
        nonlocal s
        s, n = re.subn(rx, repl, s, flags=flags)
        if n:
            ctx.change(label, n)

    if lang in ("python", "scala"):
        if lang == "python":

            def wdef(m: re.Match[str]) -> str:
                ctx.params[ident(m.group(3))] = m.group(5)
                return f"{m.group(1)}# [dbx2fabric] widget '{m.group(3)}' -> parameter cell"

            def wget(m: re.Match[str]) -> str:
                n = ident(m.group(2))
                ctx.params.setdefault(n, "")
                return n

            sub("dbutils.widgets -> parameter cell", WIDGET_DEF_RE, wdef, re.M)
            sub("dbutils.widgets.get -> parameter variable", WIDGET_GET_RE, wget)
            sub(
                "dbutils.widgets.remove removed",
                WIDGET_RM_RE,
                lambda m: f"{m.group(1)}# [dbx2fabric] widgets.remove not needed",
                re.M,
            )

        sub(
            "self-assignment removed (value comes from the parameter cell)",
            r"^([ \t]*)(\w+)[ \t]*=[ \t]*\2[ \t]*$",
            lambda m: f"{m.group(1)}# [dbx2fabric] {m.group(2)} comes from the parameter cell",
            re.M,
        )
        sub(
            "mlflow.set_registry_uri(databricks) removed (Fabric uses the workspace registry)",
            r"^[ \t]*mlflow\.set_registry_uri\([\"']databricks[^\n]*$",
            lambda m: "# [dbx2fabric] removed: " + m.group(0).strip(),
            re.M,
        )

        def sec(m: re.Match[str]) -> str:
            scope, key = m.group(2), m.group(4)
            ctx.secrets.add((scope, key))
            vault = cfg["keyvault_urls"].get(scope, cfg["keyvault_url_default"])
            return f'notebookutils.credentials.getSecret("{vault}", "{key}")'

        sub("dbutils.secrets.get -> notebookutils.credentials.getSecret", SECRET_RE, sec)

        def nrun(m: re.Match[str]) -> str:
            ctx.deps.add(nb_name(m.group(2)))
            return f'notebookutils.notebook.run("{nb_name(m.group(2))}"'

        sub("dbutils.notebook.run -> notebookutils.notebook.run", NB_RUN_RE, nrun)
        sub(
            "dbutils.notebook.exit -> notebookutils.notebook.exit",
            r"dbutils\.notebook\.exit\(",
            "notebookutils.notebook.exit(",
        )
        sub(
            "dbutils.fs -> notebookutils.fs",
            r"dbutils\.fs\.(ls|cp|mv|rm|mkdirs|put|head|append)\b",
            r"notebookutils.fs.\1",
        )
        sub(
            "dbutils.library.restartPython -> notebookutils.session.restartPython",
            r"dbutils\.library\.restartPython\(\)",
            "notebookutils.session.restartPython()",
        )

    for mnt in sorted(cfg["mount_map"], key=len, reverse=True):
        dest, m_ = cfg["mount_map"][mnt].rstrip("/"), mnt.rstrip("/")

        def to_dest(_m: re.Match[str], d: str = dest) -> str:
            return d

        sub(f"mount {m_} -> {dest}", re.escape("dbfs:" + m_) + r"(?![\w])", to_dest)
        sub(f"mount {m_} -> {dest}", r"(?<![\w/.:\-])" + re.escape(m_) + r"(?![\w])", to_dest)

    for cat in ctx.catalogs:
        lh = cfg["catalog_to_lakehouse"].get(cat, cfg["lakehouse_default"])
        rx = r"(?<![\w.`])`?" + re.escape(cat) + r"`?\.`?(\w+)`?\.`?(\w+)`?"
        whs = [x.lower() for x in cfg["warehouse_schemas"]]

        def cat_repl(m: re.Match[str], lh: str = lh, whs: list[str] = whs) -> str:
            if m.group(1).lower() in whs:
                ctx.wh_refs.add(f"{m.group(1)}.{m.group(2)}")
            return f"{lh}.{m.group(1)}.{m.group(2)}"

        new, n = re.subn(rx, cat_repl, s)
        if n:
            s = new
            ctx.change(f"catalog '{cat}' -> lakehouse '{lh}' (3-part names)", n)
            ctx.use_lh(lh)

    sub(
        "Databricks-only Spark config removed",
        r"^[ \t]*spark\.conf\.set\(\s*[\"']spark\.databricks\.[^\n]*$",
        lambda m: cm + " [dbx2fabric] removed: " + m.group(0).strip(),
        re.M,
    )
    sub(
        "Databricks-only SET removed",
        r"^[ \t]*SET\s+spark\.databricks\.[^\n]*$",
        lambda m: cm + " [dbx2fabric] removed: " + m.group(0).strip(),
        re.M,
    )

    out = []
    for line in s.split("\n"):
        stripped = line.strip()
        if stripped and not stripped.startswith((cm, "[dbx2fabric]")):
            for rid, rx_, advice in TODO_RULES:
                if rx_.search(line):
                    indent = line[: len(line) - len(line.lstrip())]
                    out.append(f"{indent}{cm} TODO[dbx2fabric:{rid}] {advice}")
                    ctx.todos.append(dict(rule=rid, advice=advice, line=stripped[:120]))
                    break
        out.append(line)
    s = "\n".join(out)
    if ctx.wh_refs:
        ctx.warns["WH-LANDING"] = (
            len(ctx.wh_refs),
            "Uses Warehouse-mapped tables ("
            + ", ".join(sorted(ctx.wh_refs)[:3])
            + "): Spark reads/writes them in the Lakehouse; publish to the Warehouse with load_tables.sql after this notebook runs",
        )
    for rid, rx_, advice in TEST_RULES:
        cnt = len(rx_.findall(src))
        if cnt:
            ctx.warns[rid] = (ctx.warns.get(rid, (0, ""))[0] + cnt, advice)
    return s
