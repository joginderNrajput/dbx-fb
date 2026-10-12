"""mapping.json: the editable "Map" step that steers routing and rewrites. Spec §3.4."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.conversion.common import load_json, safe, save_json
from app.core.exceptions import ValidationFailed

__all__ = ["DEFAULT_MAPPING", "init_mapping", "load_cfg", "merge_mapping", "validate_overrides"]

DEFAULT_MAPPING: dict[str, Any] = {
    "workspace_id": "",
    "lakehouse_default": "lh_main",
    "catalog_to_lakehouse": {},
    "warehouse_name": "wh_main",
    "warehouse_schemas": ["gold", "dw", "mart", "reporting"],  # OLAP schemas -> Fabric Warehouse
    "sqldb_name": "sqldb_main",
    "sqldb_connection_types": [
        "POSTGRESQL",
        "MYSQL",
        "SQLSERVER",
        "ORACLE",
        "SQLDW",
    ],  # OLTP sources -> SQL database
    "keyvault_url_default": "https://<your-vault>.vault.azure.net/",
    "keyvault_urls": {},  # secret scope -> vault url
    "mount_map": {},  # "/mnt/raw" -> "Files/raw"
    "adls_connection_id": "",  # Fabric connection id used by OneLake shortcuts
    "timezone": "UTC",
    "dbt_target": "warehouse",  # warehouse | lakehouse
    "dbt_projects": {},  # job name -> dbt project folder (relative to the uploaded sources)
    "staging_storage": "",  # abfss://container@account.dfs.core.windows.net/dbx_export (ADLS Gen2 you own)
    "allow_direct_managed_copy": False,  # read UC managed tables straight from their storage (only if you own it and Fabric can reach it)
}


def load_cfg(path: str | Path | None = None) -> dict[str, Any]:
    cfg: dict[str, Any] = json.loads(json.dumps(DEFAULT_MAPPING))
    if path and Path(path).exists():
        cfg.update(load_json(path))
    return cfg


def init_mapping(assess: dict[str, Any], path: str | Path | None = None) -> dict[str, Any]:
    """Derive a starting mapping from the inventory (lakehouse per catalog, mounts, scopes, dbt jobs)."""
    cfg = load_cfg(None)
    assets = assess["assets"]
    cats = sorted(
        {
            a["name"].split(".")[0]
            for a in assets
            if a["type"] in ("Table/View", "Volume") and "." in a["name"]
        }
    )
    cfg["catalog_to_lakehouse"] = {c: f"lh_{safe(c)}" for c in cats}
    if cats:
        cfg["lakehouse_default"] = f"lh_{safe(cats[0])}"
    cfg["mount_map"] = {
        a["name"]: "Files/" + a["name"].rstrip("/").split("/")[-1]
        for a in assets
        if a["type"] == "DBFS Mount"
    }
    cfg["keyvault_urls"] = {
        a["name"]: cfg["keyvault_url_default"] for a in assets if a["type"] == "Secret Scope"
    }
    cfg["dbt_projects"] = {
        a["name"]: ""
        for a in assets
        if a["type"] == "Job"
        and any("dbt_task" in t for t in a["meta"].get("settings", {}).get("tasks", []))
    }
    if path:
        save_json(path, cfg)
    return cfg


def validate_overrides(overrides: dict[str, Any], *, field: str = "mapping") -> dict[str, Any]:
    """Reject unknown keys and wrong value types so a typo is a 422 naming the key."""
    for k, v in overrides.items():
        if k not in DEFAULT_MAPPING:
            raise ValidationFailed(f"unknown mapping key {k!r}", field=f"{field}.{k}")
        expected = type(DEFAULT_MAPPING[k])
        if not isinstance(v, expected) or (expected is not bool and isinstance(v, bool)):
            raise ValidationFailed(
                f"mapping key {k!r} must be of type {expected.__name__}", field=f"{field}.{k}"
            )
    return overrides


def merge_mapping(base: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    """Overlay user overrides; dict-valued keys merge (so one catalog can be renamed alone)."""
    cfg = json.loads(json.dumps(base))
    for k, v in overrides.items():
        if isinstance(v, dict) and isinstance(cfg.get(k), dict):
            cfg[k] = {**cfg[k], **v}
        else:
            cfg[k] = v
    return cfg
