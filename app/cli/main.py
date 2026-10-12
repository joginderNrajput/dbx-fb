"""Operator CLI: serve the API, run an offline migration, convert one notebook, hash API keys. Spec §5.8."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from app.core.config import get_settings
from app.core.security import generate_api_key, hash_api_key

app = typer.Typer(help="Databricks -> Microsoft Fabric migration service", no_args_is_help=True)


@app.command()
def serve(host: str = "0.0.0.0", port: int = 9093, reload: bool = False) -> None:  # nosec B104
    """Run the HTTP API (same as `uvicorn app.main:app`)."""
    import uvicorn

    uvicorn.run("app.main:app", host=host, port=port, reload=reload)


@app.command("hash-key")
def hash_key(
    key: Annotated[str | None, typer.Argument(help="existing key; omit to generate one")] = None,
) -> None:
    """Print an API key and its Argon2id hash for API_KEY_HASHES."""
    key = key or generate_api_key()
    typer.echo(f"API key (give to the client):  {key}")
    typer.echo(f"API_KEY_HASHES entry (.env):   '{hash_api_key(key)}'")


@app.command("convert-notebook")
def convert_notebook(
    path: Path, out: Annotated[Path | None, typer.Option(help="output .ipynb")] = None
) -> None:
    """Convert one notebook file offline and print the TODO blockers."""
    from app.conversion.code import Ctx
    from app.conversion.notebook import build_ipynb, convert_cells, outcome_from_ctx, parse_source
    from app.migration.mapping import load_cfg
    from app.sources.offline import detect_language

    text = path.read_text("utf-8", "ignore")
    cfg = load_cfg(None)
    ctx = Ctx(cfg, [])
    nb = build_ipynb(convert_cells(parse_source(text, detect_language(path, text)), ctx))
    target = out or path.with_name(path.stem + ".fabric.ipynb")
    target.write_text(json.dumps(nb, indent=2), "utf-8")
    typer.echo(f"{outcome_from_ctx(ctx)}: wrote {target}")
    for t in ctx.todos:
        typer.echo(f"  TODO {t['rule']}: {t['advice']}  [{t['line']}]")


@app.command()
def run(
    source_dir: Annotated[
        Path,
        typer.Option(
            help="folder of exported notebooks (and optional assessment.json / mapping.json)"
        ),
    ],
    out: Annotated[Path, typer.Option(help="output folder")] = Path("migration_out"),
    mapping: Annotated[Path | None, typer.Option(help="mapping.json overrides")] = None,
    databricks_host: Annotated[str | None, typer.Option(envvar="DATABRICKS_HOST")] = None,
    databricks_token: Annotated[str | None, typer.Option(envvar="DATABRICKS_TOKEN")] = None,
) -> None:
    """Offline migration without the API: inventory -> plan -> convert -> preflight -> report."""
    from app.conversion.common import LABEL
    from app.databricks.auth import DatabricksCredentials
    from app.migration.session import MigrationSession, SessionSpec
    from app.sources.offline import EXT_LANG

    files = [p for p in source_dir.rglob("*") if p.is_file()]
    notebooks = [
        str(p.relative_to(source_dir).as_posix()) for p in files if p.suffix.lower() in EXT_LANG
    ]
    assess_p = source_dir / "assessment.json"
    map_p = mapping or source_dir / "mapping.json"
    spec = SessionSpec(
        source_kind="upload",
        sources_dir=source_dir,
        out_dir=out,
        uploaded_notebooks=notebooks,
        assessment=json.loads(assess_p.read_text("utf-8")) if assess_p.exists() else None,
        uploaded_mapping=json.loads(map_p.read_text("utf-8")) if map_p.exists() else {},
        databricks=DatabricksCredentials(databricks_host.rstrip("/"), databricks_token)
        if databricks_host and databricks_token
        else None,
    )
    session = MigrationSession(spec, get_settings())
    try:
        session.inventory()
        session.plan()
        session.convert()
        issues = session.run_preflight()
        summary = session.report()
    finally:
        session.close()
    for i in issues:
        typer.echo(f"{i['level'].upper():5} {i['where']}: {i['message']}")
    typer.echo(
        " | ".join(f"{LABEL[k]} {summary[k]}" for k in LABEL)
        + f"\nReport: {out / 'post_migration_report.html'}"
    )


if __name__ == "__main__":
    app()
