"""AI completion of notebook cells that still carry TODO markers (options.ai_fix). Spec §6.3."""

from __future__ import annotations

import re
from typing import Any

from app.agents.llm import LLMClient
from app.agents.prompts import CELL_FIX_PROMPT
from app.conversion.code import Ctx
from app.core.exceptions import AIUnavailable
from app.core.logging import get_logger

__all__ = ["CellFixer"]

log = get_logger(__name__)


class CellFixer:
    """Callable ``(cells, ctx) -> fixed_count`` with a per-job budget of rewritten cells.

    Ported from the reference tool's ``ai_fix_cells``: only code cells containing a TODO marker
    are sent; a failed call leaves the cell unchanged and the item stays Partial.
    """

    def __init__(self, llm: LLMClient, max_cells: int) -> None:
        self.llm = llm
        self.remaining = max_cells

    def __call__(self, cells: list[dict[str, Any]], ctx: Ctx) -> int:
        fixed = 0
        for c in cells:
            if c["kind"] != "code" or "TODO[dbx2fabric:" not in c["src"]:
                continue
            if self.remaining <= 0:
                log.warning("ai_cell_budget_exhausted")
                break
            self.remaining -= 1
            try:
                txt = self.llm.complete(
                    CELL_FIX_PROMPT.format(
                        todos=", ".join(sorted({t["rule"] for t in ctx.todos})), cell=c["src"]
                    )
                )
            except AIUnavailable as e:
                log.warning("ai_cell_fix_failed", error=e.message)
                continue
            m = re.search(r"```(?:\w+)?\n(.*?)```", txt, re.S)
            if m:
                c["src"] = m.group(1).rstrip("\n")
                fixed += 1
        return fixed
