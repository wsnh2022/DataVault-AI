"""
core/nodes/fast_path.py - Pandas dispatch fast paths (zero LLM tokens).

Fast path 1: exact question match in DISPATCH table -> instant pandas result.
Fast path 2: fuzzy percentile pattern for all columns -> pandas quantile.

If either hits, is_pandas_dispatch=True and the graph routes directly to narrator,
skipping schema loading, SQL generation, and execution entirely.
"""

import re
import logging
import pandas as pd

from core.sql import pandas_dispatch
from core.sql.pandas_dispatch import _load_df
from core.sql.sql_executor import SqlExecutor
from core.graph_state import PipelineState

logger = logging.getLogger(__name__)

_PERCENTILE_PATTERN = re.compile(
    r'percentile|quartile|\bp25\b|\bp50\b|\bp75\b|25th|50th|75th|interquartile|iqr',
    re.IGNORECASE,
)
_ALL_COLUMNS_PATTERN = re.compile(
    r'all\s+(numeric\s+)?columns?|every\s+column|each\s+column',
    re.IGNORECASE,
)


def fast_path_node(state: PipelineState) -> dict:
    question = state["question"]

    # Fast path 1: exact dispatch match
    result_df = pandas_dispatch.try_dispatch(question, state["db_path"])
    if result_df is not None:
        return {
            "is_pandas_dispatch": True,
            "df": result_df,
            "sql": "-- Computed with pandas (no SQL needed)",
        }

    # Fast path 2: fuzzy percentile pattern for all columns
    if _PERCENTILE_PATTERN.search(question) and _ALL_COLUMNS_PATTERN.search(question):
        pct_fn = pandas_dispatch.DISPATCH[
            "show the 25th, 50th, and 75th percentile for all numeric columns"
        ]
        try:
            all_tbls = SqlExecutor(state["db_path"]).get_table_names()
            parts = []
            for tbl in all_tbls[:4]:
                tdf = _load_df(state["db_path"], tbl)
                if tdf.empty:
                    continue
                tbl_pct = pct_fn(tdf)
                if len(all_tbls) > 1:
                    tbl_pct.insert(0, "table", tbl)
                parts.append(tbl_pct)
            if parts:
                pandas_df = pd.concat(parts, ignore_index=True)
                return {
                    "is_pandas_dispatch": True,
                    "df": pandas_df,
                    "sql": "-- Computed with pandas (no SQL needed)",
                }
        except Exception as exc:
            logger.warning("Percentile fast path failed: %s", exc)

    return {"is_pandas_dispatch": False}


def route_fast_path(state: PipelineState) -> str:
    return "pandas_hit" if state.get("is_pandas_dispatch") else "miss"
