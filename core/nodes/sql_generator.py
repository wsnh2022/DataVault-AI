"""
core/nodes/sql_generator.py - Node wrapper around SqlGenerator engine.

Three-layer defence against wrong table usage:
  Layer 1 (table_selector): LLM picks the right tables upfront.
  Layer 2 (this file):       Post-generation check - if the SQL references a table
                             that table_selector excluded, retry with the full schema.
  Layer 3 (sql_generator):   System prompt explicitly instructs single-table preference.

Also handles CTE follow-up mode and narration context injection.
"""

import re
import logging
from core.sql.sql_generator import SqlGenerator as SqlGeneratorEngine
from core.graph_state import PipelineState

logger = logging.getLogger(__name__)


def sql_generator_node(state: PipelineState) -> dict:
    gen = SqlGeneratorEngine()

    follow_up_type = state.get("follow_up_type", "fresh")
    cte_prefix = state.get("cte_prefix", "")
    prev_columns = state.get("prev_result_columns", [])

    # Narration mode: pass previous answer as context (still generates SQL)
    prev_narration = ""
    if follow_up_type == "narration_follow_up":
        raw_narration = state.get("prev_narration", "")
        if len(raw_narration) >= 80:
            prev_narration = raw_narration

    # CTE mode: suppress table hint so LLM uses prev_result, not original tables
    effective_tables = [] if cte_prefix else state.get("table_names", [])

    sql = gen.generate(
        state["question"],
        state["ddl"],
        state.get("sample_csv", ""),
        effective_tables,
        cte_prefix=cte_prefix,
        prev_narration=prev_narration,
        prev_columns=prev_columns if cte_prefix else [],
    )

    # ------------------------------------------------------------------
    # Layer 2: table reference validation (non-CTE mode only)
    # If the LLM referenced a table that table_selector excluded, the
    # selector made the wrong call. Retry with the full schema so the
    # SQL generator can see all tables and write a correct query.
    # table_ddl and table_samples are preserved in state even after
    # table_selector ran, so full context is always available for retry.
    # ------------------------------------------------------------------
    if not cte_prefix:
        all_known = set(state.get("table_ddl", {}).keys())
        selected = set(state.get("table_names", []))
        excluded = all_known - selected

        if excluded:
            hallucinated = _find_referenced_excluded_table(sql, excluded)
            if hallucinated:
                logger.warning(
                    "SQL references excluded table '%s' - table_selector chose wrong "
                    "tables, retrying with full schema", hallucinated
                )
                full_ddl = "\n\n".join(state["table_ddl"].values())
                full_samples = "\n".join(state.get("table_samples", {}).values())
                sql = gen.generate(
                    state["question"],
                    full_ddl,
                    full_samples,
                    list(all_known),
                )
                logger.info("Full-schema retry SQL: %s", sql)
                return {"sql": sql, "retried": True}

    # ------------------------------------------------------------------
    # CTE post-generation validation
    # ------------------------------------------------------------------
    if cte_prefix:
        if "prev_result" not in sql.lower():
            logger.warning(
                "LLM ignored CTE prefix (no 'prev_result' in output) - "
                "retrying as fresh query"
            )
            sql = gen.generate(
                state["question"],
                state["ddl"],
                state.get("sample_csv", ""),
                state.get("table_names", []),
            )
            logger.info("Fresh retry SQL: %s", sql)
            return {
                "sql": sql,
                "follow_up_type": "fresh",
                "is_follow_up": False,
                "retried": True,
            }
        else:
            sql = cte_prefix + "\n" + sql

    logger.info("Generated SQL: %s", sql)
    return {"sql": sql}


def _find_referenced_excluded_table(sql: str, excluded: set[str]) -> str | None:
    """
    Returns the first excluded table name found in a FROM or JOIN clause, or None.
    Uses word-boundary matching to avoid false positives on partial name matches.
    """
    sql_lower = sql.lower()
    for tbl in excluded:
        pattern = rf'(?:from|join)\s+["\']?{re.escape(tbl.lower())}["\']?(?:\s|;|$|\))'
        if re.search(pattern, sql_lower):
            return tbl
    return None
