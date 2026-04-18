"""
core/nodes/cte_builder.py - Builds the CTE prefix from prev_sql when in CTE follow-up mode.

Only active when follow_up_type == "cte_follow_up". Returns {} for all other modes.

Handles all 8 CTE error cases:
  1. prev_sql already has CTEs          -> merge_cte() wraps as subquery (DuckDB supports this)
  2. trailing semicolon in prev_sql     -> stripped by merge_cte()
  3. prev was pandas dispatch           -> reset to fresh (no SQL to wrap)
  4. prev result was empty              -> reset to fresh (nothing to scope to)
  5. LLM ignores prev_result in output  -> detected in sql_generator_node post-generation
  6. prev SQL was "Cannot answer" stub  -> reset to fresh
  7. single-row aggregation result      -> reset to fresh (scoping a scalar is not useful)
  8. ORDER BY inside CTE                -> warn only; DuckDB ignores ORDER BY inside CTEs;
                                           outer SELECT must specify its own ORDER BY
"""

import logging
from core.sql.cte_utils import merge_cte
from core.graph_state import PipelineState

logger = logging.getLogger(__name__)


def cte_builder_node(state: PipelineState) -> dict:
    if state.get("follow_up_type") != "cte_follow_up":
        return {}  # No CTE needed for fresh or narration mode

    prev_sql = state.get("prev_sql", "")
    prev_shape = state.get("prev_result_shape", (0, 0))
    prev_was_pandas = state.get("prev_was_pandas", False)

    # Case 3: previous result was pandas dispatch - no SQL to wrap
    if prev_was_pandas:
        logger.info("CTE builder: prev was pandas dispatch - falling back to fresh")
        return {"follow_up_type": "fresh", "is_follow_up": False, "cte_prefix": ""}

    # Case 4: previous result was empty - nothing to scope to
    if not prev_shape or prev_shape[0] == 0:
        logger.info("CTE builder: prev result was empty - falling back to fresh")
        return {"follow_up_type": "fresh", "is_follow_up": False, "cte_prefix": ""}

    # Case 6: previous SQL was a "Cannot answer" stub
    if "Cannot answer" in prev_sql:
        logger.info("CTE builder: prev SQL was 'Cannot answer' stub - falling back to fresh")
        return {"follow_up_type": "fresh", "is_follow_up": False, "cte_prefix": ""}

    # Case 7: single-row aggregation (e.g. SELECT COUNT(*) returns 1 row, 1 col)
    if prev_shape == (1, 1):
        logger.info("CTE builder: prev was single-row aggregate - falling back to fresh")
        return {"follow_up_type": "fresh", "is_follow_up": False, "cte_prefix": ""}

    # Cases 1 & 2: handled by merge_cte (nested CTE support + semicolon stripping)
    cte_prefix = merge_cte(prev_sql)

    # Case 8: warn if ORDER BY in prev_sql - order is not guaranteed inside a CTE
    if "ORDER BY" in prev_sql.upper():
        logger.warning(
            "CTE builder: prev_sql contains ORDER BY - order inside a CTE is not "
            "guaranteed. The outer SELECT should specify its own ORDER BY."
        )

    logger.info("CTE builder: built prefix for follow-up (%d chars)", len(cte_prefix))
    return {"cte_prefix": cte_prefix}
