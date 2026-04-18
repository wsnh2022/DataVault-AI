"""
core/nodes/follow_up_detector.py - Graph node wrapping follow-up classification.

Runs before schema_loader. Reads force_follow_up from state (set by the
UI toggle in app.py) to decide whether to scope this query to the previous
result or treat it as a fresh query against the full dataset.

When force_follow_up=True and a valid previous SQL exists, always returns
cte_follow_up - no regex or keyword matching needed.
When force_follow_up=False, always returns fresh.
CTE builder fallback cases (empty prev, pandas prev, single-row, etc.)
still apply downstream regardless of this classification.
"""

import logging
from core.graph_state import PipelineState

logger = logging.getLogger(__name__)


def follow_up_detector_node(state: PipelineState) -> dict:
    force = state.get("force_follow_up", False)
    prev_sql = state.get("prev_sql", "")

    if force and prev_sql and not prev_sql.startswith("-- Computed with pandas"):
        logger.info(
            "Follow-up detector: forced cte_follow_up (depth=%d) question=%r",
            state.get("follow_up_depth", 0), state["question"],
        )
        return {"is_follow_up": True, "follow_up_type": "cte_follow_up"}

    logger.info(
        "Follow-up detector: fresh (force=%s prev_sql=%s) question=%r",
        force, bool(prev_sql), state["question"],
    )
    return {"is_follow_up": False, "follow_up_type": "fresh"}
