"""
core/nodes/grounding.py - Node wrapper around GroundingVerifier.

Non-blocking: never sets error. Only logs a warning and flags grounding dict
when narration numbers don't match the result data.
"""

import logging
import pandas as pd
from core.sql.grounding_verifier import GroundingVerifier
from core.graph_state import PipelineState

logger = logging.getLogger(__name__)


def grounding_node(state: PipelineState) -> dict:
    gv = GroundingVerifier()
    df = state.get("df", pd.DataFrame())
    grounding = gv.verify(state.get("narration", ""), df, state["question"])
    if not grounding.get("is_grounded"):
        logger.warning("Grounding mismatch - flagged: %s", grounding.get("flagged"))
    return {"grounding": grounding}
