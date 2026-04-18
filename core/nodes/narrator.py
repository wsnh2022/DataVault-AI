"""
core/nodes/narrator.py - Node wrapper around Narrator engine.

Converts the result DataFrame to a plain-English narration.
Handles both SQL path (df from executor) and pandas path (df from fast_path).
"""

import pandas as pd
from core.sql.narrator import Narrator as NarratorEngine
from core.graph_state import PipelineState


def narrator_node(state: PipelineState) -> dict:
    n = NarratorEngine()
    df = state.get("df", pd.DataFrame())
    narration = n.narrate(state["question"], state["sql"], df)
    return {"narration": narration}
