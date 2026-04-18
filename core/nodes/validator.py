"""
core/nodes/validator.py - Node wrapper around SqlValidator.

Blocks destructive SQL before it reaches the executor.
Sets error in state if invalid - graph routes to END.
"""

from core.sql.sql_validator import SqlValidator
from core.graph_state import PipelineState


def validator_node(state: PipelineState) -> dict:
    v = SqlValidator()
    is_valid, reason = v.validate_sql(state["sql"])
    if not is_valid:
        return {"error": f"SQL validation failed: {reason}\nSQL: {state['sql']}"}
    return {}


def route_validator(state: PipelineState) -> str:
    return "invalid" if state.get("error") else "valid"
