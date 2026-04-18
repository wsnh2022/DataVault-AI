"""
core/nodes/empty_check.py - Retries once if the SQL result is empty.

Only triggers when the previous execution succeeded (exec_error is empty) but
returned zero rows. Rebuilds the SQL with a broader prompt suffix and re-runs.
If the retry also returns empty, accepts the empty result (retried=True).
"""

import logging
from core.sql.sql_executor import SqlExecutor
from core.sql.sql_generator import SqlGenerator as SqlGeneratorEngine
from core.sql.sql_validator import SqlValidator
from core.graph_state import PipelineState

logger = logging.getLogger(__name__)


def empty_check_node(state: PipelineState) -> dict:
    v = SqlValidator()
    df = state.get("df")

    # Only retry if the execution itself succeeded but the result is empty
    if state.get("exec_error") or not v.is_empty_result(df):
        return {}

    logger.info("Empty result - retrying with broader prompt")
    retry_suffix = v.build_retry_prompt_suffix(state["question"], state["sql"])
    gen = SqlGeneratorEngine()
    sql_retry = gen.generate(
        state["question"] + retry_suffix,
        state["ddl"],
        state.get("sample_csv", ""),
        state.get("table_names", []),
    )

    is_valid, reason = v.validate_sql(sql_retry)
    if not is_valid:
        logger.warning("Retry SQL invalid (%s) - keeping empty result", reason)
        return {"retried": True}

    executor = SqlExecutor(state["db_path"])
    try:
        df_retry = executor.run(sql_retry)
        if not v.is_empty_result(df_retry):
            return {"df": df_retry, "sql": sql_retry, "retried": True}
    except Exception as exc:
        logger.warning("Empty-result retry execution failed: %s", exc)

    return {"retried": True}
