"""
core/nodes/executor.py - Runs the generated SQL against DuckDB.

On SQL execution error: retries once with error context injected into the prompt.
On success: populates df in state and routes to empty_check.
On both attempts failing: sets error and routes to END.
"""

import logging
from core.sql.sql_executor import SqlExecutor
from core.sql.sql_generator import SqlGenerator as SqlGeneratorEngine
from core.sql.sql_validator import SqlValidator
from core.graph_state import PipelineState

logger = logging.getLogger(__name__)


def executor_node(state: PipelineState) -> dict:
    executor = SqlExecutor(state["db_path"])
    sql = state["sql"]

    # First attempt
    try:
        df = executor.run(sql)
        return {"df": df, "exec_error": ""}
    except Exception as first_err:
        exec_error = str(first_err)
        logger.warning("SQL execution error: %s - retrying with error context", exec_error)

    # Retry: regenerate SQL with the error message as context
    retry_question = (
        f"{state['question']}\n\n"
        f"The previous SQL failed with this error: {exec_error}\n"
        f"Previous SQL was:\n{sql}\n"
        f"Rewrite the query to fix the error."
    )
    gen = SqlGeneratorEngine()
    sql_retry = gen.generate(
        retry_question,
        state["ddl"],
        state.get("sample_csv", ""),
        state.get("table_names", []),
    )

    v = SqlValidator()
    is_valid, _ = v.validate_sql(sql_retry)
    if not is_valid:
        return {"error": f"SQL execution failed: {exec_error}", "exec_error": exec_error}

    # Second attempt
    try:
        df = executor.run(sql_retry)
        return {"df": df, "sql": sql_retry, "retried": True, "exec_error": ""}
    except Exception as retry_err:
        return {
            "error": f"SQL failed after retry: {retry_err}\nOriginal error: {exec_error}",
            "exec_error": exec_error,
        }


def route_executor(state: PipelineState) -> str:
    return "error" if state.get("error") else "success"
