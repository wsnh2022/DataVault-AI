"""
core/nodes/schema_loader.py - Extracts DDL from DuckDB.

Populates ddl and table_names in state.
Sets error if the database has no tables (e.g. nothing uploaded yet).
No sample rows are fetched or sent to the LLM - schema only.
"""

import logging
from core.sql.sql_executor import SqlExecutor
from core.graph_state import PipelineState

logger = logging.getLogger(__name__)


def schema_loader_node(state: PipelineState) -> dict:
    executor = SqlExecutor(state["db_path"])

    table_ddl = executor.get_table_ddl_map()
    if not table_ddl:
        return {"error": "Database has no tables. Upload a CSV first."}

    # Enforce single-table mode: filter to the active file chosen in the UI
    active = state.get("active_table", "")
    if active and active in table_ddl:
        table_ddl = {active: table_ddl[active]}

    tables = list(table_ddl.keys())
    ddl = "\n\n".join(table_ddl.values())

    return {
        "ddl": ddl,
        "sample_csv": "",
        "table_names": tables,
        "table_ddl": table_ddl,
        "table_samples": {},
    }


def route_schema_loader(state: PipelineState) -> str:
    return "error" if state.get("error") else "ok"
