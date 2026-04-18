"""
core/graph_state.py - Shared state TypedDict for the LangGraph pipeline.

PipelineState flows through every node. Each node reads what it needs and
returns a dict with only the fields it modifies - LangGraph merges partial
updates into the state automatically.

QueryResult (in query_pipeline.py) is the external interface that app.py sees.
PipelineState is the internal representation inside the graph.
"""

from typing import Any, TypedDict


class PipelineState(TypedDict):
    # --- Inputs ---
    question: str
    db_path: str

    # --- Schema (populated by schema_loader) ---
    ddl: str
    sample_csv: str
    table_names: list[str]
    table_ddl: dict      # {table_name: ddl_string} - per-table, for table_selector filtering
    table_samples: dict  # {table_name: sample_csv_string} - per-table, for table_selector filtering

    # --- Table selection (populated by table_selector) ---
    selected_tables: list[str]  # subset of table_names chosen by LLM (or all if single/fallback)

    # --- SQL generation ---
    sql: str

    # --- Execution ---
    df: Any          # pd.DataFrame - stored as Any since TypedDict can't import pandas
    exec_error: str  # non-empty if the first executor attempt failed

    # --- Output ---
    narration: str
    grounding: dict

    # --- Follow-up context (passed in from app.py session state) ---
    prev_sql: str              # SQL from the previous query (empty = no previous query)
    prev_narration: str        # narration text from previous query
    prev_result_shape: tuple   # (rows, cols) of the previous result DataFrame
    prev_result_columns: list[str]  # column names from the previous result
    prev_was_pandas: bool      # True if the previous result came from pandas dispatch
    follow_up_depth: int       # consecutive follow-up count (cap is 3)

    # --- Follow-up classification (set by follow_up_detector node) ---
    is_follow_up: bool         # True if this question was classified as a follow-up
    follow_up_type: str        # "cte_follow_up" | "narration_follow_up" | "fresh"

    # --- CTE mode (set by cte_builder node) ---
    cte_prefix: str            # "WITH prev_result AS (...)" or empty if not CTE mode

    # --- Active table (set by app.py file switcher) ---
    active_table: str          # table to query; schema_loader filters to this table only

    # --- Routing / status flags ---
    force_follow_up: bool      # True when user has follow-up toggle ON in the UI
    is_pandas_dispatch: bool   # True if pandas fast path handled the question
    retried: bool              # True if any retry (SQL error or empty result) occurred
    error: str                 # non-empty = pipeline stops here, surfaces to user
