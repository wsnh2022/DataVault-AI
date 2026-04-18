"""
core/query_pipeline.py - Thin wrapper: builds initial PipelineState, invokes the
LangGraph pipeline, and maps the final state back to QueryResult.

app.py interface is unchanged: QueryPipeline().run(question, db_path) -> QueryResult.
Optional follow_up_context dict carries previous query context from app.py session state.
All pipeline logic lives in core/graph_pipeline.py and core/nodes/.
"""

import logging
import pandas as pd
from dataclasses import dataclass, field
from pathlib import Path

from core.graph_pipeline import pipeline_graph

logger = logging.getLogger(__name__)


@dataclass
class QueryResult:
    """Single return type from QueryPipeline.run(). All fields always populated."""
    question: str
    sql: str                        = ""
    df: pd.DataFrame                = field(default_factory=pd.DataFrame)
    narration: str                  = ""
    grounding: dict                 = field(default_factory=dict)
    error: str                      = ""
    retried: bool                   = False
    is_follow_up: bool              = False


class QueryPipeline:
    """Stateless orchestrator. One method: run(question, db_path)."""

    def run(
        self,
        question: str,
        db_path: str | Path,
        follow_up_context: dict | None = None,
        active_table: str | None = None,
    ) -> QueryResult:
        ctx = follow_up_context or {}

        initial_state = {
            # Inputs
            "question": question,
            "db_path": str(db_path),
            "active_table": active_table or "",
            # Schema (populated by schema_loader)
            "ddl": "",
            "sample_csv": "",
            "table_names": [],
            "table_ddl": {},
            "table_samples": {},
            "selected_tables": [],
            # SQL
            "sql": "",
            # Execution
            "df": pd.DataFrame(),
            "exec_error": "",
            # Output
            "narration": "",
            "grounding": {},
            # Follow-up context (from app.py session state)
            "prev_sql": ctx.get("prev_sql", ""),
            "prev_narration": ctx.get("prev_narration", ""),
            "prev_result_shape": ctx.get("prev_result_shape", (0, 0)),
            "prev_result_columns": ctx.get("prev_result_columns", []),
            "prev_was_pandas": ctx.get("prev_was_pandas", False),
            "follow_up_depth": ctx.get("follow_up_depth", 0),
            # Follow-up classification (set by follow_up_detector node)
            "is_follow_up": False,
            "follow_up_type": "fresh",
            # CTE mode (set by cte_builder node)
            "cte_prefix": "",
            # Flags
            "force_follow_up": ctx.get("force_follow_up", False),
            "is_pandas_dispatch": False,
            "retried": False,
            "error": "",
        }

        try:
            final_state = pipeline_graph.invoke(initial_state)
        except Exception as e:
            logger.exception("pipeline_graph.invoke() unhandled error")
            return QueryResult(question=question, error=str(e))

        return QueryResult(
            question=question,
            sql=final_state.get("sql", ""),
            df=final_state.get("df", pd.DataFrame()),
            narration=final_state.get("narration", ""),
            grounding=final_state.get("grounding", {}),
            error=final_state.get("error", ""),
            retried=final_state.get("retried", False),
            is_follow_up=final_state.get("is_follow_up", False),
        )
