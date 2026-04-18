"""
core/graph_pipeline.py - Assembles and compiles the LangGraph pipeline.

Graph flow:
  START -> fast_path
    "pandas_hit" -> narrator -> grounding -> END
    "miss"       -> follow_up_detector -> schema_loader
      "error" -> END
      "ok"    -> table_selector -> cte_builder -> sql_generator -> validator
        "invalid" -> END
        "valid"   -> executor
          "error"   -> END
          "success" -> empty_check -> narrator -> grounding -> END
"""

from langgraph.graph import StateGraph, START, END

from core.graph_state import PipelineState
from core.nodes.fast_path import fast_path_node, route_fast_path
from core.nodes.follow_up_detector import follow_up_detector_node
from core.nodes.schema_loader import schema_loader_node, route_schema_loader
from core.nodes.table_selector import table_selector_node
from core.nodes.cte_builder import cte_builder_node
from core.nodes.sql_generator import sql_generator_node
from core.nodes.validator import validator_node, route_validator
from core.nodes.executor import executor_node, route_executor
from core.nodes.empty_check import empty_check_node
from core.nodes.narrator import narrator_node
from core.nodes.grounding import grounding_node


def build_graph():
    builder = StateGraph(PipelineState)

    # Register nodes
    builder.add_node("fast_path", fast_path_node)
    builder.add_node("follow_up_detector", follow_up_detector_node)
    builder.add_node("schema_loader", schema_loader_node)
    builder.add_node("table_selector", table_selector_node)
    builder.add_node("cte_builder", cte_builder_node)
    builder.add_node("sql_generator", sql_generator_node)
    builder.add_node("validator", validator_node)
    builder.add_node("executor", executor_node)
    builder.add_node("empty_check", empty_check_node)
    builder.add_node("narrator", narrator_node)
    builder.add_node("grounding", grounding_node)

    # Entry point
    builder.add_edge(START, "fast_path")

    # fast_path branches
    builder.add_conditional_edges(
        "fast_path",
        route_fast_path,
        {"pandas_hit": "narrator", "miss": "follow_up_detector"},
    )

    # follow_up_detector -> schema_loader (always - classification only, no routing needed)
    builder.add_edge("follow_up_detector", "schema_loader")

    # schema_loader branches
    builder.add_conditional_edges(
        "schema_loader",
        route_schema_loader,
        {"error": END, "ok": "table_selector"},
    )

    # table_selector -> cte_builder (always)
    builder.add_edge("table_selector", "cte_builder")

    # cte_builder -> sql_generator (always - returns {} if not in CTE mode)
    builder.add_edge("cte_builder", "sql_generator")

    # sql_generator -> validator (always)
    builder.add_edge("sql_generator", "validator")

    # validator branches
    builder.add_conditional_edges(
        "validator",
        route_validator,
        {"invalid": END, "valid": "executor"},
    )

    # executor branches
    builder.add_conditional_edges(
        "executor",
        route_executor,
        {"error": END, "success": "empty_check"},
    )

    # empty_check -> narrator -> grounding -> END (always)
    builder.add_edge("empty_check", "narrator")
    builder.add_edge("narrator", "grounding")
    builder.add_edge("grounding", END)

    return builder.compile()


pipeline_graph = build_graph()
