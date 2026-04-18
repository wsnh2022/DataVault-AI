"""
core/nodes/table_selector.py - Selects relevant tables before SQL generation.

Cheap LLM pre-step: asks the model which tables are needed to answer the question.
SQL generator only receives DDL for selected tables - eliminates ambiguous JOINs
caused by shared column names (e.g. both tables having a 'severity' column).

Single-table databases skip the LLM call entirely.
On any failure (parse error, empty response, LLM error), falls back to all tables.
"""

import json
import logging
from core.llm_client import call_llm
from core.graph_state import PipelineState

logger = logging.getLogger(__name__)


def table_selector_node(state: PipelineState) -> dict:
    tables = state["table_names"]

    # Single table: no selection needed, skip LLM call
    if len(tables) <= 1:
        return {"selected_tables": list(tables)}

    table_ddl = state.get("table_ddl", {})

    # Build column map from actual uploaded schema - no hardcoding
    table_cols: dict[str, list[str]] = {}
    col_to_tables: dict[str, list[str]] = {}
    for tbl in tables:
        cols = _extract_col_names(table_ddl.get(tbl, ""))
        table_cols[tbl] = cols
        for col in cols:
            col_to_tables.setdefault(col, []).append(tbl)

    # Columns that exist in more than one table - computed from actual schema
    shared = {col: tbls for col, tbls in col_to_tables.items() if len(tbls) > 1}

    # Table summaries
    table_lines = [
        f"- {tbl}: {', '.join(table_cols.get(tbl, []))}"
        for tbl in tables
    ]

    # Shared columns section - fully dynamic from schema
    if shared:
        shared_lines = [
            f"  '{col}' appears in: {', '.join(tbls)}"
            for col, tbls in shared.items()
        ]
        shared_section = (
            "\nColumns that exist in multiple tables:\n"
            + "\n".join(shared_lines)
            + "\n"
        )
    else:
        shared_section = ""

    prompt = (
        "You are a database query planner. "
        "Select the MINIMUM number of tables needed to answer the question.\n\n"
        "Tables:\n" + "\n".join(table_lines)
        + shared_section
        + "\nReasoning steps - work through these in order:\n"
        "Step 1: What entity is the question counting, summing, or listing? "
        "Find the table whose primary subject is that entity.\n"
        "Step 2: What is the filter condition? Find which table already contains "
        "that filter column alongside the entity from Step 1.\n"
        "Step 3: If one table satisfies both Step 1 and Step 2, use ONLY that table.\n"
        "Step 4: A JOIN is needed ONLY when the answer requires columns that live in "
        "DIFFERENT tables at the same time and cannot be obtained from a single table.\n"
        "Step 5: A column appearing in multiple tables is NOT a reason to JOIN. "
        "It is a reason to pick the table whose entity best matches the question.\n\n"
        f"Question: {state['question']}\n\n"
        "Return ONLY a JSON array of table names. Example: [\"table_name\"]"
    )

    selected = list(tables)  # default fallback: all tables

    try:
        response = call_llm(
            messages=[{"role": "user", "content": prompt}],
            max_tokens=150,
            temperature=0.0,
        )
        selected = _parse_table_list(response, tables)
        logger.info("Table selector: %s -> %s", list(tables), selected)
    except Exception as exc:
        logger.warning("Table selector LLM call failed (%s) - using all tables", exc)

    return _build_filtered_state(state, selected)


def _extract_col_names(ddl: str) -> list[str]:
    """Extract column names from a CREATE TABLE DDL string."""
    names = []
    for line in ddl.splitlines():
        line = line.strip().rstrip(',')
        if line.startswith('"') and '"' in line[1:]:
            col = line.split('"')[1]
            names.append(col)
    return names


def _parse_table_list(response: str, all_tables: list[str]) -> list[str]:
    """
    Parse LLM response as a JSON array of table names.
    Validates each name against the known table list.
    Falls back to all_tables on parse error or empty result.
    """
    text = response.strip()
    start = text.find('[')
    end = text.rfind(']')
    if start == -1 or end == -1:
        logger.warning("Table selector: no JSON array in response: %r", text[:200])
        return list(all_tables)

    try:
        parsed = json.loads(text[start:end + 1])
    except json.JSONDecodeError:
        logger.warning("Table selector: JSON parse failed on: %r", text[start:end + 1])
        return list(all_tables)

    if not isinstance(parsed, list):
        return list(all_tables)

    valid = [t for t in parsed if t in all_tables]
    if not valid:
        logger.warning("Table selector returned no valid names - using all tables")
        return list(all_tables)

    return valid


def _build_filtered_state(state: PipelineState, selected: list[str]) -> dict:
    """Rebuild ddl, sample_csv, table_names restricted to the selected tables."""
    table_ddl = state.get("table_ddl", {})
    table_samples = state.get("table_samples", {})

    filtered_ddl = "\n\n".join(table_ddl[t] for t in selected if t in table_ddl)
    filtered_samples = "\n".join(table_samples[t] for t in selected if t in table_samples)

    return {
        "selected_tables": selected,
        "ddl": filtered_ddl,
        "sample_csv": filtered_samples,
        "table_names": selected,
    }
