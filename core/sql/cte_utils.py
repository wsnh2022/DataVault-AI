"""
core/cte_utils.py - CTE wrapping utility for follow-up queries.

merge_cte() wraps a previous SQL statement as a named CTE so follow-up
queries can scope to the previous result via: SELECT ... FROM prev_result.

DuckDB supports subquery CTEs (WITH x AS (WITH y AS (...) SELECT ...))
so prev_sql that already contains CTEs is handled safely by simple wrapping.
"""

import re


def merge_cte(prev_sql: str, alias: str = "prev_result") -> str:
    """
    Wraps prev_sql as a named CTE: WITH {alias} AS (\n  {sql}\n)

    Handles:
    - Trailing semicolons (stripped before wrapping)
    - prev_sql that already starts with WITH (DuckDB supports nested CTEs)
    """
    sql = prev_sql.strip().rstrip(";").rstrip()
    return f"WITH {alias} AS (\n  {sql}\n)"
