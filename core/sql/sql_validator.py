"""
core/sql_validator.py - Validates generated SQL before execution and handles empty results.

Vanna lesson: is_sql_valid() in base.py uses sqlparse to check query type.
Vanna lesson: RunSqlTool detects empty DataFrame and sets result message separately
from the execution path - same separation here.
Prompt requirement: SQL generation must include one retry if result is empty.
"""

import re
import logging
import sqlparse
import pandas as pd

logger = logging.getLogger(__name__)


class SqlValidator:
    """
    Two responsibilities:
    1. Pre-execution: check the SQL is a safe SELECT before we touch the DB.
    2. Post-execution: detect empty results and signal that a retry is needed.
    """

    # Patterns that should never reach the executor
    _BLOCKED_KEYWORDS = re.compile(
        r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|ATTACH|DETACH)\b",
        re.IGNORECASE,
    )

    def validate_sql(self, sql: str) -> tuple[bool, str]:
        """
        Returns (is_valid, reason).
        is_valid=True means safe to run. False means reject with reason string.
        """
        stripped = sql.strip()

        if not stripped:
            return False, "SQL is empty."

        # Block any destructive or DDL keywords
        match = self._BLOCKED_KEYWORDS.search(stripped)
        if match:
            return False, f"Blocked keyword detected: {match.group(0)}"

        # Require at least a SELECT or WITH to be present
        parsed = sqlparse.parse(stripped)
        if not parsed:
            return False, "Could not parse SQL."

        query_type = parsed[0].get_type()
        # WITH-CTEs parse as UNKNOWN in sqlparse - acceptable here
        if query_type not in ("SELECT", "UNKNOWN"):
            return False, f"Only SELECT queries are permitted. Got type: {query_type}"

        # Final check: first meaningful keyword must be SELECT or WITH
        upper = stripped.upper()
        if not (upper.startswith("SELECT") or upper.startswith("WITH")):
            return False, "Query must begin with SELECT or WITH."

        return True, "ok"

    def is_empty_result(self, df: pd.DataFrame) -> bool:
        """
        Returns True if the DataFrame has no rows.
        Empty result triggers one retry in query_pipeline.py per prompt requirement.
        """
        return df is None or df.empty

    def build_retry_prompt_suffix(self, original_question: str, failed_sql: str) -> str:
        """
        Appended to the user message on the retry attempt.
        Tells the LLM its first SQL returned zero rows and asks for a broader query.
        """
        return (
            f"\n\nNote: The previous SQL query returned zero rows:\n{failed_sql}\n"
            "Please rewrite the query to be less restrictive and return results."
        )
