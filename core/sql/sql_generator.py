"""
core/sql_generator.py - Builds SQL generation prompt and calls OpenRouter.

Uses core.llm_client (raw requests) instead of openai package.
Sample rows injected into prompt so LLM has data context for vague questions.
Backtick fix: DuckDB uses double quotes for identifiers, not backticks (MySQL syntax).
Function fix: LLM sometimes outputs MIN "col" instead of MIN("col") - patched in post-process.
Alias fix: aggregate columns must have readable AS aliases, not raw COUNT(*) headers.
Column classifier: infers temporal/metric/dimension/id from DDL to inject a DATA CONTEXT
  section - makes the prompt domain-agnostic (works for sales, medical, academic, etc.).
"""

import re
import logging
from core.llm_client import call_llm
import config

logger = logging.getLogger(__name__)

_SAMPLE_ROWS_IN_PROMPT = 3

# Aggregate functions that must have parentheses around their argument
_AGG_FUNCTIONS = ["MIN", "MAX", "SUM", "AVG", "COUNT", "TOTAL", "GROUP_CONCAT"]

# Column name patterns for classification (no LLM needed)
_ID_PATTERN     = re.compile(r'^id$|_id$|^id_|_key$|^pk$|^fk_', re.IGNORECASE)
_TEMPORAL_PATTERN = re.compile(
    r'date|time|_at$|_on$|_dt$|timestamp|created|updated|year|month|day|week',
    re.IGNORECASE,
)
_NUMERIC_TYPE_PATTERN = re.compile(
    r'\b(INT|INTEGER|REAL|FLOAT|NUMERIC|DECIMAL|DOUBLE|NUMBER|BIGINT|SMALLINT)\b',
    re.IGNORECASE,
)


class SqlGenerator:

    # ------------------------------------------------------------------
    # Column classifier - domain-agnostic context builder
    # ------------------------------------------------------------------

    @staticmethod
    def _classify_columns(ddl: str) -> dict[str, list[str]]:
        """
        Parses one or more CREATE TABLE DDL statements and classifies every column into:
          temporal   - date/time columns (STRFTIME, date math)
          metric     - numeric columns that should be aggregated (SUM/AVG/MIN/MAX)
          dimension  - text/category columns for GROUP BY / filtering
          id         - surrogate/foreign key columns (COUNT DISTINCT only)

        Works purely on column names + column type affinity - no LLM call.
        Handles multi-table DDL (splits on CREATE TABLE so each block is parsed cleanly).
        """
        result: dict[str, list[str]] = {
            "temporal": [], "metric": [], "dimension": [], "id": [],
        }

        # Split on each CREATE TABLE so greedy (.+) only spans one table's column block
        chunks = re.split(r'(?=CREATE\s+TABLE)', ddl, flags=re.IGNORECASE)
        for chunk in chunks:
            block_match = re.search(r'\((.+)\)', chunk, re.DOTALL)
            if not block_match:
                continue

            for raw_line in block_match.group(1).splitlines():
                line = raw_line.strip().rstrip(',')
                if not line:
                    continue
                # Skip table-level constraints
                if re.match(r'(PRIMARY|FOREIGN|UNIQUE|CHECK|INDEX)\b', line, re.IGNORECASE):
                    continue

                # Match: optional quote, column name, optional quote, whitespace, type
                col_match = re.match(r'["\`]?([\w ]+?)["\`]?\s+(\w+)?', line)
                if not col_match:
                    continue

                col_name = col_match.group(1).strip()
                col_type = col_match.group(2) or ""

                if _ID_PATTERN.search(col_name):
                    result["id"].append(col_name)
                elif _TEMPORAL_PATTERN.search(col_name):
                    result["temporal"].append(col_name)
                elif _NUMERIC_TYPE_PATTERN.search(col_type):
                    result["metric"].append(col_name)
                else:
                    result["dimension"].append(col_name)

        return result

    @staticmethod
    def _build_column_context(classified: dict[str, list[str]]) -> str:
        """
        Builds the ===Data Context section from classified columns.
        Empty string if no columns were classified (safe fallback).
        """
        if not any(classified.values()):
            return ""

        lines = ["===Data Context"]
        if classified["temporal"]:
            cols = ", ".join(classified["temporal"])
            lines.append(
                f"- Date/time columns: {cols}\n"
                "  Match time granularity to the question:\n"
                "    year trend / annual / yearly  -> STRFTIME('%Y', col)\n"
                "    month trend / monthly         -> STRFTIME('%Y-%m', col)\n"
                "    day / daily / date            -> STRFTIME('%Y-%m-%d', col)\n"
                "  DATE(col) for date arithmetic."
            )
        if classified["metric"]:
            cols = ", ".join(classified["metric"])
            lines.append(f"- Numeric/metric columns (SUM / AVG / MIN / MAX these): {cols}")
        if classified["dimension"]:
            cols = ", ".join(classified["dimension"])
            lines.append(f"- Category columns (GROUP BY and filter on these): {cols}")
        if classified["id"]:
            cols = ", ".join(classified["id"])
            lines.append(
                f"- ID columns (use COUNT(DISTINCT ...) only - never SUM or AVG): {cols}"
            )
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Prompt builders
    # ------------------------------------------------------------------

    def _build_system_prompt(self, ddl: str, sample_csv: str = "", cte_prefix: str = "", prev_columns: list[str] = None) -> str:
        sample_section = (
            f"\n===Sample Data (first {_SAMPLE_ROWS_IN_PROMPT} rows per table)\n{sample_csv}\n"
            if sample_csv else ""
        )

        classified = self._classify_columns(ddl)
        col_context = self._build_column_context(classified)
        col_context_section = f"\n{col_context}\n" if col_context else ""

        # Pick one real column name to use in inline examples - avoids hardcoding
        # school/domain-specific column names in the prompt
        example_col = (
            (classified["dimension"] or classified["metric"]
             or classified["temporal"] or classified["id"] or ["column_name"])[0]
        )
        example_alias = re.sub(r'\W+', '_', example_col).lower()

        # Follow-up CTE section - injected when querying prev_result
        if cte_prefix and prev_columns:
            col_list = ", ".join(f'"{c}"' for c in prev_columns)
            follow_up_section = (
                "\n===Follow-Up Query Context\n"
                'The user is asking a follow-up question. A CTE named "prev_result" has been\n'
                "prepared containing all rows from the previous query result.\n"
                f'"prev_result" has these columns: {col_list}\n'
                'You MUST write: SELECT ... FROM "prev_result" [WHERE ...] [GROUP BY ...] etc.\n'
                'Do NOT reference any original table directly. Use ONLY "prev_result" as your FROM source.\n'
            )
        else:
            follow_up_section = ""

        return f"""You are a DuckDB SQL expert. Your only job is to write a single valid DuckDB SELECT query.

===Database Schema
{ddl}
{sample_section}{col_context_section}
===Response Guidelines
1. Output ONLY the raw SQL query - no explanation, no markdown, no code fences.
2. Use ONLY the table listed in the schema above. Never reference any other table.
3. Always end the query with a semicolon.
4. Never use INSERT, UPDATE, DELETE, DROP, ALTER, or ATTACH.
5. If the question cannot be answered from the schema, output exactly:
   SELECT 'Cannot answer from available data' AS message;
6. Always write a single-table query. Never use JOIN.
   All columns needed to answer any question are in the one table provided.

===Identifier Quoting
Use double quotes for column and table identifiers (DuckDB/standard SQL, NOT backticks).
  Correct:   SELECT "{example_col}" FROM table_name;
  Incorrect: SELECT `{example_col}` FROM table_name;

===Aggregate Functions
- Always wrap aggregate arguments in parentheses.
  Correct:   MIN("{example_col}"), COUNT(*)
  Incorrect: MIN "{example_col}"
- Every aggregate column MUST have a readable AS alias.
  Correct:   COUNT(*) AS total_count, AVG("{example_col}") AS avg_{example_alias}
  Incorrect: COUNT(*), AVG("{example_col}")

===Text Filtering
- Text comparisons are case-sensitive. Always apply LOWER() to both sides.
  Correct:   WHERE LOWER("{example_col}") = LOWER('value')
  Incorrect: WHERE "{example_col}" = 'Value'

===Percentages and Ratios
- Integer division truncates in DuckDB. Cast to DOUBLE for any ratio or percentage.
  Correct:   CAST(COUNT(CASE WHEN condition THEN 1 END) AS DOUBLE) / COUNT(*) * 100 AS pct
  Incorrect: COUNT(CASE WHEN condition THEN 1 END) / COUNT(*) * 100

- "Convert X to percentage" means multiply the SAME aggregate expression by 100 - never
  use a different group level or a window function for the percentage column.
  If the question asks for avg_score AND avg_score as percentage, both columns MUST derive
  from the identical AVG() call so they are always consistent (pct = score * 100):
  Correct:
    AVG("score") AS avg_score,
    CAST(AVG("score") AS DOUBLE) * 100 AS avg_score_pct
  Incorrect (different aggregation level - values will NOT match * 100):
    AVG("score") AS avg_score,
    AVG("score") OVER (PARTITION BY group_col) * 100 AS avg_score_pct

===Ranking and Sorting
- When LIMIT is used, always include ORDER BY.
- "top / highest / most / best / largest" -> ORDER BY [metric] DESC LIMIT N
- "bottom / lowest / least / worst / smallest" -> ORDER BY [metric] ASC LIMIT N
- Only add LIMIT when the question explicitly asks for top/bottom N.

===Top-N Per Group (Window Functions)
- When the question asks for "the highest/lowest/top/worst [metric] for each/per [group]"
  you MUST return exactly ONE row per group (the winner), NOT all rows sorted.
- Use ROW_NUMBER() OVER (PARTITION BY [group] ORDER BY [metric] DESC) in a CTE.
  Example - "which month had the most accidents in each year":
    WITH ranked AS (
      SELECT
        STRFTIME('%Y', "date") AS year,
        STRFTIME('%m', "date") AS month,
        COUNT(*) AS total_accidents,
        ROW_NUMBER() OVER (
          PARTITION BY STRFTIME('%Y', "date")
          ORDER BY COUNT(*) DESC
        ) AS rn
      FROM table_name
      GROUP BY 1, 2
    )
    SELECT year, month, total_accidents FROM ranked WHERE rn = 1 ORDER BY year;
- Key phrases that require this pattern: "for each [group]", "per [group]",
  "in each [group]", "by each [group]" combined with "highest / most / top / best /
  lowest / least / worst / bottom".
- DuckDB fully supports all window functions (ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD).

===Grouping
- "breakdown / per / by / each / compare / across" -> GROUP BY the relevant category column.
- Return ALL groups unless the question specifies a count limit.
- When filtering AND grouping, keep the GROUP BY so every matching group appears in results.

===NULL Handling
- Use COALESCE(col, 0) when summing or averaging columns that may contain NULLs.
- COUNT(*) counts all rows; COUNT("col") counts only non-NULL values in that column.

===DuckDB Advanced Functions
DuckDB supports the full standard SQL function set - use these directly:
- Percentiles:        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "col") AS median_col
                      PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY "col") AS p25_col
- Standard deviation: STDDEV("col") AS stddev_col
- Variance:           VARIANCE("col") AS variance_col
- String aggregation: STRING_AGG("col", ', ' ORDER BY "col") AS combined
- Date truncation:    DATE_TRUNC('month', "col") AS month  -- alternative to STRFTIME
{follow_up_section}"""

    def _build_user_prompt(self, question: str, table_names: list[str] | str = "", prev_narration: str = "") -> str:
        if isinstance(table_names, str):
            table_names = [table_names] if table_names else []
        table_hint = f"\nYou MUST use FROM \"{table_names[0]}\" in your query.\n" if table_names else ""
        narration_ctx = (
            f"\nPrevious answer for context:\n{prev_narration[:500]}\n"
            if prev_narration else ""
        )
        return f"Question: {question}{table_hint}{narration_ctx}\n\nSQL:"

    # ------------------------------------------------------------------
    # SQL post-processors (unchanged - keep these as safety net)
    # ------------------------------------------------------------------

    @staticmethod
    def _fix_aggregate_syntax(sql: str) -> str:
        """
        Repairs LLM-generated aggregate syntax errors.
        Pattern caught: MIN "col" -> MIN("col"), MAX col -> MAX(col)
        """
        for fn in _AGG_FUNCTIONS:
            pattern = rf'\b{fn}\b\s+("?[\w]+"?)'
            replacement = rf'{fn}(\1)'
            sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)
        return sql

    @staticmethod
    def _fix_backtick_quoting(sql: str) -> str:
        """Replaces MySQL backtick quoting with DuckDB double-quote quoting."""
        return re.sub(r'`([^`]+)`', r'"\1"', sql)

    @staticmethod
    def extract_sql(llm_response: str) -> str:
        text = llm_response.strip()

        # Extract SQL from markdown fences first
        match = re.search(r"```sql\s*\n(.*?)```", text, re.DOTALL | re.IGNORECASE)
        if match:
            text = match.group(1).strip()
        else:
            match = re.search(r"```(.*?)```", text, re.DOTALL)
            if match:
                text = match.group(1).strip()

        sql = text

        m = re.search(r"\bWITH\b.*?;", text, re.DOTALL | re.IGNORECASE)
        if m:
            sql = m.group(0).strip()
        else:
            m = re.search(r"\bSELECT\b.*?;", text, re.DOTALL | re.IGNORECASE)
            if m:
                sql = m.group(0).strip()

        # Post-process: fix common LLM syntax errors
        sql = SqlGenerator._fix_backtick_quoting(sql)
        sql = SqlGenerator._fix_aggregate_syntax(sql)

        return sql

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def generate(
        self,
        question: str,
        ddl: str,
        sample_csv: str = "",
        table_names: list[str] | str = "",
        cte_prefix: str = "",
        prev_narration: str = "",
        prev_columns: list[str] = None,
    ) -> str:
        messages = [
            {"role": "system", "content": self._build_system_prompt(
                ddl, sample_csv,
                cte_prefix=cte_prefix,
                prev_columns=prev_columns or [],
            )},
            {"role": "user", "content": self._build_user_prompt(
                question, table_names,
                prev_narration=prev_narration,
            )},
        ]
        raw = call_llm(messages)
        logger.debug("Raw LLM response: %s", raw)
        sql = self.extract_sql(raw)
        logger.info("Final SQL: %s", sql)
        return sql
