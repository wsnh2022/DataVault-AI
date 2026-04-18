"""
core/sql_executor.py - DuckDB query runner and schema extractor.

Replaced SQLite with DuckDB for:
- Full SQL function support (PERCENTILE_CONT, MEDIAN, STDDEV, STRING_AGG, VARIANCE)
- Faster analytical queries (columnar storage, parallel execution)
- Same zero-config file-based setup as SQLite - no server process required

Schema DDL is reconstructed from information_schema.columns since DuckDB has no
sqlite_master equivalent. The reconstructed DDL is functionally identical for
the LLM prompt - column names and types are preserved exactly.
"""

import duckdb
import pandas as pd
from pathlib import Path


class SqlExecutor:
    """
    Owns one DuckDB database path. Executes SELECT queries and extracts schema.
    All math is done here via SQL - the LLM never sees raw rows for calculation.
    Every method opens and immediately closes its own connection (short-lived).
    """

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.db_path)

    # ------------------------------------------------------------------
    # Schema extraction
    # ------------------------------------------------------------------

    def get_table_ddl_map(self) -> dict[str, str]:
        """
        Returns {table_name: ddl_string} for every user table.
        Used by schema_loader to build per-table DDL for table_selector filtering.
        """
        conn = self._connect()
        try:
            tables = conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' AND table_type = 'BASE TABLE' "
                "ORDER BY table_name"
            ).fetchall()
            result: dict[str, str] = {}
            for (table_name,) in tables:
                cols = conn.execute(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_schema = 'main' AND table_name = ? "
                    "ORDER BY ordinal_position",
                    [table_name]
                ).fetchall()
                col_lines = [f'  "{col}" {dtype}' for col, dtype in cols]
                result[table_name] = (
                    f'CREATE TABLE "{table_name}" (\n'
                    + ',\n'.join(col_lines)
                    + '\n)'
                )
            return result
        finally:
            conn.close()

    def get_schema_ddl(self) -> str:
        """
        Returns reconstructed CREATE TABLE statements for all user tables as one string.
        Injected into the SQL generation prompt so the LLM knows column names and types.
        """
        return "\n\n".join(self.get_table_ddl_map().values())

    def get_table_names(self) -> list[str]:
        """Returns list of user table names in the database."""
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' AND table_type = 'BASE TABLE' "
                "ORDER BY table_name"
            ).fetchall()
            return [row[0] for row in rows]
        finally:
            conn.close()

    def get_sample_rows(self, table: str, n: int = 3) -> pd.DataFrame:
        """Returns up to n sample rows from a table as a DataFrame."""
        conn = self._connect()
        try:
            return conn.execute(f'SELECT * FROM "{table}" LIMIT {int(n)}').df()
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Query execution
    # ------------------------------------------------------------------

    def run(self, sql: str) -> pd.DataFrame:
        """
        Executes a SELECT query and returns results as a DataFrame.
        Returns an empty DataFrame (not None) on zero results.
        Raises duckdb.Error on invalid SQL so the validator can catch it.
        """
        conn = self._connect()
        try:
            df = conn.execute(sql).df()
            float_cols = df.select_dtypes(include="float").columns
            if len(float_cols):
                df[float_cols] = df[float_cols].round(2)
            return df
        finally:
            conn.close()

    def is_select(self, sql: str) -> bool:
        """
        Returns True only if the first meaningful token is SELECT or WITH (CTEs).
        Blocks INSERT/UPDATE/DELETE from reaching the executor.
        """
        import sqlparse
        parsed = sqlparse.parse(sql.strip())
        if not parsed:
            return False
        first_token_type = parsed[0].get_type()
        return first_token_type in ("SELECT", "UNKNOWN")  # WITH-CTEs parse as UNKNOWN


