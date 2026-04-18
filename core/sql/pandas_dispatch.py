"""
core/pandas_dispatch.py - Direct pandas computation for known prompt patterns.

Prompts in the snippet cards that match a key in DISPATCH are computed
directly from the DataFrame - no LLM call, no SQL, no tokens consumed.

Adding a new shortcut:
  1. Write a function: def _my_fn(df: pd.DataFrame) -> pd.DataFrame
  2. Add an entry: DISPATCH["exact prompt text lowercase"] = _my_fn
  3. Add the same text to _DEFAULT_SNIPPETS in app.py (case-insensitive match)
"""

import duckdb
import logging
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Internal loader
# ------------------------------------------------------------------

def _load_df(db_path: str | Path, table_name: str | None = None) -> pd.DataFrame:
    conn = duckdb.connect(str(db_path))
    try:
        if table_name is None:
            rows = conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' AND table_type = 'BASE TABLE'"
            ).fetchall()
            if not rows:
                return pd.DataFrame()
            table_name = rows[0][0]
        return conn.execute(f'SELECT * FROM "{table_name}"').df()
    finally:
        conn.close()


# ------------------------------------------------------------------
# Data Profile operations
# ------------------------------------------------------------------

def _numeric_summary(df: pd.DataFrame) -> pd.DataFrame:
    """min / max / avg / total / count for every numeric column."""
    num = df.select_dtypes(include="number")
    if num.empty:
        return pd.DataFrame({"message": ["No numeric columns found"]})
    result = num.agg(["min", "max", "mean", "sum", "count"]).T.round(2)
    result.index.name = "column"
    result.columns = ["min", "max", "avg", "total", "count"]
    return result.reset_index()


def _unique_counts(df: pd.DataFrame) -> pd.DataFrame:
    """Unique value count per column, sorted least to most."""
    return pd.DataFrame({
        "column":        df.columns.tolist(),
        "unique_values": [df[c].nunique() for c in df.columns],
        "total_rows":    len(df),
    }).sort_values("unique_values").reset_index(drop=True)


def _dtypes_sample(df: pd.DataFrame) -> pd.DataFrame:
    """Data type, non-null count, and a sample value for every column."""
    rows = []
    for c in df.columns:
        non_null = df[c].dropna()
        rows.append({
            "column":        c,
            "dtype":         str(df[c].dtype),
            "non_null_count": int(df[c].notna().sum()),
            "null_count":    int(df[c].isna().sum()),
            "sample_value":  str(non_null.iloc[0])[:50] if not non_null.empty else "N/A",
        })
    return pd.DataFrame(rows)


def _random_sample(df: pd.DataFrame) -> pd.DataFrame:
    """20 random rows from the dataset."""
    return df.sample(min(20, len(df))).reset_index(drop=True)


def _column_spread(df: pd.DataFrame) -> pd.DataFrame:
    """Difference between max and min for every numeric column (highest first)."""
    num = df.select_dtypes(include="number")
    if num.empty:
        return pd.DataFrame({"message": ["No numeric columns found"]})
    spread = (num.max() - num.min()).round(2).reset_index()
    spread.columns = ["column", "spread_max_minus_min"]
    return spread.sort_values("spread_max_minus_min", ascending=False).reset_index(drop=True)


# ------------------------------------------------------------------
# Quality Check operations
# ------------------------------------------------------------------

def _missing_pct(df: pd.DataFrame) -> pd.DataFrame:
    """Percentage of missing values per column, sorted highest to lowest."""
    result = (df.isnull().mean() * 100).round(2).reset_index()
    result.columns = ["column", "missing_pct"]
    return result.sort_values("missing_pct", ascending=False).reset_index(drop=True)


def _duplicate_count(df: pd.DataFrame) -> pd.DataFrame:
    """Count of duplicate rows, unique rows, and duplicate percentage."""
    n_dup = int(df.duplicated().sum())
    total = len(df)
    return pd.DataFrame({
        "total_rows":    [total],
        "duplicate_rows": [n_dup],
        "unique_rows":   [total - n_dup],
        "duplicate_pct": [round(n_dup / total * 100, 2)],
    })


def _low_cardinality_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Columns with fewer than 3 unique values (near-constant, low information)."""
    rows = []
    for c in df.columns:
        n = df[c].nunique()
        if n < 3:
            vals = ", ".join(str(v) for v in df[c].dropna().unique()[:10])
            rows.append({"column": c, "unique_count": n, "values": vals})
    if not rows:
        return pd.DataFrame({"message": ["No columns with fewer than 3 unique values"]})
    return pd.DataFrame(rows)


def _zero_value_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Rows where any numeric column contains exactly zero (first 50 shown)."""
    num = df.select_dtypes(include="number")
    if num.empty:
        return pd.DataFrame({"message": ["No numeric columns found"]})
    mask = (num == 0).any(axis=1)
    result = df[mask].reset_index(drop=True)
    if result.empty:
        return pd.DataFrame({"message": ["No rows with zero values in numeric columns"]})
    return result.head(50)


def _high_missing_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Columns where more than 50% of values are missing."""
    pct = df.isnull().mean() * 100
    high = pct[pct > 50].round(2).reset_index()
    if high.empty:
        return pd.DataFrame({"message": ["No columns with more than 50% missing values"]})
    high.columns = ["column", "missing_pct"]
    return high.sort_values("missing_pct", ascending=False).reset_index(drop=True)


# ------------------------------------------------------------------
# Statistics / percentile (also used by pattern-based shortcut in pipeline)
# ------------------------------------------------------------------

def _percentile_quartiles(df: pd.DataFrame) -> pd.DataFrame:
    """25th, 50th, 75th percentile for every numeric column."""
    num = df.select_dtypes(include="number")
    if num.empty:
        return pd.DataFrame({"message": ["No numeric columns found"]})
    return (
        num.quantile([0.25, 0.50, 0.75])
        .T
        .rename(columns={0.25: "p25", 0.50: "p50", 0.75: "p75"})
        .round(2)
        .reset_index()
        .rename(columns={"index": "column"})
    )


# ------------------------------------------------------------------
# Dispatch table
# Key  = exact prompt text, lowercased and stripped
# Value = function(df: pd.DataFrame) -> pd.DataFrame
# ------------------------------------------------------------------

DISPATCH: dict[str, callable] = {
    # --- Data Profile ---
    "numeric summary: min, max, average, total for all numeric columns": _numeric_summary,
    "how many unique values in each column? (sorted least to most)":     _unique_counts,
    "show data types and sample value for each column":                  _dtypes_sample,
    "show 20 random rows from the dataset":                              _random_sample,
    "which numeric columns have the highest spread between min and max?": _column_spread,

    # --- Quality Check ---
    "missing value percentage for each column (sorted highest to lowest)": _missing_pct,
    "count of duplicate rows in the dataset":                            _duplicate_count,
    "columns with fewer than 3 unique values (near-constant columns)":   _low_cardinality_cols,
    "rows where any numeric column contains a zero value":               _zero_value_rows,
    "which columns have more than 50% missing values?":                  _high_missing_cols,

    # --- Statistics (snippet card) ---
    "show the 25th, 50th, and 75th percentile for all numeric columns":  _percentile_quartiles,
}


def try_dispatch(question: str, db_path: str | Path) -> pd.DataFrame | None:
    """
    If the question exactly matches a known pandas shortcut, compute and return
    the result DataFrame directly. Returns None to fall through to the LLM pipeline.
    For multi-table sessions, runs per table and prepends a "table" column.
    """
    key = question.strip().lower()
    fn = DISPATCH.get(key)
    if fn is None:
        return None
    try:
        conn = duckdb.connect(str(db_path))
        tables = [
            r[0] for r in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' AND table_type = 'BASE TABLE'"
            ).fetchall()
        ]
        conn.close()
        if not tables:
            return None
        if len(tables) == 1:
            df = _load_df(db_path, tables[0])
            if df.empty:
                return None
            return fn(df)
        # Multiple tables: run per table, label results, concatenate
        parts = []
        for tbl in tables:
            df = _load_df(db_path, tbl)
            if df.empty:
                continue
            tbl_result = fn(df)
            tbl_result.insert(0, "table", tbl)
            parts.append(tbl_result)
        if not parts:
            return None
        return pd.concat(parts, ignore_index=True)
    except Exception as exc:
        logger.warning("Pandas dispatch failed for '%s': %s", key, exc)
        return None
