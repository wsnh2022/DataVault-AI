"""
app.py - DataVault AI - NiceGUI UI

Replaces the Streamlit app.py. All of core/, export/, config.py untouched.

Layout
------
Header   : fixed top - title, theme toggle, Clear Chat, New File
Sidebar  : always-visible left drawer - chat history list with delete buttons
Tabs     : Chat | Suggestions | Dataset (sticky below header)
Chat     : upload zone (pre-file) -> scrollable thread + pinned input bar (post-file)
Sidebar  : New Chat button + past sessions with click-to-load and X-to-delete

No emojis anywhere - causes UnicodeEncodeError on Windows Python.
No em dash - use hyphen only.
dotenv_values() not load_dotenv() - see config.py.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import markdown2
import duckdb
import pandas as pd
from nicegui import app, events, ui

import config
from core import chat_store
from core.llm_client import call_llm
from core.query_pipeline import QueryPipeline
from core.sql.sql_executor import SqlExecutor
from core.sql.sql_generator import SqlGenerator
from export.excel_exporter import dataframe_to_excel_bytes
from export.pdf_generator import dataframe_to_pdf_bytes

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CHAT_DB_PATH = config.DATA_DIR / "chat_history.db"
# Single fixed DuckDB file for all uploaded tables - avoids per-file path
# changes and concurrent connection issues between uploads
DUCKDB_PATH = config.DATA_DIR / "datavault.duckdb"
_SNIPPETS_FILE = Path(__file__).parent / "prompt_snippets.json"

# ---------------------------------------------------------------------------
# Default prompt snippets
# ---------------------------------------------------------------------------

_DEFAULT_SNIPPETS: dict[str, list[str]] = {
    "Data Profile": [
        "Numeric summary: min, max, average, total for all numeric columns",
        "How many unique values in each column? (sorted least to most)",
        "Show data types and sample value for each column",
        "Show 20 random rows from the dataset",
        "Which numeric columns have the highest spread between min and max?",
    ],
    "Quality Check": [
        "Missing value percentage for each column (sorted highest to lowest)",
        "Count of duplicate rows in the dataset",
        "Columns with fewer than 3 unique values (near-constant columns)",
        "Rows where any numeric column contains a zero value",
        "Which columns have more than 50% missing values?",
    ],
    "Business Insights": [
        "Which category has the highest total across all numeric columns?",
        "Show rows where any numeric value exceeds twice the column average",
        "Rank all categories by their average numeric value from highest to lowest",
        "What percentage of total does each category contribute?",
        "Show the top 5 and bottom 5 records by the most important numeric column",
    ],
}

# ---------------------------------------------------------------------------
# Application state  (single-user local app - module-level instance is fine)
# ---------------------------------------------------------------------------

@dataclass
class AppState:
    # File / DB
    db_path: Path | None = None
    table_names: list[str] = field(default_factory=list)
    uploaded_filenames: list[str] = field(default_factory=list)
    active_table: str = ""
    column_stats: list[dict] = field(default_factory=list)
    dataset_info: dict = field(default_factory=dict)
    ai_suggestions: list[dict] = field(default_factory=list)

    # Chat session
    current_chat_id: str | None = None
    msg_counter: int = 0

    # Follow-up tracking
    follow_up_enabled: bool = False
    last_sql: str = ""
    last_narration: str = ""
    last_result_shape: tuple = (0, 0)
    last_result_columns: list[str] = field(default_factory=list)
    follow_up_depth: int = 0
    last_was_pandas: bool = False

    # Prompt snippets
    prompt_snippets: dict = field(default_factory=dict)

    # Clarification state (session-only, reset on new chat / load chat)
    clarification_active: bool = False
    clarification_history: list = field(default_factory=list)
    clarification_count: int = 0
    original_question: str = ""
    sanity_warnings: dict = field(default_factory=dict)  # {table_name: list[dict]}



state = AppState()
pipeline = QueryPipeline()

# ---------------------------------------------------------------------------
# Markdown normalizer
# ---------------------------------------------------------------------------

def _fix_markdown(text: str) -> str:
    """Ensure blank line before list blocks and headers so marked.js renders them.

    LLMs often emit 'Some text\n- item' with no blank line before the list.
    Standard CommonMark requires a blank line to start a new list block.
    """
    if not text:
        return text
    # Add blank line before bullet/numbered list lines that lack one
    text = re.sub(r'([^\n])\n([ \t]*[-*+\d][\.\)] )', r'\1\n\n\2', text)
    # Add blank line before ATX headings (# Heading) that lack one
    text = re.sub(r'([^\n])\n(#{1,6} )', r'\1\n\n\2', text)
    return text


# ---------------------------------------------------------------------------
# Prompt snippet helpers
# ---------------------------------------------------------------------------

def load_prompt_snippets() -> dict[str, list[str]]:
    if _SNIPPETS_FILE.exists():
        try:
            with open(_SNIPPETS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
        except Exception:
            pass
    save_prompt_snippets(_DEFAULT_SNIPPETS)
    return {k: list(v) for k, v in _DEFAULT_SNIPPETS.items()}


def save_prompt_snippets(data: dict[str, list[str]]) -> None:
    try:
        with open(_SNIPPETS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as exc:
        logger.warning("Could not save prompt snippets: %s", exc)

# ---------------------------------------------------------------------------
# Data ingest helpers  (logic preserved verbatim from original app.py)
# ---------------------------------------------------------------------------

def sanitize_column_name(col: str) -> str:
    """DuckDB-safe identifier from any column name."""
    col = col.lower().strip()
    col = re.sub(r"[^a-z0-9]+", "_", col)
    col = col.strip("_")
    return col or "col"


_TEMPORAL_COL = re.compile(
    r"date|time|_at$|created|updated|timestamp", re.IGNORECASE
)

_NUMERIC_LIKE_RE = re.compile(r'^[\$£€]?[\d,]+\.?\d*[kKmMbB%]?$')

_NEGATIVE_SUSPICIOUS_COL = re.compile(
    r'qty|quantity|amount|price|revenue|cost|count|units|volume|sales|total',
    re.IGNORECASE,
)

# Matches HH:MM or HH:MM:SS - time-of-day values that should not be treated as dates
_TIME_OF_DAY_RE = re.compile(r'^\d{1,2}:\d{2}(:\d{2})?$')


def _normalize_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convert date-like columns to ISO YYYY-MM-DD so DuckDB STRFTIME works."""
    for col in df.columns:
        if not _TEMPORAL_COL.search(col):
            continue
        if df[col].dtype != object:
            continue
        try:
            converted = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
            valid = converted.dropna()
            if (
                len(valid) / max(len(converted), 1) > 0.5
                and (valid.dt.year >= 1900).mean() > 0.8
            ):
                df[col] = converted.dt.strftime("%Y-%m-%d")
        except Exception:
            pass
    return df


def ingest_file_to_duckdb(
    file_bytes: bytes, filename: str, db_path: Path
) -> tuple[Path, str]:
    """
    Ingest CSV or Excel bytes into DuckDB as a new table.
    Returns (db_path, table_name).
    """
    stem = Path(filename).stem
    suffix = Path(filename).suffix.lower()
    table_name = sanitize_column_name(stem)

    # Write bytes to a temp file so pandas can read it
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp.write(file_bytes)
        tmp_path = Path(tmp.name)

    try:
        if suffix in (".xlsx", ".xls"):
            with pd.ExcelFile(tmp_path) as xl:
                if len(xl.sheet_names) > 1:
                    raise ValueError(
                        f"Multiple sheets detected ({', '.join(xl.sheet_names)}) - "
                        "upload a single-sheet file."
                    )
                df = xl.parse(xl.sheet_names[0])
        else:
            df = pd.read_csv(tmp_path)
    finally:
        tmp_path.unlink(missing_ok=True)

    df.columns = [sanitize_column_name(c) for c in df.columns]
    df = _normalize_date_columns(df)
    for col in df.columns:
        if df[col].dtype == object:
            try:
                df[col] = pd.to_numeric(df[col])
            except (ValueError, TypeError):
                pass

    conn = duckdb.connect(str(db_path))
    conn.register("_upload_tmp", df)
    conn.execute(f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT * FROM _upload_tmp')
    conn.close()
    return db_path, table_name


def get_dataset_info(db_path: Path, table_names: list[str]) -> dict:
    """Return per-table row/column counts."""
    conn = duckdb.connect(str(db_path))
    tables_info = []
    for table_name in table_names:
        rows = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
        cols = conn.execute(
            "SELECT COUNT(*) FROM information_schema.columns "
            "WHERE table_schema = 'main' AND table_name = ?",
            [table_name],
        ).fetchone()[0]
        tables_info.append({"name": table_name, "rows": rows, "columns": cols})
    conn.close()
    return {"tables": tables_info}


def _fmt_stat(v) -> str:
    """Format a numeric stat value to 2 decimal places; pass-through for non-floats."""
    if v is None:
        return "-"
    return f"{v:.2f}" if isinstance(v, float) else str(v)


def compute_column_stats(db_path: Path, table_name: str) -> list[dict]:
    """One dict per column: {name, type, min, max, avg}."""
    _NUMERIC_BASE_TYPES = {
        "INTEGER", "INT", "BIGINT", "HUGEINT", "SMALLINT", "TINYINT",
        "UBIGINT", "UINTEGER", "USMALLINT", "UTINYINT",
        "FLOAT", "DOUBLE", "REAL", "DECIMAL", "NUMERIC",
    }
    conn = duckdb.connect(str(db_path))
    cols_info = conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = 'main' AND table_name = ? ORDER BY ordinal_position",
        [table_name],
    ).fetchall()
    stats: list[dict] = []
    for col_name, data_type in cols_info:
        base_type = data_type.upper().split("(")[0].strip()
        is_numeric = base_type in _NUMERIC_BASE_TYPES
        is_date = base_type in {
            "DATE", "TIMESTAMP", "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE"
        } or bool(_TEMPORAL_COL.search(col_name))
        if is_numeric:
            try:
                r = conn.execute(
                    f'SELECT MIN("{col_name}"), MAX("{col_name}"), '
                    f'ROUND(AVG("{col_name}"), 2) FROM "{table_name}"'
                ).fetchone()
                stats.append({"name": col_name, "type": "number",
                               "min": r[0], "max": r[1], "avg": r[2]})
            except Exception:
                stats.append({"name": col_name, "type": "text"})
        elif is_date:
            try:
                r = conn.execute(
                    f'SELECT MIN("{col_name}"), MAX("{col_name}") FROM "{table_name}"'
                ).fetchone()
                stats.append({"name": col_name, "type": "date",
                               "min": r[0], "max": r[1]})
            except Exception:
                stats.append({"name": col_name, "type": "date"})
        else:
            stats.append({"name": col_name, "type": "text"})
    conn.close()
    return stats


def run_data_sanity_check(
    db_path: Path, table_name: str, column_stats: list[dict]
) -> list[dict]:
    """
    Rule-based data quality checks after ingest. Returns list of issue dicts:
        {"level": "warning"|"error", "column": str|None, "message": str}
    Always fails open - any exception returns empty list so upload is never blocked.
    """
    issues = []
    stats_map = {s["name"]: s for s in column_stats}
    today_str = datetime.now().strftime("%Y-%m-%d")

    # Null-like placeholder strings that look like real values but are not
    _NULL_LIKE = (
        "'N/A'", "'NA'", "'n/a'", "'na'", "'NULL'", "'null'",
        "'None'", "'none'", "'NaN'", "'nan'", "'-'", "'--'",
        "'missing'", "'unknown'", "'UNKNOWN'", "'#N/A'",
        "'TBD'", "'tbd'", "'TBA'", "'tba'",
    )

    try:
        conn = duckdb.connect(str(db_path))

        # ── Table-level: row count ──────────────────────────────────────────
        row_count = conn.execute(
            f'SELECT COUNT(*) FROM "{table_name}"'
        ).fetchone()[0]

        if row_count == 0:
            issues.append({"level": "error", "column": None,
                           "message": "Table has 0 rows - nothing to query"})
            conn.close()
            return issues

        if row_count < 5:
            issues.append({"level": "warning", "column": None,
                           "message": f"Only {row_count} rows - results may not be meaningful"})

        schema_rows = conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'main' AND table_name = ? ORDER BY ordinal_position",
            [table_name],
        ).fetchall()

        # ── Table-level: duplicate rows ─────────────────────────────────────
        try:
            col_list = ", ".join(f'"{c}"' for c, _ in schema_rows)
            distinct_count = conn.execute(
                f'SELECT COUNT(*) FROM (SELECT DISTINCT {col_list} FROM "{table_name}")'
            ).fetchone()[0]
            dup_count = row_count - distinct_count
            dup_pct = dup_count / row_count
            if dup_pct > 0.05:
                issues.append({"level": "warning", "column": None,
                               "message": f"{dup_count:,} duplicate rows ({int(dup_pct * 100)}%) - aggregations may be inflated"})
        except Exception:
            pass

        # ── Per-column checks ───────────────────────────────────────────────
        for col_name, dtype in schema_rows:
            is_text = dtype.upper() in ("VARCHAR", "TEXT")
            stat = stats_map.get(col_name, {})

            # 1. High null percentage (NULL or blank string for text columns)
            try:
                if is_text:
                    null_count = conn.execute(
                        f'SELECT COUNT(*) FROM "{table_name}" '
                        f'WHERE "{col_name}" IS NULL OR TRIM("{col_name}") = \'\''
                    ).fetchone()[0]
                else:
                    null_count = conn.execute(
                        f'SELECT COUNT(*) FROM "{table_name}" WHERE "{col_name}" IS NULL'
                    ).fetchone()[0]
                null_pct = null_count / row_count
                if null_pct > 0.8:
                    issues.append({"level": "warning", "column": col_name,
                                   "message": f"{int(null_pct * 100)}% null or blank - not useful for filtering or grouping"})
                    continue  # Skip remaining checks for this column
            except Exception:
                pass

            # 2. Near-constant OR all-unique (one COUNT DISTINCT serves both)
            try:
                unique_count = conn.execute(
                    f'SELECT COUNT(DISTINCT "{col_name}") FROM "{table_name}"'
                ).fetchone()[0]
                if unique_count == 1:
                    issues.append({"level": "warning", "column": col_name,
                                   "message": "Only 1 unique value - grouping or filtering won't work"})
                elif (unique_count == row_count and row_count > 10
                        and is_text and not _TEMPORAL_COL.search(col_name)):
                    issues.append({"level": "warning", "column": col_name,
                                   "message": "Every value is unique - likely an ID column, GROUP BY won't be meaningful"})
            except Exception:
                pass

            # 3. Null-like placeholder strings ("N/A", "none", "NULL", "-", etc.)
            if is_text:
                try:
                    null_like_count = conn.execute(
                        f'SELECT COUNT(*) FROM "{table_name}" '
                        f'WHERE "{col_name}" IN ({", ".join(_NULL_LIKE)})'
                    ).fetchone()[0]
                    if null_like_count > 0 and null_like_count / row_count > 0.01:
                        issues.append({"level": "warning", "column": col_name,
                                       "message": f"{null_like_count:,} values look like nulls (N/A, none, NULL...) - IS NOT NULL filters won't catch them"})
                except Exception:
                    pass

            # 4. Formatted numeric text ("$1,200", "1.5k", "45%") stored as VARCHAR
            if is_text and not _TEMPORAL_COL.search(col_name):
                try:
                    samples = conn.execute(
                        f'SELECT "{col_name}" FROM "{table_name}" '
                        f'WHERE "{col_name}" IS NOT NULL LIMIT 30'
                    ).fetchall()
                    if samples:
                        vals = [str(r[0]).strip() for r in samples]
                        numeric_like = sum(1 for v in vals if _NUMERIC_LIKE_RE.match(v))
                        if numeric_like / len(vals) > 0.8:
                            issues.append({"level": "warning", "column": col_name,
                                           "message": "Values look numeric but column is text - SUM/AVG won't work (strip $, commas, % before upload)"})
                except Exception:
                    pass

            # Detect time-of-day columns (HH:MM / HH:MM:SS) - skip date checks for these
            is_time_of_day = False
            if _TEMPORAL_COL.search(col_name):
                try:
                    tod_samples = conn.execute(
                        f'SELECT "{col_name}" FROM "{table_name}" '
                        f'WHERE "{col_name}" IS NOT NULL LIMIT 20'
                    ).fetchall()
                    if tod_samples:
                        tod_vals = [str(r[0]).strip() for r in tod_samples]
                        tod_hits = sum(1 for v in tod_vals if _TIME_OF_DAY_RE.match(v))
                        is_time_of_day = tod_hits / len(tod_vals) > 0.8
                except Exception:
                    pass

            # 5. Unparseable date column (temporal name but normalization rejected it)
            if _TEMPORAL_COL.search(col_name) and is_text and not is_time_of_day:
                col_min = stat.get("min")
                normalized_ok = (
                    isinstance(col_min, str)
                    and len(col_min) >= 4
                    and col_min[:4].isdigit()
                    and int(col_min[:4]) >= 1900
                )
                if not normalized_ok:
                    issues.append({"level": "warning", "column": col_name,
                                   "message": "Column name suggests dates but values could not be parsed - date filters may not work"})

            # 6. Future dates in temporal columns
            if _TEMPORAL_COL.search(col_name) and stat.get("type") == "date" and not is_time_of_day:
                col_max = stat.get("max")
                if col_max and str(col_max) > today_str:
                    issues.append({"level": "warning", "column": col_name,
                                   "message": f"Max date {col_max} is in the future - may indicate data entry errors"})

            # 7. Negative values in quantity / amount / price columns
            if (stat.get("type") == "number"
                    and _NEGATIVE_SUSPICIOUS_COL.search(col_name)
                    and stat.get("min") is not None
                    and stat["min"] < 0):
                issues.append({"level": "warning", "column": col_name,
                               "message": f"Contains negative values (min: {stat['min']}) - totals may be understated if these are data errors"})

        conn.close()
    except Exception:
        pass  # Fail open - upload always completes
    return issues


def generate_suggestions_llm(
    db_path: Path, table_name: str, col_stats: list[dict] | None = None
) -> list[dict]:
    """Generate 3 questions at Beginner/Intermediate/Advanced levels via LLM."""
    try:
        executor = SqlExecutor(db_path)
        ddl = executor.get_schema_ddl()
        classified = SqlGenerator._classify_columns(ddl)
        col_context_lines = []
        if classified["temporal"]:
            col_context_lines.append(
                f"- Date/time columns: {', '.join(classified['temporal'])}"
            )
        if classified["metric"]:
            col_context_lines.append(
                f"- Numeric columns: {', '.join(classified['metric'])}"
            )
        if classified["dimension"]:
            col_context_lines.append(
                f"- Category columns: {', '.join(classified['dimension'])}"
            )
        system = (
            "You are a senior data analyst. Generate exactly 3 questions about a dataset "
            "at increasing difficulty levels.\n\n"
            "Output format - exactly 3 lines, no other text:\n"
            "Beginner: <question>\n"
            "Intermediate: <question>\n"
            "Advanced: <question>\n\n"
            "Rules:\n"
            "- Every question must return a results table, not a yes/no answer.\n"
            "- Use only column names that exist in the schema.\n"
            "- If date/time columns exist, include at least one time-based question."
        )
        user = (
            f"Schema:\n{ddl}\n\n"
            f"Column types:\n" + "\n".join(col_context_lines)
        )
        msgs = [{"role": "system", "content": system},
                {"role": "user", "content": user}]
        raw = call_llm(msgs, max_tokens=300, temperature=0.7)
        suggestions = []
        for line in raw.strip().splitlines():
            line = line.strip()
            if ": " in line:
                level, question = line.split(": ", 1)
                level = level.strip()
                if level in ("Beginner", "Intermediate", "Advanced") and question.strip():
                    suggestions.append({"level": level, "question": question.strip()})
            if len(suggestions) == 3:
                break
        return suggestions
    except Exception as exc:
        logger.warning("LLM suggestion generation failed: %s", exc)
        return []

# ---------------------------------------------------------------------------
# Startup hook
# ---------------------------------------------------------------------------

@app.on_startup
async def startup() -> None:
    config.DATA_DIR.mkdir(exist_ok=True)
    chat_store.init_db(CHAT_DB_PATH)
    # Reclaim freed pages from prior deletes - runs in background, non-blocking
    asyncio.get_event_loop().run_in_executor(None, chat_store.vacuum_db, CHAT_DB_PATH)
    state.prompt_snippets = load_prompt_snippets()
    state.db_path = DUCKDB_PATH  # fixed path - never changes between uploads
    try:
        config.validate()
    except ValueError as e:
        logger.error("Config validation error: %s", e)

# ---------------------------------------------------------------------------
# Main page
# ---------------------------------------------------------------------------

@ui.page("/")
def main_page() -> None:
    """Build the full DataVault AI UI.

    All handler functions are defined FIRST so they can be passed directly
    to NiceGUI as async callbacks (no asyncio.create_task wrappers needed).
    NiceGUI awaits async handlers within the client context, so UI updates
    are sent to the correct browser tab.
    """
    dark = ui.dark_mode(False)

    ui.add_head_html("""
    <style>
    .narration-content p          { margin-bottom: 0.6rem; margin-top: 0; }
    .narration-content h1         { font-size: 1rem; font-weight: 700; margin: 1rem 0 0.4rem; }
    .narration-content h2         { font-size: 0.9rem; font-weight: 700; margin: 0.8rem 0 0.3rem; }
    .narration-content h3         { font-size: 0.85rem; font-weight: 600; margin: 0.6rem 0 0.2rem; }
    .narration-content strong,
    .narration-content b          { font-weight: 600; }
    .narration-content ul         { list-style: disc; padding-left: 1.25rem; margin: 0.4rem 0 0.6rem; }
    .narration-content ol         { list-style: decimal; padding-left: 1.25rem; margin: 0.4rem 0 0.6rem; }
    .narration-content li         { margin-top: 0.25rem; line-height: 1.5; }
    .narration-content hr         { border: none; border-top: 1px solid #d1d5db; margin: 0.75rem 0; }
    .narration-content table      { border-collapse: collapse; width: 100%; margin: 0.5rem 0; font-size: 0.8rem; }
    .narration-content th,
    .narration-content td         { border: 1px solid #d1d5db; padding: 0.3rem 0.5rem; text-align: left; }
    .narration-content th         { background: #f3f4f6; font-weight: 600; }
    .body--dark .narration-content hr { border-top-color: #4b5563; }
    .body--dark .narration-content th { background: #1f2937; }
    .body--dark .narration-content th,
    .body--dark .narration-content td { border-color: #374151; }
    </style>
    """)

    # =========================================================================
    # Handler definitions  (UI element refs are closure variables - looked up
    # at CALL time, so they work even though the elements are built below)
    # =========================================================================

    # ── Data refresh (re-scan existing DB on reload) ─────────────────────────

    async def _refresh_data() -> None:
        """Re-scan DUCKDB_PATH for existing tables and repopulate state + UI tabs.

        Called automatically on page load (via ui.timer) so that suggestions and
        dataset tabs work even after a browser reload when DuckDB already has data.
        Also wired to the Refresh button in the header for on-demand use.
        """
        if not DUCKDB_PATH.exists():
            ui.notify("No database file found yet - upload a file first", type="warning")
            return
        try:
            conn = duckdb.connect(str(DUCKDB_PATH))
            tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
            conn.close()
        except Exception as exc:
            logger.warning("Could not scan database: %s", exc)
            ui.notify(f"Could not read database: {exc}", type="negative")
            return

        if not tables:
            return  # DB exists but is empty - no notification needed

        # Merge with existing state (don't wipe files the user uploaded this session)
        for t in tables:
            if t not in state.table_names:
                state.table_names.append(t)
                state.uploaded_filenames.append(t)
        if not state.active_table and state.table_names:
            state.active_table = state.table_names[0]

        state.dataset_info = await asyncio.to_thread(
            get_dataset_info, DUCKDB_PATH, state.table_names
        )
        state.column_stats = await asyncio.to_thread(
            compute_column_stats, DUCKDB_PATH, state.active_table
        )

        _activate_chat_view()
        _rebuild_dataset_tab()
        _rebuild_suggest_tab()
        _refresh_sidebar()
        asyncio.create_task(_fetch_suggestions_bg())
        ui.notify("Data refreshed", type="positive", timeout=2000)

    # ── Sidebar ──────────────────────────────────────────────────────────────

    def _refresh_sidebar() -> None:
        sidebar_chats.clear()
        chats = chat_store.list_chats(CHAT_DB_PATH)
        if not chats:
            with sidebar_chats:
                ui.label("No chats yet").classes(
                    "text-xs text-gray-500 px-2 py-2 italic"
                )
            return
        with sidebar_chats:
            for chat in chats:
                title_text = chat["title"]
                try:
                    dt = datetime.fromisoformat(chat["created_at"]).astimezone()
                    date_text = dt.strftime("%b %d, %Y  %I:%M %p")
                except Exception:
                    date_text = ""
                with ui.row().classes(
                    "w-full items-center gap-0 px-1 py-0.5"
                    " rounded hover:bg-gray-700 group cursor-pointer overflow-hidden"
                ):
                    with (
                        ui.column()
                        .classes("flex-1 min-w-0 gap-0 cursor-pointer")
                        .on("click", lambda c=chat: _load_chat(c["id"]))
                    ):
                        ui.label(title_text).classes(
                            "text-sm text-white truncate min-w-0 py-0 px-1 leading-tight"
                        )
                        if date_text:
                            ui.label(date_text).classes(
                                "text-xs text-gray-400 px-1 leading-tight"
                            )
                    (
                        ui.button(
                            "X",
                            on_click=lambda c=chat: _delete_chat(c["id"]),
                        )
                        .classes("text-red-400 text-xs opacity-0 group-hover:opacity-100 shrink-0")
                        .props("flat dense")
                    )

    # ── View transitions ─────────────────────────────────────────────────────

    def _activate_chat_view() -> None:
        upload_zone.set_visibility(False)
        chat_messages.set_visibility(True)
        # ui.footer uses Quasar value prop - must set .value, not set_visibility
        input_footer.value = True
        if len(state.table_names) > 1:
            file_switcher.set_options(state.uploaded_filenames)
            file_switcher.value = (
                state.uploaded_filenames[state.table_names.index(state.active_table)]
                if state.active_table in state.table_names
                else state.uploaded_filenames[0]
            )
            file_switcher_row.set_visibility(True)

    def _clear_chat() -> None:
        chat_messages.clear()
        state.current_chat_id = None
        _reset_clarification_state()

    def _new_chat() -> None:
        state.current_chat_id = None
        state.msg_counter = 0
        state.follow_up_enabled = False
        state.last_sql = ""
        state.last_narration = ""
        state.last_result_shape = (0, 0)
        state.last_result_columns = []
        state.follow_up_depth = 0
        state.last_was_pandas = False
        _reset_clarification_state()
        chat_messages.clear()
        tabs.set_value(tab_chat)
        if state.db_path and state.table_names:
            # File already loaded - stay in chat view, just cleared
            chat_messages.set_visibility(True)
            upload_zone.set_visibility(False)
            input_footer.value = True
        else:
            # No file loaded - return to upload screen
            upload_zone.set_visibility(True)
            chat_messages.set_visibility(False)
            input_footer.value = False
        _refresh_sidebar()

    def _show_upload() -> None:
        upload_zone.set_visibility(True)
        chat_messages.set_visibility(False)
        input_footer.value = False
        tabs.set_value(tab_chat)

    def _on_file_switch(e) -> None:
        filename = e.value
        if filename in state.uploaded_filenames:
            idx = state.uploaded_filenames.index(filename)
            state.active_table = state.table_names[idx]
            state.last_sql = ""
            state.last_narration = ""
            state.last_result_shape = (0, 0)
            state.last_result_columns = []
            state.follow_up_depth = 0
            state.last_was_pandas = False
            _reset_clarification_state()

    # ── Upload + ingest ──────────────────────────────────────────────────────

    async def _handle_upload(e: events.UploadEventArguments) -> None:
        # NiceGUI 3.10.0 API: e.file.name, await e.file.read() (async)
        filename = e.file.name
        try:
            file_bytes = await e.file.read()
        except Exception as exc:
            ui.notify(f"Could not read upload: {exc}", type="negative", timeout=8000)
            return

        ui.notify(f"Processing '{filename}'...", type="info", timeout=3000)
        try:
            # Always ingest into the fixed shared database - no per-file paths
            _db_path, table_name = await asyncio.to_thread(
                ingest_file_to_duckdb, file_bytes, filename, DUCKDB_PATH
            )
            # state.db_path is already DUCKDB_PATH (set at startup); no change needed
            if table_name not in state.table_names:
                state.table_names.append(table_name)
                state.uploaded_filenames.append(filename)
            state.active_table = table_name

            state.dataset_info = await asyncio.to_thread(
                get_dataset_info, DUCKDB_PATH, state.table_names
            )
            state.column_stats = await asyncio.to_thread(
                compute_column_stats, DUCKDB_PATH, table_name
            )
            warnings = await asyncio.to_thread(
                run_data_sanity_check, DUCKDB_PATH, table_name, state.column_stats
            )
            state.sanity_warnings[table_name] = warnings

            _activate_chat_view()

            with chat_messages:
                _render_dataset_card(filename, state.dataset_info)

            _rebuild_suggest_tab()
            _rebuild_dataset_tab()
            if warnings:
                ui.notify(
                    f"'{filename}' loaded with {len(warnings)} data quality notice(s) - check the Dataset tab",
                    type="warning",
                    timeout=8000,
                )
            else:
                ui.notify(f"'{filename}' loaded successfully", type="positive")

            # AI suggestions in background - client context copied via contextvars
            asyncio.create_task(_fetch_suggestions_bg())

        except Exception as exc:
            logger.exception("Upload handler error")
            ui.notify(
                f"Error processing '{filename}': {exc}",
                type="negative",
                timeout=10000,
            )
            uploader.reset()

    async def _fetch_suggestions_bg() -> None:
        suggestions = await asyncio.to_thread(
            generate_suggestions_llm,
            state.db_path,
            state.active_table,
            state.column_stats,
        )
        state.ai_suggestions = suggestions
        _rebuild_suggest_tab()

    # ── Clarification helpers ─────────────────────────────────────────────────

    def _get_active_ddl() -> str:
        if not state.db_path or not state.active_table:
            return ""
        try:
            ddl_map = SqlExecutor(state.db_path).get_table_ddl_map()
            return ddl_map.get(state.active_table, "")
        except Exception:
            return ""

    def _reset_clarification_state() -> None:
        state.clarification_active = False
        state.clarification_history = []
        state.clarification_count = 0
        state.original_question = ""

    def _show_clarification_card(text: str, chips: list[str] = []) -> None:
        with chat_messages:
            with ui.card().classes("w-full border border-blue-700 bg-blue-950 p-3"):
                with ui.row().classes("items-start gap-2"):
                    ui.icon("help_outline", size="sm").classes("text-blue-400 shrink-0 mt-0.5")
                    with ui.column().classes("gap-1 w-full"):
                        ui.label("Need a bit more detail").classes("text-blue-300 text-xs font-semibold")
                        ui.label(text).classes("text-blue-100 text-sm leading-relaxed")
                        if chips:
                            with ui.row().classes("gap-2 flex-wrap mt-2"):
                                for chip_text in chips:
                                    def _make_chip_handler(answer: str):
                                        async def _on_chip_click():
                                            chat_input.value = answer
                                            await _handle_send()
                                        return _on_chip_click
                                    (
                                        ui.chip(chip_text, on_click=_make_chip_handler(chip_text))
                                        .props("clickable outline color=blue-4")
                                        .classes("text-xs text-blue-200 cursor-pointer")
                                    )

    def _save_clarification_message(text: str) -> None:
        if state.current_chat_id is None:
            return
        chat_store.save_message(CHAT_DB_PATH, state.current_chat_id, "assistant", text)

    def _build_enriched_question(original: str, history: list) -> str:
        user_answers = [m["content"] for m in history if m["role"] == "user"]
        if not user_answers:
            return original
        answers_text = "; ".join(user_answers)
        return f"{original}. Additional context: {answers_text}"

    def _check_ambiguity(question: str, ddl: str) -> tuple[bool, str, list[str]]:
        try:
            system = (
                "You are a data analyst assistant. Given a user question and a table schema, "
                "decide if the question is too vague to write reliable SQL. "
                "A question is vague only if 3 or more of these are critically unclear: "
                "time scope, thresholds/filters, granularity, metric definition, "
                "scope/population, comparison baseline, output shape. "
                "If the question is detailed enough to generate reasonable SQL, it is NOT vague. "
                "If vague, write ONE focused clarifying question (closed/bounded preferred). "
                "Also provide 2-3 concrete short answer chips a non-technical user could click. "
                "Chips must be plain phrases, under 6 words, no punctuation. "
                'Respond with JSON only: {"is_vague": bool, "question": "...or empty", "chips": ["Option A", "Option B", "Option C"]}'
            )
            user_msg = f"Schema:\n{ddl}\n\nQuestion: {question}"
            raw = call_llm(
                [{"role": "system", "content": system},
                 {"role": "user", "content": user_msg}],
                max_tokens=250,
                temperature=0.0,
            )
            raw = raw.strip().strip("```json").strip("```").strip()
            data = json.loads(raw)
            chips = [str(c)[:40] for c in data.get("chips", []) if c][:3]
            return bool(data.get("is_vague", False)), str(data.get("question", "")), chips
        except Exception as exc:
            logger.warning("Ambiguity check failed: %s", exc)
            return False, "", []

    def _check_clarification_complete(original_q: str, history: list) -> tuple[bool, str, list[str]]:
        try:
            history_text = "\n".join(f"{m['role'].upper()}: {m['content']}" for m in history)
            system = (
                "You are a data analyst assistant. Given an original question and a clarification "
                "conversation, decide if you now have enough detail to write reliable SQL. "
                "If yes: value = a single enriched question combining the original + user answers. "
                "If no: value = the next single clarifying question (closed/bounded preferred), "
                "and provide 2-3 concrete short answer chips (plain phrases, under 6 words, no punctuation). "
                'Respond with JSON only: {"is_clear": bool, "value": "...", "chips": ["Option A", "Option B"]}'
            )
            user_msg = f"Original question: {original_q}\n\nClarification so far:\n{history_text}"
            raw = call_llm(
                [{"role": "system", "content": system},
                 {"role": "user", "content": user_msg}],
                max_tokens=280,
                temperature=0.0,
            )
            raw = raw.strip().strip("```json").strip("```").strip()
            data = json.loads(raw)
            chips = [str(c)[:40] for c in data.get("chips", []) if c][:3]
            return bool(data.get("is_clear", True)), str(data.get("value", original_q)), chips
        except Exception as exc:
            logger.warning("Clarification complete check failed: %s", exc)
            return True, original_q, []

    # ── Chat send ────────────────────────────────────────────────────────────

    async def _handle_send() -> None:
        question = chat_input.value.strip()
        if not question or state.db_path is None:
            return
        chat_input.value = ""

        with chat_messages:
            ui.chat_message(question, name="You", sent=True).classes("text-sm")

        # ── Clarification branch ─────────────────────────────────────────────
        if state.clarification_active:
            state.clarification_history.append({"role": "user", "content": question})
            state.clarification_count += 1

            # Detect impatience signals - user wants to proceed without more questions
            _proceed_signals = ("just", "generate", "don't ask", "no more", "proceed",
                                "go ahead", "skip", "ignore", "whatever", "thats all",
                                "that's all", "stop asking", "enough")
            _user_wants_to_proceed = any(s in question.lower() for s in _proceed_signals)

            if state.clarification_count >= 2 or _user_wants_to_proceed:
                enriched = _build_enriched_question(state.original_question, state.clarification_history)
                _reset_clarification_state()
                question = enriched
            else:
                is_clear, value, next_chips = await asyncio.to_thread(
                    _check_clarification_complete,
                    state.original_question,
                    state.clarification_history,
                )
                if is_clear:
                    _reset_clarification_state()
                    question = value
                else:
                    state.clarification_history.append({"role": "assistant", "content": value})
                    _show_clarification_card(value, next_chips)
                    if state.current_chat_id is None:
                        state.current_chat_id = chat_store.create_chat(
                            CHAT_DB_PATH,
                            state.original_question[:50],
                            state.uploaded_filenames[0] if state.uploaded_filenames else None,
                        )
                        _refresh_sidebar()
                    chat_store.save_message(CHAT_DB_PATH, state.current_chat_id, "user", question)
                    _save_clarification_message(value)
                    return

        elif not state.follow_up_enabled:
            # Fresh question - check ambiguity (skip for follow-ups)
            ddl = await asyncio.to_thread(_get_active_ddl)
            if ddl:
                is_vague, clarification_q, clarification_chips = await asyncio.to_thread(
                    _check_ambiguity, question, ddl
                )
                if is_vague and clarification_q:
                    state.clarification_active = True
                    state.original_question = question
                    state.clarification_count = 1
                    state.clarification_history = [{"role": "assistant", "content": clarification_q}]
                    if state.current_chat_id is None:
                        state.current_chat_id = chat_store.create_chat(
                            CHAT_DB_PATH,
                            question[:50],
                            state.uploaded_filenames[0] if state.uploaded_filenames else None,
                        )
                        _refresh_sidebar()
                    chat_store.save_message(CHAT_DB_PATH, state.current_chat_id, "user", question)
                    _show_clarification_card(clarification_q, clarification_chips)
                    _save_clarification_message(clarification_q)
                    return

        # ── Run pipeline ─────────────────────────────────────────────────────
        follow_up_ctx: dict | None = None
        if state.follow_up_enabled and state.last_sql:
            follow_up_ctx = {
                "prev_sql": state.last_sql,
                "prev_narration": state.last_narration,
                "prev_result_shape": state.last_result_shape,
                "prev_result_columns": state.last_result_columns,
                "prev_was_pandas": state.last_was_pandas,
                "follow_up_depth": state.follow_up_depth,
                "force_follow_up": True,
            }

        with chat_messages:
            spinner_row = ui.row().classes("items-center gap-2 px-2")
            with spinner_row:
                ui.spinner(size="sm")
                ui.label("Thinking...").classes("text-sm text-gray-600 dark:text-gray-400")

        result = await asyncio.to_thread(
            pipeline.run,
            question,
            state.db_path,
            follow_up_ctx,
            state.active_table,
        )
        spinner_row.delete()

        state.msg_counter += 1
        msg_id = state.msg_counter
        with chat_messages:
            _render_assistant_msg(result, msg_id)

        if not result.error:
            state.last_sql = result.sql
            state.last_narration = result.narration
            state.last_result_shape = (
                (len(result.df), len(result.df.columns))
                if result.df is not None
                else (0, 0)
            )
            state.last_result_columns = (
                list(result.df.columns) if result.df is not None else []
            )
            state.last_was_pandas = result.sql.startswith("-- Computed with pandas")
            state.follow_up_depth = (
                min(state.follow_up_depth + 1, 3) if result.is_follow_up else 0
            )

        if state.current_chat_id is None:
            state.current_chat_id = chat_store.create_chat(
                CHAT_DB_PATH,
                question[:50],
                state.uploaded_filenames[0] if state.uploaded_filenames else None,
            )
            _refresh_sidebar()

        chat_store.save_message(CHAT_DB_PATH, state.current_chat_id, "user", question)

        if not result.error:
            # Cap to 500 rows for storage - prevents multi-MB JSON blobs
            # (full data is always re-queryable; this is just for chat restore)
            _df_store = result.df.head(500) if result.df is not None else None
            df_json = chat_store.df_to_json(_df_store) if _df_store is not None else ""
            grounding_json = json.dumps(result.grounding) if result.grounding else ""
            chat_store.save_message(
                CHAT_DB_PATH,
                state.current_chat_id,
                "assistant",
                result.narration,
                result.sql,
                df_json,
                grounding_json,
                result.retried,
                result.error,
                result.is_follow_up,
                state.follow_up_depth,
            )
        else:
            chat_store.save_message(
                CHAT_DB_PATH,
                state.current_chat_id,
                "assistant",
                result.error,
                error=result.error,
            )

        chat_store.update_chat(CHAT_DB_PATH, state.current_chat_id)
        _refresh_sidebar()
        await ui.run_javascript(
            "window.scrollTo({top: document.body.scrollHeight, behavior: 'smooth'})"
        )

    # ── Chat history load / delete ───────────────────────────────────────────

    def _load_chat(chat_id: str) -> None:
        _reset_clarification_state()
        state.current_chat_id = chat_id
        state.msg_counter = 0
        chat_messages.set_visibility(True)
        upload_zone.set_visibility(False)
        input_footer.value = True
        chat_messages.clear()
        rows = chat_store.load_messages(CHAT_DB_PATH, chat_id)

        class _R:
            pass

        with chat_messages:
            for row in rows:
                if row["role"] == "user":
                    ui.chat_message(
                        row["content"], name="You", sent=True
                    ).classes("text-sm")
                elif row["role"] == "assistant":
                    df = chat_store.df_from_json(row["df_json"] or "")
                    grounding: dict = {}
                    if row["grounding_json"]:
                        try:
                            grounding = json.loads(row["grounding_json"])
                        except Exception:
                            pass
                    state.msg_counter += 1
                    r = _R()
                    r.sql = row["sql_text"] or ""
                    r.narration = row["content"]
                    r.df = df
                    r.grounding = grounding
                    r.retried = bool(row["retried"])
                    r.error = row["error"] or ""
                    r.is_follow_up = bool(row["is_follow_up"])
                    r.follow_up_depth = row["follow_up_depth"]
                    _render_assistant_msg(r, state.msg_counter)

        tabs.set_value(tab_chat)

    async def _delete_chat(chat_id: str) -> None:
        await asyncio.to_thread(chat_store.delete_chat, CHAT_DB_PATH, chat_id)
        if state.current_chat_id == chat_id:
            _new_chat()
        _refresh_sidebar()
        ui.notify("Chat deleted", type="positive")
        # Reclaim freed pages immediately - runs in background, non-blocking
        asyncio.get_event_loop().run_in_executor(None, chat_store.vacuum_db, CHAT_DB_PATH)

    # ── Rendering ────────────────────────────────────────────────────────────

    def _render_dataset_card(filename: str, dataset_info: dict) -> None:
        tables_info = dataset_info.get("tables", [])
        total_rows = sum(t["rows"] for t in tables_info)
        with ui.card().classes("w-full bg-indigo-950 border border-indigo-800"):
            ui.label(f"'{filename}' loaded successfully").classes(
                "font-semibold text-indigo-200"
            )
            with ui.row().classes("gap-6 mt-2"):
                ui.label(f"Rows: {total_rows:,}").classes("text-sm text-gray-300")
                ui.label(f"Tables: {len(tables_info)}").classes("text-sm text-gray-300")
                ui.label(
                    f"Columns: {sum(t['columns'] for t in tables_info)}"
                ).classes("text-sm text-gray-300")
            ui.label("Use the Suggestions tab for starter questions.").classes(
                "text-xs text-indigo-400 mt-1"
            )

    def _render_assistant_msg(result, msg_id: int) -> None:
        if result.error:
            with ui.card().classes("w-full border border-red-700 bg-red-950 p-3"):
                with ui.row().classes("items-start gap-2"):
                    ui.icon("error_outline", size="sm").classes("text-red-400 shrink-0 mt-0.5")
                    ui.label(f"Error: {result.error}").classes("text-red-300 text-sm")
            return

        with ui.card().classes("w-full border border-gray-700 p-0"):
            # -- Narration ------------------------------------------------
            with ui.element("div").classes("px-4 pt-4 pb-2 narration-body"):
                interpreted_q = getattr(result, "question", "")
                if interpreted_q and not result.error:
                    _display_q = interpreted_q[:120] + ("..." if len(interpreted_q) > 120 else "")
                    ui.label(f"Interpreted as: {_display_q}").classes("text-xs text-gray-500 italic mb-2")
                _narration_html = markdown2.markdown(
                    _fix_markdown(result.narration or ""),
                    extras=["fenced-code-blocks", "tables", "break-on-newline"],
                )
                ui.html(_narration_html).classes("text-sm leading-relaxed narration-content")

                # Grounding warning banner
                grounding = result.grounding or {}
                if not grounding.get("is_grounded", True):
                    flagged = grounding.get("flagged", [])
                    with ui.row().classes(
                        "items-start gap-1 mt-2 p-2 rounded"
                        " bg-yellow-950 border border-yellow-800"
                    ):
                        ui.icon("warning", size="xs").classes(
                            "text-yellow-400 mt-0.5 shrink-0"
                        )
                        ui.label(
                            "Some numbers not found verbatim in data: "
                            + ", ".join(str(f) for f in flagged)
                        ).classes("text-xs text-yellow-300")

            # -- Data table -----------------------------------------------
            df = result.df if hasattr(result, "df") else None
            has_table = df is not None and isinstance(df, pd.DataFrame) and not df.empty

            if has_table:
                cols = [
                    {
                        "name": c,
                        "label": c.replace("_", " ").title(),
                        "field": c,
                        "sortable": True,
                        "align": "left",
                    }
                    for c in df.columns
                ]
                rows = df.head(200).to_dict("records")

                with ui.element("div").classes(
                    "w-full overflow-x-auto border-t border-gray-700"
                ):
                    ui.table(columns=cols, rows=rows).classes(
                        "w-full text-xs"
                    ).props("dense flat separator=cell")

                ui.label(
                    f"{len(df):,} row{'s' if len(df) != 1 else ''}"
                    f" - {len(df.columns)} columns"
                ).classes("text-xs text-gray-500 px-4 py-1")

                # Factory functions for async handlers
                def _pdf_handler(d=df, m=msg_id):
                    async def _h():
                        await _download_pdf(d, m)
                    return _h

                def _excel_handler(d=df, m=msg_id):
                    async def _h():
                        await _download_excel(d, m)
                    return _h

            # -- Action bar -----------------------------------------------
            sql = result.sql or ""
            is_pandas = sql.startswith("-- Computed with pandas")
            sql_label = "pandas" if is_pandas else "View SQL"

            ui.separator().classes("border-gray-700")
            with ui.row().classes("w-full items-center gap-1 px-3 py-1 flex-wrap"):
                if has_table:
                    ui.button("PDF", on_click=_pdf_handler()).props(
                        "flat dense size=sm color=red-4 icon=picture_as_pdf no-caps"
                    )
                    ui.button("Excel", on_click=_excel_handler()).props(
                        "flat dense size=sm color=green-6 icon=table_chart no-caps"
                    )

                # SQL toggle pushed to the right
                with ui.expansion(sql_label, icon="code").classes(
                    "ml-auto text-xs text-gray-600 dark:text-gray-400"
                ).props("dense flat"):
                    if is_pandas:
                        ui.label(
                            "Computed with pandas - no SQL generated."
                        ).classes("text-gray-500 text-xs px-2 pb-1")
                    else:
                        ui.code(sql, language="sql").classes("w-full text-xs")

                # Status chips - subtle, right-aligned
                if getattr(result, "is_follow_up", False):
                    depth = getattr(result, "follow_up_depth", 1)
                    (
                        ui.chip(f"follow-up {depth}", icon="reply")
                        .props("dense outline color=indigo-4")
                        .classes("text-xs")
                        .tooltip(
                            f"Built on previous query context (depth {depth})"
                        )
                    )

    # ── Suggestions tab ──────────────────────────────────────────────────────

    def _rebuild_suggest_tab() -> None:
        suggest_container.clear()
        with suggest_container:
            if state.ai_suggestions:
                with ui.card().classes("w-full"):
                    ui.label("AI Suggestions for this dataset").classes(
                        "font-semibold mb-2"
                    )
                    for s in state.ai_suggestions:
                        def _make_suggest_handler(q: str):
                            async def _h():
                                await _send_suggestion(q)
                            return _h
                        with ui.row().classes("items-start gap-2 mb-1"):
                            ui.badge(s["level"]).classes(
                                "bg-indigo-700 text-white text-xs mt-1 shrink-0"
                            )
                            ui.button(
                                s["question"],
                                on_click=_make_suggest_handler(s["question"]),
                            ).props("flat no-caps dense").classes(
                                "text-left text-sm flex-1"
                            )

            for group_name, prompts in state.prompt_snippets.items():
                with ui.card().classes("w-full"):
                    ui.label(group_name).classes("font-semibold mb-2")
                    for prompt in list(prompts):
                        def _make_suggest_handler(q: str):
                            async def _h():
                                await _send_suggestion(q)
                            return _h
                        def _make_delete_handler(g: str, p: str):
                            def _h():
                                _delete_snippet(g, p)
                            return _h
                        with ui.row().classes("items-center justify-between mb-1 group"):
                            ui.button(
                                prompt,
                                on_click=_make_suggest_handler(prompt),
                            ).props("flat no-caps dense").classes(
                                "text-left text-sm flex-1"
                            )
                            ui.button(
                                "Delete",
                                on_click=_make_delete_handler(group_name, prompt),
                            ).props("flat dense").classes(
                                "text-red-400 text-xs opacity-0 group-hover:opacity-100"
                            )

                    with ui.row().classes("items-center gap-2 mt-2"):
                        new_input = ui.input(
                            placeholder="Add a prompt..."
                        ).classes("flex-1 text-sm").props("dense outlined")
                        def _make_add_handler(g: str, inp=new_input):
                            def _h():
                                _add_snippet(g, inp)
                            return _h
                        ui.button(
                            "Add", on_click=_make_add_handler(group_name)
                        ).props("flat dense color=indigo")

    async def _send_suggestion(question: str) -> None:
        chat_input.value = question
        tabs.set_value(tab_chat)
        await _handle_send()

    def _add_snippet(group: str, inp) -> None:
        text = inp.value.strip()
        if not text:
            return
        state.prompt_snippets.setdefault(group, []).append(text)
        save_prompt_snippets(state.prompt_snippets)
        inp.value = ""
        _rebuild_suggest_tab()

    def _delete_snippet(group: str, prompt: str) -> None:
        if group in state.prompt_snippets:
            try:
                state.prompt_snippets[group].remove(prompt)
            except ValueError:
                pass
            save_prompt_snippets(state.prompt_snippets)
        _rebuild_suggest_tab()

    # ── Dataset tab ──────────────────────────────────────────────────────────

    async def _clear_dataset() -> None:
        """Drop all tables from DuckDB and reset state back to the upload screen."""
        if not state.table_names:
            ui.notify("No dataset loaded", type="warning")
            return

        def _drop_tables() -> None:
            conn = duckdb.connect(str(DUCKDB_PATH))
            for table in state.table_names:
                conn.execute(f'DROP TABLE IF EXISTS "{table}"')
            conn.close()

        try:
            await asyncio.to_thread(_drop_tables)
        except Exception as exc:
            ui.notify(f"Could not clear data: {exc}", type="negative")
            return

        # Reset all data-related state
        state.table_names.clear()
        state.uploaded_filenames.clear()
        state.active_table = ""
        state.dataset_info = {}
        state.column_stats = []
        state.ai_suggestions = []
        state.sanity_warnings.clear()
        state.last_sql = ""
        state.last_narration = ""
        state.last_result_shape = (0, 0)
        state.last_result_columns = []

        # Reset UI
        chat_messages.clear()
        file_switcher.set_options([])
        upload_zone.set_visibility(True)
        chat_messages.set_visibility(False)
        input_footer.value = False
        file_switcher_row.set_visibility(False)
        tabs.set_value(tab_chat)
        _rebuild_dataset_tab()
        _rebuild_suggest_tab()
        ui.notify("Dataset cleared", type="positive")

    async def _delete_table(table_name: str) -> None:
        """Drop a single table from DuckDB and update state."""
        def _drop() -> None:
            conn = duckdb.connect(str(DUCKDB_PATH))
            conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            conn.close()

        try:
            await asyncio.to_thread(_drop)
        except Exception as exc:
            ui.notify(f"Could not delete '{table_name}': {exc}", type="negative")
            return

        # Remove from state
        state.sanity_warnings.pop(table_name, None)
        if table_name in state.table_names:
            idx = state.table_names.index(table_name)
            state.table_names.pop(idx)
            if idx < len(state.uploaded_filenames):
                state.uploaded_filenames.pop(idx)

        if state.active_table == table_name:
            state.active_table = state.table_names[0] if state.table_names else ""

        if not state.table_names:
            # No files left - back to upload screen
            state.dataset_info = {}
            state.column_stats = []
            state.ai_suggestions = []
            _reset_clarification_state()
            chat_messages.clear()
            file_switcher.set_options([])
            upload_zone.set_visibility(True)
            chat_messages.set_visibility(False)
            input_footer.value = False
            file_switcher_row.set_visibility(False)
            tabs.set_value(tab_chat)
        else:
            state.dataset_info = await asyncio.to_thread(
                get_dataset_info, DUCKDB_PATH, state.table_names
            )
            state.column_stats = await asyncio.to_thread(
                compute_column_stats, DUCKDB_PATH, state.active_table
            )

        _rebuild_dataset_tab()
        _rebuild_suggest_tab()
        ui.notify(f"'{table_name}' removed", type="positive")

    def _rebuild_dataset_tab() -> None:
        dataset_container.clear()
        if not state.table_names or state.db_path is None:
            with dataset_container:
                ui.label("No dataset loaded yet.").classes("text-gray-600 dark:text-gray-400")
            return
        tables_info = state.dataset_info.get("tables", [])
        with dataset_container:
            total_rows = sum(t["rows"] for t in tables_info)
            with ui.row().classes("gap-6 mb-2"):
                ui.label(f"Total Rows: {total_rows:,}").classes("text-sm font-semibold")
                ui.label(f"Tables: {len(tables_info)}").classes("text-sm font-semibold")
                ui.label(
                    f"Total Columns: {sum(t['columns'] for t in tables_info)}"
                ).classes("text-sm font-semibold")
            for i, tinfo in enumerate(tables_info):
                tname = tinfo["name"]
                with ui.card().classes("w-full").props("flat bordered"):
                    with ui.row().classes("w-full items-center justify-between px-2 pt-2 pb-1"):
                        with ui.column().classes("gap-0"):
                            ui.label(tname).classes("text-sm font-semibold")
                            ui.label(
                                f"{tinfo['rows']:,} rows - {tinfo['columns']} columns"
                            ).classes("text-xs text-gray-500")
                        with ui.row().classes("items-center gap-1"):
                            table_warnings_pre = state.sanity_warnings.get(tname, [])
                            if table_warnings_pre:
                                with ui.icon("warning", size="sm").classes("text-yellow-400 cursor-help"):
                                    ui.tooltip(f"{len(table_warnings_pre)} data quality notice(s)")
                            else:
                                with ui.icon("check_circle", size="sm").classes("text-green-500"):
                                    ui.tooltip("No data quality issues")
                            with ui.button(icon="delete", on_click=lambda t=tname: _delete_table(t)).props("flat dense color=red-4"):
                                ui.tooltip(f"Remove '{tname}'")
                    table_warnings = state.sanity_warnings.get(tname, [])
                    with ui.expansion("Schema", value=(i == 0)).classes("w-full text-xs text-gray-700 dark:text-gray-400"):
                        col_warnings_map: dict = {}
                        table_level_msgs: list = []
                        for w in table_warnings:
                            col = w.get("column")
                            if col:
                                col_warnings_map.setdefault(col, []).append(w["message"])
                            else:
                                table_level_msgs.append(w["message"])
                        try:
                            conn = duckdb.connect(str(state.db_path))
                            try:
                                schema_rows = conn.execute(
                                    "SELECT column_name, data_type "
                                    "FROM information_schema.columns "
                                    "WHERE table_schema = 'main' AND table_name = ? "
                                    "ORDER BY ordinal_position",
                                    [tname],
                                ).fetchall()
                            finally:
                                conn.close()
                            stats_map = (
                                {s["name"]: s for s in state.column_stats}
                                if tname == state.active_table and state.column_stats
                                else {}
                            )
                            schema_records = []
                            for col_name, dtype in schema_rows:
                                s = stats_map.get(col_name, {})
                                col_type = s.get("type", "text")
                                if col_type == "number":
                                    detail = f"min {_fmt_stat(s['min'])} / max {_fmt_stat(s['max'])} / avg {_fmt_stat(s['avg'])}"
                                elif col_type == "date":
                                    detail = f"{s['min']} to {s['max']}"
                                else:
                                    detail = dtype
                                schema_records.append({
                                    "column": col_name,
                                    "type": col_type if s else dtype,
                                    "detail": detail,
                                })
                            with ui.element("div").classes("w-full"):
                                with ui.row().classes(
                                    "w-full px-3 py-1 text-gray-500 font-semibold border-b border-gray-700"
                                ):
                                    ui.label("Column").classes("flex-1 min-w-0")
                                    ui.label("Type").classes("w-20 shrink-0")
                                    ui.label("Stats / Type").classes("flex-1 min-w-0")
                                    ui.label("").classes("w-6 shrink-0")
                                for msg in table_level_msgs:
                                    with ui.row().classes(
                                        "w-full px-3 py-1 items-center border-b border-gray-800"
                                    ):
                                        ui.label("(table)").classes("flex-1 italic text-gray-500")
                                        ui.label("").classes("w-20 shrink-0")
                                        ui.label("").classes("flex-1")
                                        with ui.icon("warning", size="xs").classes(
                                            "text-yellow-400 w-6 shrink-0 cursor-help"
                                        ):
                                            ui.tooltip(msg)
                                for record in schema_records:
                                    msgs = col_warnings_map.get(record["column"], [])
                                    with ui.row().classes(
                                        "w-full px-3 py-1 items-center border-b border-gray-800"
                                    ):
                                        ui.label(record["column"]).classes("flex-1 min-w-0 truncate")
                                        ui.label(record["type"]).classes("w-20 shrink-0 text-gray-600 dark:text-gray-400")
                                        ui.label(record["detail"]).classes(
                                            "flex-1 min-w-0 truncate text-gray-600 dark:text-gray-400"
                                        )
                                        if msgs:
                                            with ui.icon("warning", size="xs").classes(
                                                "text-yellow-400 w-6 shrink-0 cursor-help"
                                            ):
                                                ui.tooltip(" | ".join(msgs))
                                        else:
                                            ui.icon("check_circle", size="xs").classes(
                                                "text-green-500 w-6 shrink-0"
                                            )
                        except Exception as exc:
                            ui.label(f"Schema error: {exc}").classes("text-red-400 text-xs")
                    with ui.expansion("Preview data").classes("w-full text-xs text-gray-700 dark:text-gray-400"):
                        try:
                            conn = duckdb.connect(str(state.db_path))
                            try:
                                df_p = conn.execute(
                                    f'SELECT * FROM "{tname}" LIMIT 100'
                                ).df()
                            finally:
                                conn.close()
                            cols = [
                                {"name": c, "label": c, "field": c} for c in df_p.columns
                            ]
                            ui.table(
                                columns=cols, rows=df_p.to_dict("records")
                            ).classes("w-full text-xs").props("dense flat")
                        except Exception as exc:
                            ui.label(f"Preview error: {exc}").classes("text-red-400 text-xs")

    # ── Exports ──────────────────────────────────────────────────────────────

    async def _download_pdf(df: pd.DataFrame, msg_id: int) -> None:
        try:
            pdf_bytes = await asyncio.to_thread(
                dataframe_to_pdf_bytes, df, "DataVault AI Result"
            )
            ui.download(pdf_bytes, filename=f"result_{msg_id}.pdf")
        except Exception as exc:
            ui.notify(f"PDF export failed: {exc}", type="negative")

    async def _download_excel(df: pd.DataFrame, msg_id: int) -> None:
        try:
            excel_bytes = await asyncio.to_thread(dataframe_to_excel_bytes, df)
            ui.download(excel_bytes, filename=f"result_{msg_id}.xlsx")
        except Exception as exc:
            ui.notify(f"Excel export failed: {exc}", type="negative")

    # =========================================================================
    # UI construction  (handlers already defined above)
    # =========================================================================

    # ── Left drawer (built first so header can reference it) ─────────────────
    drawer = ui.left_drawer(value=True, fixed=True, bordered=True).classes(
        "bg-gray-800 text-white flex flex-col py-2"
    )
    with drawer:
        ui.button("+ New Chat", on_click=_new_chat).props("no-caps").classes(
            "mx-2 mb-2 bg-indigo-600 text-white text-sm w-11/12"
        )
        ui.separator().classes("bg-gray-600 mb-1")
        sidebar_chats = ui.column().classes("w-full gap-0 overflow-y-auto flex-1 px-1")

    # ── Header ───────────────────────────────────────────────────────────────
    with ui.header().classes(
        "flex-col bg-gray-900 text-white shadow-lg p-0"
    ):
        with ui.row().classes("items-center justify-between w-full px-4 py-2"):
            with ui.row().classes("items-center gap-2"):
                ui.button("|||", on_click=drawer.toggle).props(
                    "flat dense color=white"
                ).classes("font-mono text-lg px-2").tooltip("Toggle sidebar")
                ui.label("DataVault AI").classes("text-xl font-bold tracking-wide")
            with ui.row().classes("items-center gap-1"):
                ui.button(icon="refresh", on_click=_refresh_data).props(
                    "flat dense color=white round"
                ).tooltip("Refresh dataset and suggestions")
                ui.button(icon="upload_file", on_click=_show_upload).props(
                    "flat dense color=white round"
                ).tooltip("Upload new file")
                ui.button(icon="dark_mode", on_click=dark.toggle).props(
                    "flat dense color=white round"
                ).tooltip("Toggle light / dark theme")
                ui.button("Clear Chat", on_click=_clear_chat).props("flat dense color=white")
        with ui.tabs().classes(
            "bg-gray-900 border-t border-gray-700 px-2 w-full"
        ) as tabs:
            tab_chat    = ui.tab("Chat")
            tab_suggest = ui.tab("Suggestions")
            tab_dataset = ui.tab("Dataset")

    # ── Footer (pinned input bar) ─────────────────────────────────────────────
    with ui.footer(value=False).classes(
        "bg-gray-900 border-t border-gray-700 px-4 pt-3 pb-2"
    ) as input_footer:
        # Row 1 - main input + send button
        with ui.row().classes("w-full items-center gap-2"):
            chat_input = ui.input(
                placeholder="Ask a question about your data..."
            ).classes("flex-1").props("outlined dense dark")
            chat_input.on("keydown.enter", _handle_send)
            ui.button("Send", on_click=_handle_send).props(
                "color=indigo unelevated no-caps"
            )

        # Row 2 - follow-up toggle with hint label
        with ui.row().classes("w-full items-center gap-2 mt-1"):
            ui.switch("Follow-up mode").bind_value(
                state, "follow_up_enabled"
            ).classes("text-gray-300 text-xs").props("dense")
            ui.label("Builds on the previous query result").classes(
                "text-xs text-gray-500 italic"
            )

        # Row 3 - file switcher at bottom so dropdown opens upward (not over input)
        file_switcher_row = ui.row().classes("w-full items-center gap-2 mt-2")
        file_switcher_row.set_visibility(False)
        with file_switcher_row:
            ui.label("Active file:").classes("text-xs text-gray-600 dark:text-gray-400 shrink-0")
            file_switcher = ui.select(
                options=[], on_change=_on_file_switch
            ).classes("flex-1 text-sm").props("dense outlined dark")

    # ── Main content ──────────────────────────────────────────────────────────
    with ui.tab_panels(tabs, value=tab_chat).classes("w-full"):
        # Chat tab
        with ui.tab_panel(tab_chat).classes("p-0"):
            upload_zone = ui.column().classes(
                "w-full min-h-96 items-center justify-center gap-6 p-8"
            )
            with upload_zone:
                ui.label("DataVault AI").classes(
                    "text-4xl font-bold text-gray-700 dark:text-gray-400"
                )
                ui.label(
                    "Upload a CSV or Excel file to start querying your data"
                ).classes("text-gray-600 dark:text-gray-500 text-base text-center")
                uploader = ui.upload(
                    label="Choose file",
                    on_upload=_handle_upload,
                    auto_upload=True,
                    multiple=True,
                ).props('accept=".csv,.xlsx,.xls" flat bordered').classes("w-96")
                with ui.row().classes("items-center gap-3 mt-2"):
                    ui.label("Already have data loaded?").classes("text-gray-600 dark:text-gray-500 text-sm")
                    ui.button("+ New Chat", on_click=_new_chat).props("no-caps flat").classes(
                        "bg-indigo-600 text-white text-sm px-4"
                    )

            chat_messages = ui.column().classes("w-full gap-3 p-4 pb-32")
            chat_messages.set_visibility(False)

        # Suggestions tab
        with ui.tab_panel(tab_suggest).classes("p-4"):
            suggest_container = ui.column().classes("w-full gap-4 pb-16")
            with suggest_container:
                ui.label(
                    "Upload a file first to see AI suggestions."
                ).classes("text-gray-600 dark:text-gray-400")

        # Dataset tab
        with ui.tab_panel(tab_dataset).classes("p-4"):
            ui.label("Loaded datasets").classes("text-sm font-semibold text-gray-600 dark:text-gray-400 uppercase tracking-wide mb-3")
            dataset_container = ui.column().classes("w-full gap-4 pb-16")
            with dataset_container:
                ui.label("No dataset loaded yet.").classes("text-gray-600 dark:text-gray-400")

    # ── Initial sidebar populate ───────────────────────────────────────────
    _refresh_sidebar()

    # Auto-restore dataset state when DB already has tables (e.g. browser reload)
    ui.timer(0.3, _refresh_data, once=True)


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

try:
    ui.run(
        title="DataVault AI",
        port=8080,
        reload=False,
        show=True,
        dark=True,
    )
except KeyboardInterrupt:
    pass
