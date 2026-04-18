# DataVault AI - Feature Documentation

A NiceGUI desktop app that lets users query CSV/Excel files in plain English.
Natural language questions go through a LangGraph pipeline powered by OpenRouter.
Chat history persists across restarts via SQLite.

---

## Table of Contents

1. [File Upload & Ingestion](#1-file-upload--ingestion)
2. [Upload Data Sanity Check](#2-upload-data-sanity-check)
3. [Natural Language Query Pipeline](#3-natural-language-query-pipeline)
4. [Multi-Turn Clarification Before SQL](#4-multi-turn-clarification-before-sql)
5. [Follow-Up Queries](#5-follow-up-queries)
6. [Result Display](#6-result-display)
7. [Export](#7-export)
8. [Chat History](#8-chat-history)
9. [Dataset Tab](#9-dataset-tab)
10. [Suggestions Tab](#10-suggestions-tab)
11. [Multi-File Support](#11-multi-file-support)
12. [LLM & API](#12-llm--api)
13. [Database Maintenance](#13-database-maintenance)
14. [Data Privacy - Synthetic Sample Rows](#14-data-privacy---synthetic-sample-rows)
15. [LLM Request Logs](#15-llm-request-logs)

---

## 1. File Upload & Ingestion

**What it does:** Accepts CSV and Excel files and loads them into a shared DuckDB database for querying.

**Details:**
- Supports `.csv`, `.xlsx`, `.xls` file formats
- Detects multi-sheet Excel files and blocks the upload, listing all sheet names so the user knows which sheet to export first
- Column names are automatically sanitized: spaces and slashes replaced with underscores, all lowercased - prevents SQL quoting issues
- Date-like columns (names containing `date`, `time`, `_at`, `created`, `updated`, `timestamp`) are auto-converted to ISO `YYYY-MM-DD` format so DuckDB `STRFTIME` works reliably
  - Guard: conversion is only applied if >50% of values parse successfully AND >80% of parsed years are >= 1900, preventing small integers (e.g. day numbers 1-31) from being silently mangled into year-0015 dates
- Object-type columns are attempted as numeric via `pd.to_numeric(errors="ignore")`
- All tables land in a single shared DuckDB file (`data/datavault.duckdb`) - prevents lock errors from concurrent async connections
- Upload errors show an informative toast with the exact reason; uploader widget is reset so the user can retry

---

## 2. Upload Data Sanity Check

**What it does:** Runs automatic rule-based data quality checks immediately after every upload and surfaces any issues before the user starts querying.

**Checks performed:**

| Check | Scope | Trigger | Level |
|---|---|---|---|
| Empty table | Table | 0 rows | Error |
| Too few rows | Table | Fewer than 5 rows | Warning |
| Duplicate rows | Table | >5% of rows are exact duplicates | Warning |
| High null rate | Column | Column >80% null | Warning |
| Near-constant column | Column | Only 1 unique value | Warning |
| All-unique text column | Column | Every value is unique (likely an ID column) | Warning |
| Null-like placeholder strings | Column | Values such as N/A, none, NULL, -, TBD exceed 1% of rows | Warning |
| Formatted numeric text | Column | >80% of a text column's sampled values match numeric patterns ($, commas, k/M/%) | Warning |
| Unparseable date column | Column | Temporal column name but values could not be parsed as dates with year >= 1900 | Warning |
| Future dates | Column | Max date in a temporal column is beyond today | Warning |
| Negative values in amount columns | Column | qty / price / revenue / cost / total / count column has min < 0 | Warning |

**Behavior:**
- Upload always completes regardless of warnings - the check never blocks the ingest
- If warnings are found: amber toast notification directs user to the Dataset tab; yellow warning card appears inside the table card in the Dataset tab listing each issue with the affected column name
- If no warnings: standard green "loaded successfully" toast
- Warnings are cleared automatically when a table is deleted or the dataset is fully cleared
- All checks fail open - any internal error is silently skipped so the upload is never disrupted

---

## 3. Natural Language Query Pipeline

**What it does:** Converts a plain English question into a DuckDB SQL query, executes it, and narrates the result in structured markdown. Implemented as a LangGraph StateGraph.

**Pipeline nodes (in execution order):**

| Node | Role |
|---|---|
| `fast_path` | Detects exact prompt snippet matches and dispatches directly to pandas - no LLM call, no SQL |
| `follow_up_detector` | Reads the `force_follow_up` flag from the UI toggle; routes to CTE path or fresh query |
| `schema_loader` | Reads DDL and 3 synthetic sample rows from DuckDB; filters to the active table only |
| `table_selector` | Single-table fast-path when only one table exists after schema load |
| `cte_builder` | Wraps previous SQL as `WITH prev_result AS (...)` for follow-up queries |
| `sql_generator` | LLM call → DuckDB SQL; post-processes backticks to double-quotes, fixes `MIN "col"` → `MIN("col")` |
| `validator` | Blocks destructive keywords: `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER` |
| `executor` | Runs `SELECT` → DataFrame; retries once on error |
| `empty_check` | If zero rows returned, retries once with a broader prompt before giving up |
| `narrator` | DataFrame → structured markdown via a second LLM call |
| `grounding` | Verifies that numbers in the narration actually appear in the raw DataFrame |

**SQL generation rules enforced:**
- Single-table mode only - the LLM receives DDL for exactly one table and is instructed never to JOIN
- Percentage rule: use the same `AVG`/`COUNT` expression multiplied by 100, never mix aggregation levels
- STRFTIME granularity matched to the question: year → `%Y`, month → `%Y-%m`, day → `%Y-%m-%d`
- Temperature 0.0 for deterministic SQL

**Narration length tiers:**
- 1 row, 1 column: one bold sentence
- 2-8 rows: bullet per row
- 9-30 rows: top 3 / bottom 3 flat sections, max 600 tokens
- 30+ rows: 3-5 flat insight bullets only, max 350 tokens - no nested lists, no data enumeration

**Result object:** `QueryResult` dataclass - never raises. Always check `result.error` first.

---

## 4. Multi-Turn Clarification Before SQL

**What it does:** Before writing any SQL for a fresh question, evaluates whether the question is too vague to produce reliable results. If vague, asks the user targeted clarifying questions in chat (up to 5 rounds) until the intent is unambiguous, then runs SQL with the enriched question.

**Ambiguity dimensions checked:**

| Dimension | Example of vague signal |
|---|---|
| Time scope | "recently", "last period", "over time" |
| Thresholds / filters | "high value", "slow movers", "top customers" |
| Granularity | "by region" (which level?), "per product" (SKU or category?) |
| Metric definition | "performance", "activity", "spend" |
| Scope / population | "active", "relevant", "our customers" |
| Comparison baseline | "more than usual", "worse than expected" |
| Output shape | "show me", "analyse", "summarise" |

**Decision rule:** If 2 or more dimensions are ambiguous, ask clarifying questions. The LLM picks the single most structurally impactful question each turn.

**Flow:**
1. Fresh question → ambiguity LLM call (temp=0, max 150 tokens)
2. If vague → blue "Need a bit more detail" card appears in chat; clarification state is activated
3. User replies in the same chat input → clarification LLM evaluates if intent is now clear
4. If still unclear AND fewer than 5 turns → asks the next question
5. If clear OR 5 turns reached → merges original question + all user answers into an enriched question → runs the full pipeline

**Bypass conditions (no clarification check):**
- Follow-up mode is ON (context already known from prior SQL)
- Fast-path question (exact snippet match)
- No active table or no DDL available

**Reliability:**
- All clarification messages are saved to chat history
- State is reset on new chat, file switch, load chat, and clear chat - stale loops never carry over
- Fails open: any LLM or parse error → SQL runs immediately as if the question were clear

---

## 5. Follow-Up Queries

**What it does:** Allows the user to ask a follow-up question that builds on the previous query's result set, without re-explaining the full context.

**Mechanism:** Previous SQL is wrapped as a named CTE: `WITH prev_result AS (...)`. The LLM writes `SELECT ... FROM prev_result` for the follow-up.

**Activation:** Toggle "Follow-up mode" switch in the input bar. When ON and a previous result exists, the pipeline automatically receives the prior SQL, narration, result shape, and column list as context.

**Guardrails:**
- Falls back to a fresh query if previous SQL is empty, was a pandas computation, was a single-row aggregate, or the CTE build fails
- Follow-up depth capped at 3 (tracked per session)
- Switching files or starting a new chat resets follow-up context

---

## 6. Result Display

**What it does:** Renders each query result as a structured card in the chat thread.

**Card sections:**
- **Narration:** LLM-generated structured markdown with Tailwind-styled headings, bullets, and bold text
- **Grounding warning:** Yellow banner if any number in the narration was not found verbatim in the raw data - alerts the user to potential hallucination
- **Data table:** First 200 rows of the result DataFrame displayed as a sortable NiceGUI table, with a row/column count label
- **Action bar:** PDF export button, Excel export button, SQL expansion panel ("View SQL" or "pandas" label), follow-up depth chip (indigo badge showing depth 1-3)
- **Error card:** Red card with error icon if the pipeline returns an error

**Spinner:** "Thinking..." row with spinner shown while the pipeline runs, removed when the result arrives.

---

## 7. Export

**What it does:** Downloads the result DataFrame as a formatted file directly from the browser.

**PDF export:**
- Landscape A4, generated with `fpdf2`
- Includes a title header and all result columns/rows
- Triggered per result card - each result can be exported independently

**Excel export:**
- Generated with `openpyxl`
- Single worksheet with auto-fitted columns
- Triggered per result card

Both exports run in `asyncio.to_thread` to avoid blocking the NiceGUI event loop.

---

## 8. Chat History

**What it does:** Persists all conversations (questions, SQL, results, narrations) to a local SQLite database so sessions survive app restarts.

**Storage details:**
- Database: `data/chat_history.db`
- Schema: `chats` table (id, title, filename, created_at, updated_at) + `messages` table (role, content, sql_text, df_json, grounding_json, retried, error, is_follow_up, follow_up_depth, created_at)
- DataFrame stored capped at 500 rows to prevent multi-hundred-MB SQLite files; full data always re-queryable from DuckDB

**Sidebar features:**
- Lists all past chats with creation date and time
- Click any chat to restore the full conversation thread including all result cards, SQL panels, and export buttons
- X button to delete individual chats
- SQLite `VACUUM` runs at startup and after each delete to reclaim space

**Session lifecycle:**
- New Chat: clears messages, resets all session state including follow-up and clarification state
- Load Chat: restores messages, resets session state so no stale follow-up or clarification loops carry over
- Clear Chat: wipes messages from the current view

---

## 9. Dataset Tab

**What it does:** Provides a full-dataset overview for each uploaded table.

**Per-table card contains:**

**Data quality notices (yellow banner)** - shown if the sanity check found issues with this table; lists each affected column and the specific problem.

**Schema expansion** - collapsible panel showing:
- Column name
- Data type
- For the active table: min/max/avg for numeric columns, date range for date columns
- For other tables: raw DuckDB data type string

**Preview data expansion** - collapsible panel showing the first 100 rows of the table as a dense NiceGUI table.

**Table header** - shows table name, row count, column count, and a delete button to drop just this table from DuckDB.

**Summary row** - total rows, total tables, total columns across all loaded tables.

---

## 10. Suggestions Tab

**What it does:** Provides ready-made questions the user can click to send, eliminating the blank-page problem when starting with a new dataset.

**AI Suggestions:** Three LLM-generated questions at increasing complexity levels (Beginner / Intermediate / Advanced), tailored to the actual schema, column types, and sample data of the active table. Generated in the background after upload - does not block the UI.

**Prompt Snippet Library:** Curated collections of reusable questions organised into groups:
- **Data Profile:** Numeric summaries, unique value counts, data types, random sample rows, spread analysis
- **Quality Check:** Missing value rates, duplicate rows, near-constant columns, zero-value rows, high-null columns
- **Business Insights:** Top category by total, outlier rows (>2x column average), rank by average, percentage contribution, top 5 / bottom 5

Clicking any suggestion or snippet sends it directly to the chat input and triggers the query pipeline.

---

## 11. Multi-File Support

**What it does:** Allows multiple CSV/Excel files to be uploaded in the same session, each stored as a separate table in the shared DuckDB file.

**File switcher:** Dropdown in the input bar switches the active table. Switching resets follow-up context, clarification state, and query history for the new table - SQL is never cross-contaminated between tables.

**Single-table mode enforced:** The pipeline always queries exactly one table (the active table). The schema loader filters to the active table only, and the SQL generator is instructed never to JOIN.

---

## 12. LLM & API

**What it does:** All LLM calls go through OpenRouter via a raw `httpx`/`requests` HTTP client - no `openai` SDK.

**Models:**
- `SQL_MODEL`: `deepseek/deepseek-chat` (DeepSeek V3), temperature 0.0
- `NARRATION_MODEL`: `deepseek/deepseek-chat`, temperature 0.3
- `CLARIFICATION_MODEL`: same as SQL_MODEL, temperature 0.0

**Fallback chain:** If the primary model fails (HTTP error or timeout), the client automatically retries with:
1. `anthropic/claude-3.5-haiku`
2. `openai/gpt-4o-mini`

Only raises `RuntimeError` if all three fail.

**Configuration (`.env` file only - never `setx`):**

| Key | Default | Notes |
|---|---|---|
| `OPENROUTER_API_KEY` | required | Must be in `.env` - `setx` causes 401 errors |
| `LLM_MAX_TOKENS` | 2048 | Per LLM call |
| `LLM_TEMPERATURE` | 0.0 | SQL model |
| `LLM_TIMEOUT_SECONDS` | 60 | Per-request HTTP timeout |

**Why not the `openai` package:** The `openai` package injects `OpenAI-Organization` and `OpenAI-Project` headers that cause 401 errors on certain OpenRouter account types. Raw `requests` sends exactly what is specified.

**Why `dotenv_values()` not `load_dotenv()`:** `load_dotenv()` silently skips keys already present in the Windows shell environment. If a stale `setx` variable exists, the wrong key would be used. `dotenv_values()` always reads directly from the `.env` file.

**Token usage tracking:** A module-level accumulator in `llm_client.py` adds up `prompt_tokens`, `completion_tokens`, and `total_tokens` from every `call_llm()` response in a single pipeline run. `reset_usage()` is called before each pipeline run; `get_usage()` reads the totals after. This lets the UI log per-request token spend without changing any call sites.

---

## 13. Database Maintenance

**What it does:** Keeps both databases lean without user intervention.

**SQLite (`chat_history.db`):**
- `VACUUM` runs at app startup to reclaim space from previously deleted chats
- `VACUUM` runs again after each individual chat delete
- DataFrames stored capped at 500 rows - prevents JSON blobs from bloating the DB

**DuckDB (`datavault.duckdb`):**
- Single shared file for all tables - never recreated per upload
- Individual tables can be dropped from the Dataset tab without affecting other tables
- Full dataset clear drops all tables and resets to the upload screen

---

---

## 14. Data Privacy - Synthetic Sample Rows

**What it does:** Replaces all real cell values in the sample rows sent to the LLM with Faker-generated synthetic equivalents, so actual data never leaves the machine.

**Why it is needed:** Every LLM call for SQL generation includes 3 sample rows from the uploaded table as context. Without anonymization, real names, emails, amounts, and dates are transmitted to OpenRouter and potentially logged by the model provider.

**How it works:**
- `synthesize_df(df)` in `core/sql_executor.py` iterates over every column, reads the pandas dtype, and generates a realistic fake value using column-name heuristics:

| Column dtype | Column name hint | Generated value example |
|---|---|---|
| string | `email` | `jsmith@example.com` |
| string | `name` | `Kelly Gates` |
| string | `city` | `Austin` |
| string | `status`, `type` | `active` / `pending` |
| string | `company` | `Acme Corp` |
| integer | `id`, `code` | `27005` |
| integer | `age` | `34` |
| float | `rate`, `pct` | `61.42` |
| float | `revenue`, `amount` | `9501.60` |
| datetime | any | `2024-07-15` |
| boolean | any | `True` |

- Column names and types (DDL) are always sent as-is - the LLM needs them for SQL generation.
- Applied in two places: `schema_loader_node` (3 rows for SQL generation) and `generate_suggestions_llm` in `app.py` (5 rows for AI suggestion chips).
- Uses the `faker` library (v33+), installed in the project venv.

**What the LLM receives instead of real data:**

```
accident_id,city,state,latitude,longitude,date
27005,North Elizabethtown,Wyoming,60.22,-21.07,2024-03-12
99027,Lake Sandra,Kansas,-54.82,142.22,2023-11-08
34325,East Brian,Arkansas,46.23,-47.63,2024-07-01
```

The LLM still has full DDL with column names and types - enough to generate correct SQL.

---

## 15. LLM Request Logs

**What it does:** Records every successful LLM-backed query with token usage and the exact synthetic rows that were sent, visible in a dedicated tab and exportable as CSV.

**Storage:** `llm_logs` table in `data/chat_history.db` (same SQLite file as chat history).

**Schema:**

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-increment |
| `question` | TEXT | User question (truncated to 500 chars) |
| `model` | TEXT | Primary model used (e.g. `deepseek/deepseek-chat`) |
| `prompt_tokens` | INTEGER | Tokens in the prompt across all LLM calls for this request |
| `completion_tokens` | INTEGER | Tokens in the response |
| `total_tokens` | INTEGER | Sum of prompt + completion |
| `sample_csv` | TEXT | Synthetic CSV rows sent to the LLM for this request |
| `created_at` | TEXT | ISO UTC timestamp |

**Auto-prune:** After every insert, rows outside the most recent 150 are deleted:
```sql
DELETE FROM llm_logs WHERE id NOT IN (SELECT id FROM llm_logs ORDER BY id DESC LIMIT 150)
```

**LLM Logs tab (UI):**
- Shows one card per request, newest first
- Each card: timestamp, model badge, full question text, token counters (Prompt / Completion / Total) with colored numbers
- Expandable "Synthetic rows sent to AI" section renders the CSV as a proper table with column headers
- **Export CSV** button at the top downloads all 150 log entries as a flat CSV file
- Tab is rebuilt on every app startup and after each new request

**Migration:** Existing `chat_history.db` files from before this feature are automatically migrated - an `ALTER TABLE ADD COLUMN sample_csv` is attempted on startup and silently skipped if the column already exists.

---

## Quick-Start Commands

```bash
# First-time setup (creates .venv, installs deps, runs smoke tests)
setup.bat

# Start the app
start_chatbot.bat

# Run smoke tests (no API calls)
python -m pytest tests/smoke_test.py -v

# Diagnose API key issues
python test_key.py
```
