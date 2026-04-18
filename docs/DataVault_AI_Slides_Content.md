# DataVault AI - Presentation Slide Content

---

## Slide 1 - Title

**Headline:** DataVault AI
**Tagline:** Query Your Data. No SQL Required.
**Sub-line:** Local AI Analytics Platform | April 2026

---

## Slide 2 - The Problem

**Title:** The Problem

**Card 1 - Data Trapped in Spreadsheets**
Teams spend hours manually filtering, pivoting, and copy-pasting across CSV and Excel files just to answer basic business questions.

**Card 2 - SQL Expertise Required**
Extracting meaningful insights from structured data requires SQL knowledge that most business users and analysts simply do not have.

**Card 3 - Cloud Upload Risks**
Sending sensitive business data to cloud-based AI tools creates compliance risks, data privacy concerns, and loss of control over your own information.

---

## Slide 3 - Our Solution

**Title:** Our Solution

**Step 1 - Upload Your File**
Drag and drop any CSV or Excel file. DataVault AI ingests, cleans, and prepares it instantly.

**Step 2 - Ask in Plain English**
Type your question naturally - "Show me top 5 customers by revenue last quarter" - no SQL needed.

**Step 3 - Get Instant Insights**
Receive a narrated answer, a formatted data table, and one-click PDF or Excel export.

---

## Slide 4 - How It Works

**Title:** How It Works

1. Upload a CSV or Excel file - auto-cleaned and loaded into a local database
2. Automated quality checks run instantly - flagging nulls, duplicates, and data issues
3. Type your question in plain English in the chat interface
4. AI asks clarifying questions if your query is ambiguous - before running anything
5. SQL is generated, validated, and executed against your local data
6. Results are narrated in structured markdown - with PDF and Excel export in one click

---

## Slide 5 - Key Features

**Title:** Key Features

**Natural Language Queries**
Ask any question in plain English. No SQL, no formulas, no technical knowledge required.

**Smart Clarification**
Before running a query, the AI asks targeted questions if your request is ambiguous - preventing meaningless results.

**Data Quality Checks**
11 automated rules run at upload to flag nulls, duplicates, formatting issues, and bad data before you query.

**Follow-up Queries**
Toggle follow-up mode to scope your next question to the previous result - drill down without re-running from scratch.

**PDF and Excel Export**
One-click export of any result - formatted landscape PDF or styled .xlsx with headers and auto-fitted columns.

**Chat History**
Every session is saved locally. Return to any past conversation, reload results, and continue where you left off.

---

## Slide 6 - AI Pipeline

**Title:** AI Pipeline

**Flow (left to right):**

Schema Load  ->  SQL Generation (DeepSeek V3)  ->  Validation  ->  Execution  ->  Narration + Grounding

**Schema Load**
Fetches table structure (column names and types) from the local database - no row data is sent to the AI.

**SQL Generation**
Primary model: DeepSeek V3 (temperature 0.0 - fully deterministic). Generates precise DuckDB SQL from your question.

**Validation**
Blocks any destructive keywords (INSERT, UPDATE, DELETE, DROP) before execution. Read-only guaranteed.

**Execution**
Runs the SELECT query against your local DuckDB file. Retries once automatically if the first attempt fails.

**Narration + Grounding**
Converts the raw result into structured markdown. A grounding check then verifies every number in the narration against the raw data - flags any mismatch.

**Note:** Full 11-node pipeline internally: fast_path, follow_up_detector, schema_loader, table_selector, cte_builder, sql_generator, validator, executor, empty_check, narrator, grounding.

---

## Slide 7 - Technology Stack

**Title:** Technology Stack

**UI Layer**
NiceGUI (>=1.4.0) - web-based desktop UI, file upload, chat interface, tabs, sidebar | Python 3.10+

**AI Layer**
LangGraph (>=1.0.0) - 11-node pipeline orchestration | OpenRouter API - routes to multiple LLM providers | DeepSeek V3 - primary model (SQL + narration) | httpx - raw HTTP client (no SDK)

**Data Layer**
DuckDB (>=0.10.0) - in-process analytical SQL engine | pandas (>=2.0.0) - data ingestion and transformation | SQLite (stdlib) - local chat history persistence

**Export Layer**
fpdf2 (>=2.7.0) - landscape A4 PDF generation | openpyxl (>=3.1.0) - styled Excel (.xlsx) export

---

## Slide 8 - Data Quality Intelligence

**Title:** Data Quality Intelligence
**Subtitle:** 11 automated sanity checks run on every file upload - before you ask a single question.

**Column 1**
1. Empty table - zero rows detected (error, upload still completes)
2. Very small dataset - fewer than 5 rows (results may not be meaningful)
3. Duplicate rows - more than 5% identical rows (aggregations may be inflated)
4. High null rate - column is over 80% empty (not useful for filtering or grouping)
5. Near-constant column - only 1 unique value across all rows
6. All-unique text column - likely an ID field, GROUP BY won't be meaningful

**Column 2**
7. Null-like placeholders - values such as N/A, none, NULL, TBD stored as real text
8. Numeric text as VARCHAR - values like $1,200 or 1.5k stored as text (SUM/AVG won't work)
9. Unparseable temporal column - column name suggests a date but values could not be parsed
10. Future dates - max date is beyond today, may indicate data entry errors
11. Negative values - amount or quantity columns contain unexpected negative numbers

---

## Slide 9 - Privacy and Security

**Title:** Privacy and Security

**Headline:** 100% Local Processing

Your data never leaves your machine. DataVault AI runs entirely on your local environment - no cloud, no third-party storage, no exposure.

- All uploaded files are stored in a single local DuckDB file on your machine
- Chat history and query results are saved in a local SQLite database only
- No file contents, results, or metadata are sent to any external server
- Only the question text and table schema (column names and types) are sent to the AI model - never your actual data rows

**Footer:** Privacy-first by design. Compliant by default.

---

## Slide 10 - Multi-Model AI Reliability

**Title:** Multi-Model AI Reliability

**Fallback Chain:**

Primary: DeepSeek V3 (deepseek/deepseek-chat)
  -> Fallback 1: Claude 3.5 Haiku (anthropic/claude-3.5-haiku)
    -> Fallback 2: GPT-4o Mini (openai/gpt-4o-mini)

If the primary model fails or times out, the next model is tried automatically - no manual intervention, no error shown to the user unless all three fail.

**Technical details:**
- SQL generation: temperature 0.0 (fully deterministic output)
- Narration: temperature 0.3 (natural, readable prose)
- Timeout per call: 60 seconds
- Provider: OpenRouter (single API key, access to all three models)

---

## Slide 11 - Export and Integration

**Title:** Export and Integration

**PDF Export**
- Landscape A4 format (optimized for wide data tables)
- Auto-fitted column widths
- Alternating row shading for readability
- Bold formatted header row
- Instant browser download - no save dialog needed

**Excel Export (.xlsx)**
- Full openpyxl styling
- Bold header row with auto-fit column widths (capped at 50 characters)
- Frozen top row for easy scrolling
- Filter-ready columns out of the box
- Instant browser download

Both exports run in a background thread - the UI stays fully responsive during generation.

---

## Slide 12 - Why DataVault AI

**Title:** Why DataVault AI

**Speed**
Answers in seconds. No pivot tables, no VLOOKUP chains, no waiting for a data analyst to get back to you.

**No SQL Required**
Ask in plain English. Every member of your team - sales, finance, operations - can query your data directly.

**Privacy-First**
100% local. Your data stays on your machine. Zero cloud risk. Zero compliance exposure. Zero data handed to a third party.

**Cost-Efficient**
DeepSeek V3 costs approximately $0.27 per 1 million input tokens. A typical query costs a fraction of a cent - orders of magnitude cheaper than enterprise BI tools.

---

## Slide 13 - Getting Started

**Title:** Getting Started

**Step 1 - Add your API key**
Add your OpenRouter API key to the `.env` file in the project root:
`OPENROUTER_API_KEY=your_key_here`

**Step 2 - Run setup**
Double-click `setup.bat` - it creates a virtual environment, installs all dependencies, and runs the smoke tests automatically.

**Step 3 - Launch the app**
Double-click `start_DataVault_AI.bat` - the app opens in your default browser. Upload a CSV or Excel file and start asking questions.

---
DataVault AI - Local AI Analytics Platform
