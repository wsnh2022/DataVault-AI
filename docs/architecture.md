# DataVault AI - Architecture

Six diagrams covering the full system: component overview, upload pipeline, query pipeline, clarification flow, data layer, and UI layout.

---

## 1. System Architecture Overview

How the major components connect at runtime.

```mermaid
flowchart TB
    subgraph APP["Desktop App  -  Single User, Local Machine"]
        subgraph UI["NiceGUI UI  (app.py)"]
            direction LR
            UPLOAD_ZONE["Upload Zone"]
            CHAT_THREAD["Chat Thread"]
            SIDEBAR["Chat Sidebar"]
            DATASET_TAB["Dataset Tab"]
            SUGGEST_TAB["Suggestions Tab"]
            INPUT_BAR["Input Bar + Follow-up Toggle"]
        end

        subgraph STATE["AppState  (module-level singleton)"]
            direction TB
            S1["db_path · table_names · active_table"]
            S2["current_chat_id · msg_counter"]
            S3["follow_up state · clarification state"]
            S4["column_stats · sanity_warnings · ai_suggestions"]
        end
    end

    subgraph CORE["Core Pipeline  (core/)"]
        direction TB
        QP["QueryPipeline\nquery_pipeline.py"]
        GRAPH["LangGraph StateGraph\ngraph_pipeline.py"]
        NODES["11 Pipeline Nodes\nnodes/"]
        LLM["LLM Client\nllm_client.py  -  raw httpx"]
        SQLEXEC["SqlExecutor\nsql_executor.py"]
        CHATSTORE["chat_store.py\nSQLite adapter"]
    end

    subgraph STORAGE["Local Storage"]
        DUCKDB[("DuckDB\ndatavault.duckdb\nAll uploaded tables live here")]
        SQLITE[("SQLite\nchat_history.db\nChats + Messages")]
    end

    subgraph OPENROUTER["OpenRouter  (HTTPS)"]
        direction TB
        M1["DeepSeek V3  -  primary"]
        M2["Claude 3.5 Haiku  -  fallback 1"]
        M3["GPT-4o Mini  -  fallback 2"]
        M1 -.->|on failure| M2
        M2 -.->|on failure| M3
    end

    UI -->|"question + active_table + follow_up context"| QP
    QP --> GRAPH
    GRAPH --> NODES
    NODES -->|"SQL generation / narration calls"| LLM
    NODES -->|"SELECT queries"| SQLEXEC
    SQLEXEC <-->|"duckdb.connect"| DUCKDB
    LLM -->|"POST /chat/completions"| M1
    GRAPH -->|"QueryResult dataclass"| UI
    UI -->|"save / load messages"| CHATSTORE
    CHATSTORE <-->|"sqlite3 WAL mode"| SQLITE
    UI -->|"ingest CSV / Excel bytes"| DUCKDB
```

---

## 2. File Upload and Ingest Pipeline

Every step from file drop to DuckDB table, including the new sanity check.

```mermaid
flowchart TD
    A(["User drops CSV or Excel file"]) --> B{Excel file?}

    B -->|Yes| C{Multiple sheets?}
    C -->|Yes| BLOCK[/"Block - show sheet names in error toast\nUser must export a single sheet first"/]
    C -->|No| PARSE_XL["Parse single sheet with pd.ExcelFile"]

    B -->|No| PARSE_CSV["Read with pd.read_csv"]

    PARSE_XL --> SANITIZE
    PARSE_CSV --> SANITIZE

    SANITIZE["Sanitize column names\nspaces + slashes → underscores\nall lowercased"]

    SANITIZE --> NORM["_normalize_date_columns\nFor each column matching date · time · _at · created · updated · timestamp"]

    NORM --> DATE_CHECK{"dtype == object\nAND name matches\ntemporal pattern?"}

    DATE_CHECK -->|Yes| PARSE_DT["pd.to_datetime\nerrors=coerce  dayfirst=True"]
    PARSE_DT --> YEAR_GUARD{">50% parse OK\nAND >80% of years >= 1900?"}
    YEAR_GUARD -->|Yes| CONVERT["Format as YYYY-MM-DD string"]
    YEAR_GUARD -->|"No - e.g. day numbers 1-31\nwould become year 0001-0031"| LEAVE["Leave column as-is"]
    CONVERT --> NUMERIC
    LEAVE --> NUMERIC
    DATE_CHECK -->|No| NUMERIC

    NUMERIC["pd.to_numeric coercion\non remaining object columns"]

    NUMERIC --> DUCKDB_WRITE["CREATE OR REPLACE TABLE in DuckDB\nvia duckdb.register + SELECT *"]

    DUCKDB_WRITE --> STATS["compute_column_stats\nmin / max / avg for numeric\ndate range for date\ntype label for text"]

    STATS --> SANITY["run_data_sanity_check\n5 rule-based DuckDB queries"]

    SANITY --> W1{"0 rows?"}
    W1 -->|Yes| ERR_ROWS[/"Error: Table has 0 rows"/]
    W1 -->|No| W2{"< 5 rows?"}
    W2 -->|Yes| WARN_FEW["Warning: Only N rows"]
    W2 -->|No| W3
    WARN_FEW --> W3

    W3{"Any column\n> 80% null?"}
    W3 -->|Yes| WARN_NULL["Warning: X% null - per column"]
    W3 --> W4
    WARN_NULL --> W4

    W4{"Any column\n1 unique value?"}
    W4 -->|Yes| WARN_CONST["Warning: Only 1 unique value - per column"]
    W4 --> W5
    WARN_CONST --> W5

    W5{"Temporal column name\nbut stored as VARCHAR?"}
    W5 -->|Yes| WARN_DATE["Warning: dates could not be parsed"]
    W5 --> RESULT
    WARN_DATE --> RESULT

    RESULT{"Any warnings\ncollected?"}
    RESULT -->|Yes| AMBER[/"Amber toast - N notices\nYellow card in Dataset tab"/]
    RESULT -->|No| GREEN[/"Green success toast"/]

    AMBER --> ACTIVATE
    GREEN --> ACTIVATE

    ACTIVATE["Activate chat view\nRebuild Dataset + Suggestions tabs"]
    ACTIVATE --> BG["Fire AI suggestions generation\nin background asyncio task"]
```

---

## 3. LangGraph Query Pipeline

The 11-node StateGraph that processes every natural language question into a narrated result.

```mermaid
flowchart TD
    START(["_handle_send - question + context"]) --> FP

    FP["fast_path\nLook up question in prompt snippet cache\nDispatch to pandas for exact matches"]

    FP -->|"pandas_hit\nExact snippet matched - no LLM needed"| NAR

    FP -->|"miss\nNo cached match"| FUD

    FUD["follow_up_detector\nRead force_follow_up flag from UI toggle\nON + valid prev SQL → CTE path\nOFF → fresh query"]

    FUD --> SL

    SL["schema_loader\nFetch DDL from information_schema\nFetch 3 sample rows\nFilter schema to active_table only"]

    SL -->|"error\nDuckDB unreachable or no tables"| DEAD1(["END - error returned"])

    SL -->|ok| TS

    TS["table_selector\nSingle-table fast-path\nPasses through - one table after schema filter"]

    TS --> CB

    CB["cte_builder\nFollow-up mode ON:\nWrap prev_sql as WITH prev_result AS ...\nLLM will SELECT ... FROM prev_result\nFollow-up mode OFF:\nNo-op - passes state through"]

    CB --> SG

    SG["sql_generator\nBuild prompt: DDL + samples + rules\nLLM call - DeepSeek V3  temp=0\nPost-process: backtick → double-quote\nFix MIN col → MIN col"]

    SG --> VAL

    VAL["validator\nScan generated SQL for\nINSERT · UPDATE · DELETE · DROP · ALTER\nBlock any destructive statement"]

    VAL -->|"invalid\nDestructive keyword detected"| DEAD2(["END - blocked"])

    VAL -->|valid| EX

    EX["executor\nRun SELECT against DuckDB\nOn error: retry once with corrected prompt"]

    EX -->|"error\nSQL failed on both attempts"| DEAD3(["END - error returned"])

    EX -->|success| EC

    EC["empty_check\nIf DataFrame has 0 rows:\nRetry sql_generator with broader prompt\nIf still empty: pass through with empty DF"]

    EC --> NAR

    NAR["narrator\nChoose length tier by row count:\n1×1 → one bold sentence\n2-8 rows → bullet per row\n9-30 rows → top 3 / bottom 3  max 600 tokens\n30+ rows → 3-5 insight bullets  max 350 tokens\nLLM call - DeepSeek V3  temp=0.3"]

    NAR --> GR

    GR["grounding\nExtract numbers from narration text\nCheck each against raw DataFrame values\nFlag mismatches → yellow warning banner in UI"]

    GR --> OK(["END - QueryResult returned to _handle_send"])
```

---

## 4. Multi-Turn Clarification Flow

How `_handle_send` intercepts a vague question before the pipeline runs.

```mermaid
flowchart TD
    ENTRY(["_handle_send called\nUser message displayed in chat"]) --> ACTIVE

    ACTIVE{"state.clarification_active?"}

    %% ── Clarification already in progress ──────────────────────────────────
    ACTIVE -->|"Yes - user is answering a clarification"| APPEND
    APPEND["Append user reply to clarification_history\nIncrement clarification_count"]
    APPEND --> MAX{"count >= 5\nMax turns reached?"}

    MAX -->|Yes| FORCE["_build_enriched_question\nConcat: original question + all user answers\njoined with semicolons"]
    FORCE --> RESET1["_reset_clarification_state"]
    RESET1 --> PIPELINE

    MAX -->|No| CLC["_check_clarification_complete\nLLM call  temp=0  max 200 tokens\nPasses: original question + full history\nReturns JSON: is_clear + value"]
    CLC --> CLEAR{"is_clear?"}

    CLEAR -->|Yes - enough detail| ENRICH["Use value as enriched question\n_reset_clarification_state"]
    ENRICH --> PIPELINE

    CLEAR -->|"No - still vague"| NEXT["Append next question to history\n_show_clarification_card - blue UI card\n_save_clarification_message - SQLite\nReturn - pipeline does NOT run"]

    %% ── Fresh question path ────────────────────────────────────────────────
    ACTIVE -->|No - fresh question| FOLLOWUP
    FOLLOWUP{"follow_up_enabled\nOR no active table?"}
    FOLLOWUP -->|"Yes - skip check\nContext already known"| PIPELINE
    FOLLOWUP -->|No| GET_DDL["_get_active_ddl\nSqlExecutor.get_table_ddl_map\nReturns DDL for active_table only"]
    GET_DDL --> DDL_OK{"DDL returned?"}
    DDL_OK -->|No| PIPELINE
    DDL_OK -->|Yes| AMB["_check_ambiguity\nLLM call  temp=0  max 150 tokens\nPasses: question + DDL\nReturns JSON: is_vague + question"]
    AMB --> VAGUE{"is_vague AND\nclarifying question\nreturned?"}
    VAGUE -->|No - specific enough| PIPELINE
    VAGUE -->|Yes - 2+ dimensions unclear| ACTIVATE2["Set clarification_active = True\nStore original_question\ncount = 1\nhistory = first clarifying question\n_show_clarification_card - blue card\n_save_clarification_message - SQLite\nReturn - pipeline does NOT run"]

    %% ── Pipeline runs ──────────────────────────────────────────────────────
    PIPELINE(["pipeline.run - question enriched or original\nfull LangGraph execution"])

    %% ── Fail-open notes ───────────────────────────────────────────────────
    style NEXT fill:#1e3a5f,stroke:#3b82f6,color:#bfdbfe
    style ACTIVATE2 fill:#1e3a5f,stroke:#3b82f6,color:#bfdbfe
    style PIPELINE fill:#14532d,stroke:#22c55e,color:#bbf7d0
```

---

## 5. Data Layer

SQLite schema for chat persistence and DuckDB for analytical queries.

```mermaid
erDiagram
    CHATS {
        TEXT id PK "UUID - generated at chat creation"
        TEXT title "First 50 chars of the opening question"
        TEXT file_name "Uploaded filename at time of creation"
        TEXT created_at "ISO 8601 UTC timestamp"
        TEXT updated_at "Bumped on every new message"
    }

    MESSAGES {
        INTEGER id PK "Autoincrement"
        TEXT chat_id FK "References chats.id - CASCADE DELETE"
        TEXT role "user or assistant"
        TEXT content "Raw question or narration markdown"
        TEXT sql_text "Generated SQL or pandas dispatch comment"
        TEXT df_json "DataFrame as JSON records - capped at 500 rows"
        TEXT grounding_json "Number verification result from grounding node"
        INTEGER retried "1 if empty_check retried SQL generation"
        TEXT error "Error message if pipeline failed"
        INTEGER is_follow_up "1 if this was a CTE follow-up query"
        INTEGER follow_up_depth "Depth counter 0 to 3"
        TEXT created_at "ISO 8601 UTC timestamp"
    }

    DUCKDB_TABLE {
        TEXT table_name "Sanitized filename stem"
        COLUMN col_1 "Sanitized - underscored lowercased"
        COLUMN col_n "All columns from uploaded file"
    }

    CHATS ||--o{ MESSAGES : "contains"
    CHATS }o--o{ DUCKDB_TABLE : "was queried against"
```

**Storage locations:**

| Store | File | Purpose |
|---|---|---|
| SQLite | `data/chat_history.db` | Chat sessions, messages, DataFrames (≤500 rows) |
| DuckDB | `data/datavault.duckdb` | All uploaded tables - single shared file |

**SQLite maintenance:** `VACUUM` runs at startup and after each chat delete. WAL journal mode enabled for safe concurrent reads.

---

## 6. UI Component Layout

How NiceGUI constructs the full desktop window.

```mermaid
flowchart TD
    subgraph WINDOW["Browser Window  -  NiceGUI serves on localhost"]
        subgraph HEADER["Fixed Header"]
            TITLE["DataVault AI  title"]
            THEME["Theme toggle"]
            BTNS["Clear Chat  ·  New File"]
        end

        subgraph BODY["Body Row"]
            subgraph DRAWER["Left Drawer  -  always visible"]
                NEW_CHAT["+ New Chat button"]
                SEP["Separator"]
                SIDEBAR_LIST["Scrollable chat list\nEach row: title · date · delete X\nClick → _load_chat"]
            end

            subgraph MAIN["Main Content Column"]
                subgraph TABS_BAR["Sticky Tabs Row"]
                    T1["Chat tab"]
                    T2["Suggestions tab"]
                    T3["Dataset tab"]
                end

                subgraph TAB_PANELS["Tab Panels"]
                    subgraph CHAT_PANEL["Chat Panel"]
                        UPLOAD_ZONE["Upload Zone\nShown before first file\nHidden after upload"]
                        CHAT_MSGS["chat_messages column\nScrollable message thread\nUser bubbles + Assistant cards"]
                    end

                    subgraph SUGGEST_PANEL["Suggestions Panel"]
                        AI_CARDS["AI suggestion cards\nBeginner · Intermediate · Advanced"]
                        SNIPPET_GROUPS["Prompt snippet groups\nData Profile · Quality Check · Business Insights"]
                    end

                    subgraph DATASET_PANEL["Dataset Panel"]
                        SUMMARY_ROW["Total rows · tables · columns"]
                        TABLE_CARDS["Per-table cards:\n  Warning banner - yellow if issues\n  Schema expansion - columns + types + stats\n  Preview expansion - first 100 rows\n  Delete button"]
                    end
                end
            end
        end

        subgraph FOOTER["Pinned Input Footer\nHidden until file is loaded"]
            INPUT_ROW["Chat input  +  Send button"]
            TOGGLE_ROW["Follow-up mode switch  +  hint label"]
            SWITCHER_ROW["Active file dropdown - shown when 2+ files"]
        end
    end

    subgraph CARDS["Assistant Result Card  (rendered inside chat_messages)"]
        NARRATION["Narration - styled markdown"]
        GROUNDING_WARN["Grounding warning banner - yellow  if numbers mismatch"]
        DATA_TABLE["Data table - first 200 rows"]
        ACTION_BAR["PDF button  ·  Excel button  ·  View SQL panel  ·  Follow-up depth chip"]
    end

    subgraph CLARIFICATION["Clarification Card  (rendered inside chat_messages)"]
        BLUE_CARD["Blue card\nhelp_outline icon\nNeed a bit more detail header\nClarifying question text"]
    end
```

---

## AppState Field Map

All session state fields on the module-level `AppState` singleton, grouped by concern.

```mermaid
flowchart LR
    subgraph AS["AppState  -  app.py module level"]
        subgraph FILE["File / DB"]
            F1["db_path"]
            F2["table_names"]
            F3["uploaded_filenames"]
            F4["active_table"]
            F5["column_stats"]
            F6["dataset_info"]
            F7["ai_suggestions"]
            F8["sanity_warnings"]
        end

        subgraph CHAT["Chat Session"]
            C1["current_chat_id"]
            C2["msg_counter"]
        end

        subgraph FOLLOWUP["Follow-Up Tracking"]
            FU1["follow_up_enabled"]
            FU2["last_sql"]
            FU3["last_narration"]
            FU4["last_result_shape"]
            FU5["last_result_columns"]
            FU6["follow_up_depth"]
            FU7["last_was_pandas"]
        end

        subgraph CLARIFY["Clarification State"]
            CL1["clarification_active"]
            CL2["clarification_history"]
            CL3["clarification_count"]
            CL4["original_question"]
        end

        subgraph SNIPPETS["Prompt Snippets"]
            SN1["prompt_snippets"]
        end
    end

    subgraph RESET["Reset triggers"]
        R1["_new_chat"]
        R2["_clear_chat"]
        R3["_load_chat"]
        R4["_on_file_switch"]
        R5["_clear_dataset"]
    end

    R1 -->|"clears all session fields"| CHAT
    R1 -->|"resets"| FOLLOWUP
    R1 -->|"resets"| CLARIFY
    R2 -->|"clears"| CHAT
    R2 -->|"resets"| CLARIFY
    R3 -->|"resets"| CLARIFY
    R4 -->|"resets follow-up"| FOLLOWUP
    R4 -->|"resets"| CLARIFY
    R5 -->|"clears all"| FILE
```
