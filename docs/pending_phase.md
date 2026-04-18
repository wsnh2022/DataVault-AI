# DataVault AI - Roadmap

## IN PROGRESS: Multi-Turn Clarification Before SQL

**Goal:** 

## Clarifying Questions Before SQL

Before writing any SQL, evaluate the question for **query ambiguity** across these dimensions:

| Dimension | Vague Signal | Example |
|---|---|---|
| **Time scope** | Undefined bands, relative dates, no anchor | "recently", "last period", "over time" |
| **Thresholds / filters** | Undefined cutoffs, implicit "good/bad" | "high value", "slow movers", "top customers" |
| **Granularity** | Unclear grouping or aggregation level | "by region" (which level?), "per product" (SKU or category?) |
| **Metric definition** | Ambiguous what to measure | "performance", "activity", "spend" |
| **Scope / population** | Unclear which rows/entities to include | "active", "relevant", "our customers" |
| **Comparison baseline** | Implied but unstated benchmark | "more than usual", "worse than expected" |
| **Output shape** | Unclear desired result format | "show me", "analyse", "summarise" |

**Decision rule:**  
If **2 or more dimensions** are ambiguous → ask up to 3–5 targeted clarifying questions before writing any SQL. Prioritize the questions that would most change the query structure, not just the values.

**Ask questions that are:**
- Closed or bounded where possible ("Is this calendar month or rolling 30 days?")
- Ordered by structural impact (time scope and population first, formatting last)
- Skipped if context already implies the answer (prior messages, schema names, column values)

**Never guess.** A wrong assumption in a filter or date range silently returns incorrect data.

**Approach:** Handled entirely in `_handle_send` in `app.py` - no pipeline changes.

**AppState fields to add:**
```python
clarification_active: bool = False
clarification_history: list = field(default_factory=list)
clarification_count: int = 0
original_question: str = ""
```

**Flow:**
1. Fresh question → `_check_ambiguity(question, ddl)` LLM call (temp=0, max_tokens=150)
2. If vague → show blue card with clarifying question, save to chat, return (no SQL)
3. User replies → `_check_clarification_complete(original_q, history)` LLM call
4. If still unclear AND count < 5 → ask next question
5. If clear OR count >= 5 → merge into enriched_question → run `pipeline.run()`

**UI:** Blue card (`border-blue-700 bg-blue-950`), `help_outline` icon, "Need a bit more detail" header

**Helpers needed in app.py (inside main_page):**
- `_check_ambiguity(question, ddl, sample_csv) -> (bool, str)` - LLM JSON: `{"is_vague": bool, "question": str}`
- `_check_clarification_complete(original_q, history) -> (bool, str)` - LLM JSON: `{"is_clear": bool, "value": str}`
- `_reset_clarification_state()` - call on new chat / load chat / clear chat
- `_show_clarification_card(text)` - renders blue card in chat_messages
- `_save_clarification_message(text)` - saves to chat_store (no sql/df/error)
- `_build_enriched_question(original, history)` - fallback concat for max-turns
- `_get_active_ddl()` - reads DDL from DuckDB for active_table

**Fail open:** Any LLM/parse error skips clarification, runs SQL as normal.

**Full plan:** `C:\Users\yogi\.claude\plans\parsed-petting-stallman.md`

---

## COMPLETED

- Single shared DuckDB (`datavault.duckdb`) - no per-file DB paths
- SQLite VACUUM on startup + after each chat delete - no wasted space
- Multi-sheet Excel detection - blocks upload, shows sheet names
- Upload error resets uploader widget
- Chat sidebar shows creation date + time
- `pd.to_numeric(errors="ignore")` fixed for pandas 3.x
- Time-of-day log truncation fixed ([:60] removed)
