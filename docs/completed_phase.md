# DataVault AI - NiceGUI Migration Phases

## Overview

Migrated from Streamlit to NiceGUI. Only `app.py` was replaced.
All of `core/`, `export/`, `config.py`, `tests/`, `prompt_snippets.json` untouched.

**New files added:**
- `core/chat_store.py` - SQLite chat history persistence
- `data/chat_history.db` - SQLite database (auto-created on startup)

**Status legend:** [x] Done | [ ] Not started | [~] In progress / partial

---

## Phase 1 - SQLite Chat Store
**File:** `core/chat_store.py`
**Status:** [x] Done

Schema: `chats` (id, title, file_name, created_at, updated_at) + `messages` (id, chat_id,
role, content, sql_text, df_json, grounding_json, retried, error, is_follow_up,
follow_up_depth, created_at).

Functions: `init_db`, `create_chat`, `update_chat`, `save_message`, `list_chats`,
`load_messages`, `delete_chat`, `get_chat`, `df_to_json`, `df_from_json`.

---

## Phase 2 - NiceGUI App Skeleton
**File:** `app.py`
**Status:** [x] Done

- Header: fixed top - title, dark mode toggle, Clear Chat, refresh icon, upload icon
- Left drawer: always-visible sidebar - New Chat button + scrollable chat history list
- Tabs: Chat | Suggestions | Dataset
- AppState dataclass with all fields
- `ui.run()` on port 8080, dark=True
- Ctrl+C handled cleanly (KeyboardInterrupt caught)

---

## Phase 3 - File Upload + DuckDB Ingest
**File:** `app.py`
**Status:** [x] Done

- NiceGUI 3.10.0 API: `e.file.name`, `await e.file.read()` (async)
- Single fixed `DUCKDB_PATH = config.DATA_DIR / "datavault.duckdb"` for all uploads
  (avoids multi-connection DuckDB locking issues when switching files)
- Column sanitization: spaces/slashes -> underscores, lowercased
- Date normalization at ingest
- `asyncio.to_thread()` for blocking DuckDB write
- Upload zone hidden, chat area shown after successful upload
- `input_footer.value = True` (Quasar value prop, not `set_visibility`)
- `_activate_chat_view()` handles all visibility transitions
- Dataset card rendered as first chat message after upload

---

## Phase 4 - Chat Functionality + asyncio
**File:** `app.py`
**Status:** [x] Done

- `asyncio.to_thread(pipeline.run, ...)` for non-blocking LLM calls
- Follow-up context built from state when `follow_up_enabled = True`
- User + assistant messages saved to SQLite on every send
- Chat created on first message (title = first 50 chars of question)
- Sidebar refreshed after every send

---

## Phase 5 - Chat History Sidebar (Load + Delete)
**File:** `app.py`
**Status:** [x] Done

- `_load_chat()`: clears UI, fetches messages from SQLite, re-renders all bubbles
- `_delete_chat()`: CASCADE deletes, resets to new chat if current
- Sidebar item: label with `truncate` CSS (single line), X button on hover
- `_new_chat()`: resets state, keeps chat view visible if file loaded, returns to
  upload screen if no file loaded, always calls `_refresh_sidebar()`
- `CHAT_DB_PATH = config.DATA_DIR / "chat_history.db"` (absolute path via config)
- DataFrame stored capped at 500 rows to keep DB lean (full data re-queryable from DuckDB)

---

## Phase 6 - Suggestions Tab
**File:** `app.py`
**Status:** [x] Done

- AI suggestions generated in background via `asyncio.create_task(_fetch_suggestions_bg())`
- Editable snippet groups: add prompt, delete prompt, persist to `prompt_snippets.json`
- `_send_suggestion()`: fills input, switches to Chat tab, fires send
- `_rebuild_suggest_tab()` called after upload and after each suggestion fetch

---

## Phase 7 - Dataset Tab
**File:** `app.py`
**Status:** [x] Done

- Per-table cards: filename, row/col count, red delete button (per-file, not global)
- `_delete_table(table_name)`: drops table from DuckDB, removes from state, rebuilds tabs
- "Preview data" expansion per card: first 100 rows
- Summary row: total rows, tables, total columns
- Refresh icon, upload icon, no global "delete all" button
- `_rebuild_dataset_tab()` called after upload, refresh, delete

---

## Phase 8 - PDF / Excel Export
**File:** `app.py`
**Status:** [x] Done

- `_download_pdf()` / `_download_excel()` via `asyncio.to_thread()`
- `ui.download(bytes, filename)` triggers browser download
- Buttons shown in action bar below each assistant message with a non-empty DataFrame

---

## Phase 9 - Ancillary Files
**Status:** [x] Done

- `requirements.txt`: streamlit removed, nicegui added
- `start_chatbot.bat`: updated to `python app.py`
- `setup.bat`: updated launch command

---

## Phase 10 - Cloudflare Tunnel
**Status:** [ ] Not started

One-time setup:
```bash
cloudflared tunnel create datavault-ai
cloudflared tunnel route dns datavault-ai datavault.yourdomain.com
```

Config `~/.cloudflared/datavault-ai.yml`:
```yaml
tunnel: datavault-ai
credentials-file: C:\Users\yogi\.cloudflared\<tunnel-id>.json
ingress:
  - hostname: datavault.yourdomain.com
    service: http://localhost:8080
  - service: http_status:404
```

Run alongside app: `cloudflared tunnel --config ... run`

---

## Post-Migration UI Improvements (all done)

| Feature | Status | Notes |
|---|---|---|
| Chat card redesign | [x] | Card layout: narration, grounding banner, scrollable table, action bar |
| Markdown spacing | [x] | `[&_p]:mb-3`, `[&_li]:mt-1.5`, heading margins - breathing room |
| Capped heading sizes | [x] | `[&_h1]:text-base` etc. via Tailwind arbitrary variants |
| `_fix_markdown()` pre-processor | [x] | Adds blank lines before lists/headings for CommonMark |
| Remove auto-retried chip | [x] | Was showing on every query; surfaced internal detail |
| Follow-up chip | [x] | Still shown - user-meaningful context |
| Input footer 3-row layout | [x] | Input+Send / Follow-up toggle / File switcher (bottom, opens upward) |
| File switcher drop-up | [x] | Moved to bottom of footer so dropdown opens upward |
| Dark mode toggle icon | [x] | `dark_mode` Material Icon, round button (replaced "Theme" text) |
| Sidebar toggle `\|\|\|` | [x] | Header button, hides/shows left drawer |
| New Chat in upload zone | [x] | Button below upload widget for quick access |
| Refresh button | [x] | Header icon + dataset tab - re-scans DuckDB, rebuilds tabs |
| Upload button | [x] | Header icon - goes to upload screen |
| Per-file delete in dataset | [x] | Trash icon per table card, not global wipe |
| Clear dataset | [x] | `_clear_dataset()` drops all tables, resets state, returns to upload |
| Auto-restore on page reload | [x] | `ui.timer(0.3, _refresh_data, once=True)` re-scans DuckDB on connect |
| Chat history persistence | [x] | Absolute CHAT_DB_PATH, _refresh_sidebar() called from timer too |

---

## SQL / Narration Prompt Improvements (all done)

| Fix | File | Notes |
|---|---|---|
| Year/month/day granularity | `core/sql_generator.py` | Match granularity to question, not just monthly default |
| Percentage consistency rule | `core/sql_generator.py` | Prohibit mixing aggregation levels for pct vs score columns |
| Large result narration | `core/narrator.py` | >30 rows: 3-5 flat insight bullets only, no nested top/bottom lists |
| max_tokens for large results | `core/narrator.py` | 350 tokens for >30 rows - forces brevity |

---

## Known Issues / Still Pending

| Item | Priority | Notes |
|---|---|---|
| Cloudflare Tunnel | Low | Phase 10 - not started |
| Mini sidebar (icon-only mode) | Low | Was built and reverted - caused chat history not to show; needs redesign |
| Chat history restore is slow for large DFs | Medium | df_json capped at 500 rows now but full load still reads all messages |
| Grounding warning too aggressive | Low | Flags computed numbers like percentages and year numbers |
| `python -m pytest tests/smoke_test.py` | Verify | Smoke tests not re-run since NiceGUI migration |

---

## Critical Constraints (from CLAUDE.md)

- No emojis in any Python string, label, or UI text - UnicodeEncodeError on Windows
- No em dash - use hyphen only
- `dotenv_values()` not `load_dotenv()`
- `httpx` not `openai` SDK for LLM calls
- Always check `result.error` before accessing other QueryResult fields
- `QueryResult` never raises - errors surface via `result.error` string
- `DUCKDB_PATH` is a single fixed file - never change it per-upload
- `CHAT_DB_PATH` uses `config.DATA_DIR` - always absolute
- `input_footer.value = True` not `set_visibility(True)` (Quasar value prop)
- All async handlers must be defined as `async def` - not wrapped in `create_task`

---

## Final Verification Checklist

- [x] `python app.py` - browser opens at localhost:8080
- [x] Upload CSV - dataset card appears, file switcher shows for 2+ files
- [x] Ask question - result with narration + SQL expansion + table
- [x] PDF button - downloads
- [x] Excel button - downloads
- [x] Restart app - old chats appear in sidebar
- [x] Click sidebar chat - loads history, restores table
- [x] X on chat - removes from sidebar
- [x] Click suggestion - auto-sends in Chat tab
- [x] Add/delete snippet - persists after restart
- [x] Upload second file - no DuckDB lock/crash (single shared DB)
- [x] New Chat button - clears messages, stays in chat view if file loaded
- [x] Refresh icon - rebuilds dataset + suggestions tabs
- [x] Per-file delete - removes only that table
- [ ] `python -m pytest tests/smoke_test.py -v` - verify all 8 tests still pass
- [ ] Cloudflare Tunnel from phone on different network
