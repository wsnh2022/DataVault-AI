"""
core/narrator.py - Converts a SQL result DataFrame into a plain English answer.

Uses core.llm_client (raw requests) instead of openai package.
Explorer lesson: send max 20 rows to narrator - token bloat prevention.
Uses config.NARRATION_MODEL - separate from SQL model so each can be tuned independently.
Prompt improvements:
- Lead with the direct answer, not context.
- Adaptive length: short for single-value results, one line per group for grouped results.
- Truncation disclosure: acknowledge when the full result exceeds the 20-row preview.
- Number formatting: commas for thousands, 2 decimal places.
"""

import logging
import pandas as pd
from core.llm_client import call_llm
import config

logger = logging.getLogger(__name__)

_MAX_ROWS_FOR_NARRATION = 20


class Narrator:

    def _build_messages(self, question: str, sql: str, df: pd.DataFrame) -> list[dict]:
        total_rows = len(df)
        shown_rows = min(total_rows, _MAX_ROWS_FOR_NARRATION)
        preview = df.head(_MAX_ROWS_FOR_NARRATION)
        data_text = preview.to_csv(index=False)

        # Tell the LLM explicitly when it is only seeing part of the result
        truncation_note = ""
        if total_rows > _MAX_ROWS_FOR_NARRATION:
            truncation_note = (
                f"\nNote: The full result has {total_rows} rows. "
                f"Only the first {shown_rows} are shown above. "
                "Acknowledge this in your answer when relevant."
            )

        # Detect result shape to guide response length
        numeric_cols = df.select_dtypes(include="number").columns.tolist()
        is_single_value = (total_rows == 1 and len(df.columns) == 1)
        is_grouped = (total_rows > 1 and len(numeric_cols) > 0)

        if is_single_value:
            structure_rule = (
                "- Single value result: answer in one bold sentence only. "
                "No bullets, no headings."
            )
        elif is_grouped and total_rows > 30:
            structure_rule = (
                "- Large result: the full data is already shown in the table - "
                "do NOT enumerate rows, do NOT list top/bottom values per group, "
                "do NOT use nested bullets.\n"
                "- Write exactly 3 to 5 flat bullet points covering:\n"
                "  1. The overall pattern or trend in one sentence.\n"
                "  2. The single highest value (what, where, when).\n"
                "  3. The single lowest value (what, where, when).\n"
                "  4. One or two notable observations (e.g. a group that stands out).\n"
                "- Each bullet is one concise sentence. No sub-bullets. No headings."
            )
        elif is_grouped and total_rows > 8:
            structure_rule = (
                "- Many groups: add a short ## heading that captures the finding. "
                "Show the top 3 and bottom 3 as two labeled flat bullet sections "
                "(**Top 3:** and **Bottom 3:**) - no nested bullets inside them. "
                "End with one sentence noting the total group count."
            )
        elif is_grouped:
            structure_rule = (
                "- Multiple rows: use a bullet point per row. "
                "Use a ## heading only if the result has 4 or more rows. "
                "No prose paragraphs."
            )
        else:
            structure_rule = "- Use 2 to 3 bullet points maximum. No headings needed."

        system = (
            "You are a data analyst assistant. Convert SQL result data into a structured, "
            "scannable markdown answer.\n\n"
            "FORMATTING RULES:\n"
            "- Use **bold** for key values and labels.\n"
            "- Use bullet points for lists - never write tabular data as a prose paragraph.\n"
            "- Use a ## heading only when specified in the structure rule below.\n"
            "- Never write a wall of text.\n\n"
            "CONTENT RULES:\n"
            "- Lead with the direct answer. No preamble or context-setting opener.\n"
            f"{structure_rule}\n"
            "- Do NOT perform any calculations - the numbers in the data are already final.\n"
            "- If only one group appears in the result, state that explicitly.\n"
            "- Use actual column names and values - do not paraphrase them.\n"
            "- Format numbers: commas for thousands, 2 decimal places where relevant "
            "(e.g. 1,234.56 not 1234.5600003).\n"
            "- Do not repeat all rows verbatim - summarize the pattern or finding."
        )

        user = (
            f"The user asked: {question}\n\n"
            f"SQL executed:\n{sql}\n\n"
            f"Result ({total_rows} row(s), showing {shown_rows}):\n{data_text}"
            f"{truncation_note}\n\n"
            "Answer in structured markdown."
        )

        return [
            {"role": "system", "content": system},
            {"role": "user",   "content": user},
        ]

    def narrate(self, question: str, sql: str, df: pd.DataFrame) -> str:
        if df is None or df.empty:
            return "The query returned no results. Try rephrasing your question."
        try:
            messages = self._build_messages(question, sql, df)
            # Raise max_tokens: grouped results with many rows need more room
            total_rows = len(df)
            if total_rows > 30:
                max_tokens = 350   # insight summary - short by design
            elif total_rows > 10:
                max_tokens = 600
            else:
                max_tokens = 300
            return call_llm(
                messages,
                max_tokens=max_tokens,
                temperature=0.3,
                model=config.NARRATION_MODEL,
            )
        except Exception as e:
            logger.error("Narrator failed: %s", e)
            return f"Query returned {len(df)} row(s). (Narration unavailable - {e})"
