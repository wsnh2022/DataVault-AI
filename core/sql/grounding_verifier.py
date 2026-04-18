"""
core/grounding_verifier.py - Checks that numbers in the narration exist in the data.

Explorer lesson (Phase 6): verifier false-flagged list indices (1., 2., 3.) and
question numbers ("top 10") as data mismatches because they weren't in the result set.
Fix: extract numbers from the original question and ignore them in verification.
Fix: ignore small integers below config.GROUNDING_IGNORE_INTEGERS_BELOW (default 11).
Fix: ignore numbers that appear as positional list indices in the narration text.
Prompt requirement: grounding_verifier must not false-flag list indices or question numbers.
"""

import re
import logging
import pandas as pd
import config

logger = logging.getLogger(__name__)


class GroundingVerifier:
    """
    Verifies that numeric claims in a narration string are grounded in the result DataFrame.
    Returns a GroundingResult with is_grounded flag and a list of flagged values.
    """

    def _extract_numbers_from_text(self, text: str) -> set[float]:
        """
        Extracts all numeric values from a string.
        Handles integers, decimals, and comma-formatted numbers (1,234).
        """
        # Remove comma separators before parsing (1,234 -> 1234)
        cleaned = text.replace(",", "")
        raw = re.findall(r"\b\d+(?:\.\d+)?\b", cleaned)
        return {float(n) for n in raw}

    def _extract_list_indices(self, narration: str) -> set[float]:
        """
        Extracts numbers used as list indices (1. 2. 3. or 1) 2) 3)).
        Explorer lesson: these are structural, not data values - must be ignored.
        """
        indices = re.findall(r"(?:^|\n)\s*(\d+)[.)]\s", narration)
        return {float(i) for i in indices}

    def _approx_in_data(self, num: float, data_values: set[float]) -> bool:
        """
        Returns True if num matches any data value when both are rounded to 2 decimal places.
        Handles the common case where the narrator rounds a long decimal (0.3008 -> 0.30)
        for readability, which would otherwise trigger a false grounding flag.
        """
        num_r = round(num, 2)
        return any(round(d, 2) == num_r for d in data_values)

    def _get_data_values(self, df: pd.DataFrame) -> set[float]:
        """
        Collects all numeric values present anywhere in the DataFrame.
        This is the ground truth pool against which narration is verified.
        """
        values: set[float] = set()
        for col in df.select_dtypes(include=["number"]).columns:
            for v in df[col].dropna():
                try:
                    values.add(float(v))
                except (ValueError, TypeError):
                    pass
        return values

    def verify(
        self, narration: str, df: pd.DataFrame, original_question: str = ""
    ) -> dict:
        """
        Main entry point.

        Returns:
            {
                "is_grounded": bool,
                "flagged": list[float],   # numbers in narration not found in data
                "ignored": list[float],   # numbers skipped (question/indices/small)
            }

        is_grounded=False means the narration contains a number not in the data.
        The pipeline does NOT block on this - it surfaces the warning to the UI only.
        Blocking on grounding failures would make the app unusable for edge cases.
        """
        if df is None or df.empty:
            return {"is_grounded": True, "flagged": [], "ignored": []}

        narration_numbers = self._extract_numbers_from_text(narration)
        question_numbers = self._extract_numbers_from_text(original_question)  # ignore these
        list_indices = self._extract_list_indices(narration)                    # ignore these
        data_values = self._get_data_values(df)

        flagged: list[float] = []
        ignored: list[float] = []

        for num in narration_numbers:
            # Rule 1: numbers below threshold are structural (counts, ranks, indices)
            if num < config.GROUNDING_IGNORE_INTEGERS_BELOW and num == int(num):
                ignored.append(num)
                continue

            # Rule 2: number appeared in the user's original question (e.g. "top 10")
            if num in question_numbers:
                ignored.append(num)
                continue

            # Rule 3: number is a list index in the narration text
            if num in list_indices:
                ignored.append(num)
                continue

            # Rule 4: number must exist in the data (exact or rounded to 2 decimal places)
            # Narrator often rounds long decimals (0.3008 -> 0.30) for readability.
            if num not in data_values and not self._approx_in_data(num, data_values):
                flagged.append(num)
                logger.debug("Grounding flag: %.4f not found in data values", num)

        is_grounded = len(flagged) == 0
        return {
            "is_grounded": is_grounded,
            "flagged": sorted(flagged),
            "ignored": sorted(set(ignored)),
        }
