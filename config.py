"""
config.py - Central configuration loader for DataVault AI V1.

Uses dotenv_values() instead of load_dotenv() to read directly from .env file.
load_dotenv() silently skips values already set in the shell environment.
dotenv_values() always reads from file - shell environment cannot override it.
"""

from pathlib import Path
from dotenv import dotenv_values

# Always read from file - never from shell environment
_env = dotenv_values(Path(__file__).parent / ".env")

# --- OpenRouter ---
OPENROUTER_API_KEY: str = _env.get("OPENROUTER_API_KEY", "")
OPENROUTER_BASE_URL: str = "https://openrouter.ai/api/v1"

# DeepSeek V3 for both SQL and narration - strong on code/SQL benchmarks,
# better format choices than LLaMA (long-format over wide pivot),
# comparable cost. Fallback chain: claude-3.5-haiku then gpt-4o-mini.
SQL_MODEL: str = "deepseek/deepseek-chat"
NARRATION_MODEL: str = "deepseek/deepseek-chat"

# Fallback chain - only used if primary model fails (duplicates filtered automatically)
MODEL_FALLBACK_CHAIN: list[str] = [
    "anthropic/claude-3.5-haiku",
    "openai/gpt-4o-mini",
]

# LLM call settings
LLM_MAX_TOKENS: int = int(_env.get("LLM_MAX_TOKENS", "2048"))
LLM_TEMPERATURE: float = float(_env.get("LLM_TEMPERATURE", "0.0"))  # deterministic SQL
LLM_TIMEOUT_SECONDS: int = int(_env.get("LLM_TIMEOUT_SECONDS", "60"))

# SQL retry: one retry on empty result
SQL_EMPTY_RETRY_LIMIT: int = 1

# --- Paths ---
PROJECT_ROOT: Path = Path(__file__).parent
DATA_DIR: Path = PROJECT_ROOT / "data"
OUTPUTS_DIR: Path = PROJECT_ROOT / "outputs"

DATA_DIR.mkdir(exist_ok=True)
OUTPUTS_DIR.mkdir(exist_ok=True)

# --- Grounding verifier ---
GROUNDING_IGNORE_INTEGERS_BELOW: int = 11

# --- Validation ---
def validate() -> None:
    if not OPENROUTER_API_KEY:
        raise ValueError(
            "OPENROUTER_API_KEY is not set. "
            "Add it to your .env file: OPENROUTER_API_KEY=your_key_here"
        )
