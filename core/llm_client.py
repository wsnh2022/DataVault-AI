"""
core/llm_client.py - Raw HTTP client for OpenRouter using httpx.

Bypasses the openai Python package entirely.
Root cause of 401: openai package sends extra headers (OpenAI-Organization,
OpenAI-Project) that confuse OpenRouter's auth layer on certain account types.
httpx sends exactly what we specify - same as curl which confirmed working.
"""

import time
import logging
import httpx
import config

logger = logging.getLogger(__name__)

_HEADERS = {
    "Content-Type": "application/json",
    "HTTP-Referer": "https://github.com/wsnh2022",
    "X-Title": "DataVault AI",
}


def call_llm(
    messages: list[dict],
    max_tokens: int = None,
    temperature: float = None,
    model: str = None,
) -> str:
    """
    Calls OpenRouter via raw HTTP POST. Returns the response content string.
    Raises RuntimeError if all models in the fallback chain fail.

    Args:
        messages:    Chat messages list.
        max_tokens:  Defaults to config.LLM_MAX_TOKENS.
        temperature: Defaults to config.LLM_TEMPERATURE.
        model:       Primary model to use. Defaults to config.SQL_MODEL.
                     Falls back through config.MODEL_FALLBACK_CHAIN on failure.
    """
    max_tokens = max_tokens or config.LLM_MAX_TOKENS
    temperature = temperature if temperature is not None else config.LLM_TEMPERATURE

    # Build the chain: requested model first, then fallbacks
    primary = model or config.SQL_MODEL
    chain = [primary] + [m for m in config.MODEL_FALLBACK_CHAIN if m != primary]

    headers = {
        **_HEADERS,
        "Authorization": f"Bearer {config.OPENROUTER_API_KEY}",
    }

    last_error = None
    for m in chain:
        payload = {
            "model": m,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        try:
            resp = httpx.post(
                f"{config.OPENROUTER_BASE_URL}/chat/completions",
                headers=headers,
                json=payload,
                timeout=config.LLM_TIMEOUT_SECONDS,
            )
            resp.raise_for_status()
            data = resp.json()
            content = data["choices"][0]["message"]["content"] or ""
            logger.info("call_llm success | model: %s | tokens: %s",
                m, data.get("usage", {}).get("total_tokens", "?"))
            return content
        except httpx.HTTPStatusError as e:
            logger.warning("Model %s HTTP error: %s | body: %s", m, e, resp.text)
            last_error = f"{e} - {resp.text}"
            time.sleep(0.5)
        except Exception as e:
            logger.warning("Model %s failed: %s", m, e)
            last_error = str(e)
            time.sleep(0.5)

    raise RuntimeError(f"All models in fallback chain failed. Last error: {last_error}")
