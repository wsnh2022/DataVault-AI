"""
Quick API key test using raw requests - same as curl.
Usage: python test_key.py
"""
import os
from pathlib import Path
from dotenv import dotenv_values
import requests

# dotenv_values reads directly from file - ignores shell environment variables
# This is the correct way to verify what is actually in .env
env_file = Path(__file__).parent / ".env"
values = dotenv_values(env_file)

key_from_file = values.get("OPENROUTER_API_KEY", "")
key_from_env = os.environ.get("OPENROUTER_API_KEY", "")

print(f"Key in .env file : {key_from_file[:16]}...{key_from_file[-6:]} (length: {len(key_from_file)})")
print(f"Key in shell env : {key_from_env[:16]}...{key_from_env[-6:]} (length: {len(key_from_env)})")

if key_from_env and key_from_env != key_from_file:
    print("MISMATCH - shell environment variable is overriding .env file")
    print("Fix: run ->  set OPENROUTER_API_KEY=  <- in terminal to clear it")

# Use file value explicitly - bypass shell env
key = key_from_file
print(f"\nUsing key from file: {key[:16]}...")

headers = {
    "Authorization": f"Bearer {key}",
    "Content-Type": "application/json",
    "HTTP-Referer": "https://github.com/wsnh2022",
    "X-Title": "DataVault AI",
}

payload = {
    "model": "openrouter/auto",
    "messages": [{"role": "user", "content": "Say: OK"}],
    "max_tokens": 10,
}

try:
    resp = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers=headers,
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    content = resp.json()["choices"][0]["message"]["content"]
    print(f"SUCCESS: {content}")
except Exception as e:
    print(f"FAILED: {e}")
    if hasattr(e, 'response') and e.response is not None:
        print(f"Response body: {e.response.text}")
