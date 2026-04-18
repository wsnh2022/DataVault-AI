"""
tests/smoke_test.py - One-liner verifiable smoke tests for all 6 core modules.

Run from project root:
    python -m pytest tests/smoke_test.py -v

Or without pytest:
    python tests/smoke_test.py

These tests do NOT call OpenRouter. They validate:
- Module imports without errors
- SqlExecutor creates a DB, writes a table, reads schema, runs a query
- SqlValidator accepts valid SQL and rejects destructive SQL
- SqlGenerator.extract_sql() parses all known LLM output formats
- GroundingVerifier ignores list indices, question numbers, small integers
- Narrator returns a fallback string on empty DataFrame (no API call)
- QueryResult dataclass initializes with correct defaults
"""

import sys
import os
import tempfile
from pathlib import Path

# Allow imports from project root regardless of where pytest is run from
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd

# ------------------------------------------------------------------
# 1. config.py
# ------------------------------------------------------------------
def test_config_imports():
    import config
    assert config.OPENROUTER_BASE_URL == "https://openrouter.ai/api/v1"
    assert config.SQL_EMPTY_RETRY_LIMIT == 1
    assert config.GROUNDING_IGNORE_INTEGERS_BELOW == 11
    assert config.DATA_DIR.exists()
    assert config.OUTPUTS_DIR.exists()
    print("PASS: config.py")


# ------------------------------------------------------------------
# 2. core/sql_executor.py
# ------------------------------------------------------------------
def test_sql_executor():
    import duckdb
    from core.sql.sql_executor import SqlExecutor

    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        db_path = f.name
    # DuckDB rejects a pre-existing empty file - remove it so DuckDB creates fresh
    os.unlink(db_path)

    try:
        # Write a test table directly via DuckDB
        conn = duckdb.connect(db_path)
        conn.execute(
            "CREATE TABLE sales (id INTEGER, product VARCHAR, amount DOUBLE)"
        )
        conn.execute("INSERT INTO sales VALUES (1, 'Widget', 99.5)")
        conn.execute("INSERT INTO sales VALUES (2, 'Gadget', 149.0)")
        conn.close()

        ex = SqlExecutor(db_path)

        # Schema DDL must contain the table name
        ddl = ex.get_schema_ddl()
        assert "sales" in ddl.lower(), f"DDL missing table: {ddl}"

        # Table names list
        tables = ex.get_table_names()
        assert "sales" in tables

        # Sample rows
        sample = ex.get_sample_rows("sales", n=1)
        assert len(sample) == 1

        # Run a SELECT
        df = ex.run("SELECT * FROM sales;")
        assert len(df) == 2
        assert "product" in df.columns

        # is_select
        assert ex.is_select("SELECT * FROM sales")
        assert not ex.is_select("DELETE FROM sales")

        print("PASS: core/sql_executor.py")
    finally:
        os.unlink(db_path)


# ------------------------------------------------------------------
# 3. core/sql_validator.py
# ------------------------------------------------------------------
def test_sql_validator():
    from core.sql.sql_validator import SqlValidator

    v = SqlValidator()

    # Valid SELECT
    ok, reason = v.validate_sql("SELECT * FROM sales;")
    assert ok, f"Expected valid: {reason}"

    # Valid WITH (CTE)
    ok, reason = v.validate_sql("WITH x AS (SELECT 1) SELECT * FROM x;")
    assert ok, f"Expected valid CTE: {reason}"

    # Blocked: DELETE
    ok, reason = v.validate_sql("DELETE FROM sales;")
    assert not ok, "DELETE should be blocked"

    # Blocked: DROP
    ok, reason = v.validate_sql("DROP TABLE sales;")
    assert not ok, "DROP should be blocked"

    # Empty
    ok, reason = v.validate_sql("")
    assert not ok

    # is_empty_result
    assert v.is_empty_result(pd.DataFrame())
    assert not v.is_empty_result(pd.DataFrame({"a": [1]}))

    print("PASS: core/sql_validator.py")


# ------------------------------------------------------------------
# 4. core/sql_generator.py - extract_sql only (no API call)
# ------------------------------------------------------------------
def test_sql_generator_extract():
    from core.sql.sql_generator import SqlGenerator

    # Fenced ```sql block
    raw = "Here is the query:\n```sql\nSELECT * FROM sales;\n```"
    sql = SqlGenerator.extract_sql(raw)
    assert "SELECT" in sql.upper(), f"Extraction failed: {sql}"

    # Plain SELECT
    raw2 = "SELECT id, product FROM sales WHERE amount > 100;"
    sql2 = SqlGenerator.extract_sql(raw2)
    assert sql2.startswith("SELECT")

    # WITH clause
    raw3 = "WITH totals AS (SELECT SUM(amount) AS t FROM sales) SELECT t FROM totals;"
    sql3 = SqlGenerator.extract_sql(raw3)
    assert sql3.startswith("WITH")

    print("PASS: core/sql_generator.py (extract_sql)")


# ------------------------------------------------------------------
# 5. core/narrator.py - empty df path only (no API call)
# ------------------------------------------------------------------
def test_narrator_empty():
    from core.sql.narrator import Narrator

    # Patching the client to avoid real API call
    n = Narrator.__new__(Narrator)  # skip __init__ to avoid OpenAI client creation
    # Call narrate directly with empty df - should return fallback without API call
    result = n.narrate.__func__(n, "test question", "SELECT 1", pd.DataFrame())
    assert "no results" in result.lower() or "returned" in result.lower()
    print("PASS: core/narrator.py (empty path)")


# ------------------------------------------------------------------
# 6. core/grounding_verifier.py
# ------------------------------------------------------------------
def test_grounding_verifier():
    from core.sql.grounding_verifier import GroundingVerifier

    gv = GroundingVerifier()

    # Data contains 99.5 and 149.0
    df = pd.DataFrame({"amount": [99.5, 149.0], "id": [1, 2]})

    # Narration references a real value - should pass
    result = gv.verify("The top amount is 149.0.", df, "What is the top amount?")
    assert result["is_grounded"], f"Expected grounded: {result}"

    # List indices (1. 2.) should be ignored
    result2 = gv.verify("Results:\n1. Widget 99.5\n2. Gadget 149.0", df, "List products")
    assert result2["is_grounded"], f"List indices incorrectly flagged: {result2}"

    # Question number should be ignored ("top 5")
    result3 = gv.verify("Found 5 results with amount 99.5", df, "Show top 5 products")
    assert result3["is_grounded"], f"Question number incorrectly flagged: {result3}"

    # Small integer (count <= 10) should be ignored
    result4 = gv.verify("There are 2 products.", df, "How many products?")
    assert result4["is_grounded"], f"Small integer incorrectly flagged: {result4}"

    # Hallucinated value NOT in data should be flagged
    result5 = gv.verify("The total is 999999.99.", df, "What is the total?")
    assert not result5["is_grounded"], f"Hallucination not caught: {result5}"

    print("PASS: core/grounding_verifier.py")


# ------------------------------------------------------------------
# 7. core/query_pipeline.py - QueryResult defaults
# ------------------------------------------------------------------
def test_query_result_defaults():
    from core.query_pipeline import QueryResult

    r = QueryResult(question="test")
    assert r.error == ""
    assert r.sql == ""
    assert r.narration == ""
    assert r.retried is False
    assert r.df.empty
    print("PASS: core/query_pipeline.py (QueryResult defaults)")


# ------------------------------------------------------------------
# 8. outputs
# ------------------------------------------------------------------
def test_exports():
    from export.pdf_generator import dataframe_to_pdf_bytes
    from export.excel_exporter import dataframe_to_excel_bytes

    df = pd.DataFrame({"product": ["Widget", "Gadget"], "amount": [99.5, 149.0]})

    pdf_bytes = dataframe_to_pdf_bytes(df, title="Smoke Test")
    assert len(pdf_bytes) > 0
    assert pdf_bytes[:4] == b"%PDF"   # valid PDF magic bytes

    excel_bytes = dataframe_to_excel_bytes(df)
    assert len(excel_bytes) > 0
    # xlsx is a ZIP - magic bytes are PK
    assert excel_bytes[:2] == b"PK"

    print("PASS: outputs/pdf_generator.py + excel_exporter.py")


# ------------------------------------------------------------------
# Runner
# ------------------------------------------------------------------
if __name__ == "__main__":
    tests = [
        test_config_imports,
        test_sql_executor,
        test_sql_validator,
        test_sql_generator_extract,
        test_narrator_empty,
        test_grounding_verifier,
        test_query_result_defaults,
        test_exports,
    ]

    passed = 0
    failed = 0
    for t in tests:
        try:
            t()
            passed += 1
        except Exception as e:
            print(f"FAIL: {t.__name__} -> {e}")
            failed += 1

    print(f"\n{passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)
