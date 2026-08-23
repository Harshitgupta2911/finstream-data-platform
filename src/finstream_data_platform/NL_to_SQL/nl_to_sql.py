"""
NL-to-SQL for FinStream.

Flow:
    question -> fetch schema (cached) -> LLM generates SQL -> validate SQL
    -> execute against Databricks SQL Warehouse -> LLM explains results

Design decisions:
- Schema is fetched dynamically from INFORMATION_SCHEMA rather than hardcoded,
  so this stays correct as dbt models evolve without needing manual updates
  here every time a column is added/renamed.
- Only `gold` and `marts` schemas are ever queryable (see ALLOWED_SCHEMAS).
  The LLM is never given Bronze/Silver access, and validation rejects any
  SQL referencing a schema outside the allowlist even if the LLM tries.
- SQL is validated before execution: SELECT/WITH only, no DDL/DML keywords.
  We never blindly execute LLM-generated SQL.
"""

from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass

import pandas as pd
from google import genai
from google.genai import types
from databricks import sql as databricks_sql
from dotenv import load_dotenv

load_dotenv()

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
ALLOWED_SCHEMAS = [
    s.strip() for s in os.getenv("NL_TO_SQL_ALLOWED_SCHEMAS", "gold,marts").split(",")
]

# SQL keywords that indicate a write/DDL statement. If any of these appear as
# a standalone token in the generated SQL, we refuse to run it.
_FORBIDDEN_KEYWORDS = {
    "insert", "update", "delete", "drop", "alter", "truncate", "merge",
    "create", "grant", "revoke", "replace", "copy", "vacuum", "optimize",
}

_TOKEN_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


class UnsafeSQLError(Exception):
    """Raised when generated SQL fails validation and must not be executed."""


def _call_with_retry(fn, max_attempts: int = 3, base_delay_seconds: float = 2.0):
    """
    Retry a Gemini API call on transient errors (e.g. 503 UNAVAILABLE from
    upstream overload). Uses simple exponential backoff. Re-raises the last
    error if all attempts fail, so the caller still sees a real failure
    rather than this silently swallowing a persistent problem.
    """
    last_error = None
    for attempt in range(max_attempts):
        try:
            return fn()
        except Exception as e:
            last_error = e
            is_transient = "503" in str(e) or "UNAVAILABLE" in str(e) or "overloaded" in str(e).lower()
            if not is_transient or attempt == max_attempts - 1:
                raise
            time.sleep(base_delay_seconds * (2 ** attempt))
    raise last_error


@dataclass
class NLToSQLResult:
    question: str
    sql: str
    result_df: pd.DataFrame
    explanation: str


def _get_databricks_connection():
    return databricks_sql.connect(
        server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
        catalog=os.environ["DATABRICKS_CATALOG"],
    )


_schema_context_cache: str | None = None


def get_schema_context(force_refresh: bool = False) -> str:
    """
    Fetch table/column metadata for all allowed schemas from
    INFORMATION_SCHEMA and format it as text for the LLM prompt.
    Cached in-process since schema rarely changes within a session;
    pass force_refresh=True after a dbt run that changed models.
    """
    global _schema_context_cache
    if _schema_context_cache is not None and not force_refresh:
        return _schema_context_cache

    schema_filter = ", ".join(f"'{s}'" for s in ALLOWED_SCHEMAS)
    query = f"""
        SELECT table_schema, table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema IN ({schema_filter})
        ORDER BY table_schema, table_name, ordinal_position
    """

    with _get_databricks_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

    tables: dict[str, list[str]] = {}
    for table_schema, table_name, column_name, data_type in rows:
        key = f"{table_schema}.{table_name}"
        tables.setdefault(key, []).append(f"{column_name} ({data_type})")

    lines = []
    for table, columns in tables.items():
        lines.append(f"TABLE {table}:")
        lines.append("  " + ", ".join(columns))
    _schema_context_cache = "\n".join(lines)
    return _schema_context_cache


def _extract_sql(raw_response: str) -> str:
    """Strip markdown code fences if the model wraps its answer in ```sql ... ```."""
    match = re.search(r"```(?:sql)?\s*(.*?)```", raw_response, re.DOTALL)
    return (match.group(1) if match else raw_response).strip().rstrip(";")


def generate_sql(question: str, client: genai.Client) -> str:
    schema_context = get_schema_context()

    system_prompt = f"""You are a SQL generator for the FinStream financial data platform.
You only have access to these tables (schema.table: columns):

{schema_context}

Rules:
- Generate exactly ONE read-only SQL SELECT query (or WITH ... SELECT) that answers the question.
- Reference tables using exactly `schema.table` format (e.g. `gold.dim_merchant`). Do NOT include a catalog prefix — the connection is already scoped to the correct catalog.
- Only reference tables listed above. Never reference any other schema.
- Never generate INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, MERGE, TRUNCATE, or any write/DDL statement.
- Return ONLY the SQL query, no explanation, no markdown formatting.
"""

    response = _call_with_retry(lambda: client.models.generate_content(
        model=GEMINI_MODEL,
        contents=question,
        config=types.GenerateContentConfig(
            system_instruction=system_prompt,
            temperature=0,
        ),
    ))
    return _extract_sql(response.text)


def validate_sql(sql: str) -> None:
    """
    Raises UnsafeSQLError if the SQL is not a safe, read-only, allowed-schema
    query. This is the enforcement point — generate_sql's prompt instructions
    are a strong hint to the model, not a guarantee, so we check independently.
    """
    stripped = sql.strip().lower()

    if not (stripped.startswith("select") or stripped.startswith("with")):
        raise UnsafeSQLError("Generated SQL must start with SELECT or WITH.")

    tokens = set(_TOKEN_RE.findall(stripped))
    forbidden_found = tokens & _FORBIDDEN_KEYWORDS
    if forbidden_found:
        raise UnsafeSQLError(f"Generated SQL contains forbidden keyword(s): {forbidden_found}")

    # Require the schema component of every table reference (after FROM/JOIN)
    # to be in the allowlist. Handles both 2-part (schema.table) and 3-part
    # (catalog.schema.table) references — Unity Catalog typically uses
    # 3-part names. Scoped to FROM/JOIN specifically so we don't misfire on
    # alias.column references elsewhere in the query (e.g. `m.merchant_name`
    # where `m` is a table alias, not a schema).
    table_refs = re.findall(
        r"\b(?:from|join)\s+((?:[a-zA-Z_][a-zA-Z0-9_]*\.){1,2}[a-zA-Z_][a-zA-Z0-9_]*)",
        sql,
        re.IGNORECASE,
    )
    disallowed = set()
    for ref in table_refs:
        parts = ref.split(".")
        if len(parts) < 2:
            continue  # unqualified table name, nothing to check
        schema_part = parts[-2]  # segment right before the table name
        if schema_part.lower() not in [s.lower() for s in ALLOWED_SCHEMAS]:
            disallowed.add(schema_part)
    if disallowed:
        raise UnsafeSQLError(
            f"Generated SQL references schema(s) outside the allowlist {ALLOWED_SCHEMAS}: {disallowed}"
        )


def run_query(sql: str) -> pd.DataFrame:
    with _get_databricks_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=columns)


def explain_results(question: str, sql: str, result_df: pd.DataFrame, client: genai.Client) -> str:
    preview = result_df.head(20).to_csv(index=False)
    prompt = f"""Question asked: {question}
SQL executed: {sql}
Result preview (up to 20 rows, CSV):
{preview}

In 2-3 sentences, give a plain-English answer to the question based on this data.
Be specific with numbers where relevant. Do not restate the SQL."""

    response = _call_with_retry(lambda: client.models.generate_content(
        model=GEMINI_MODEL,
        contents=prompt,
        config=types.GenerateContentConfig(temperature=0.2),
    ))
    return response.text.strip()


def answer_question(question: str) -> NLToSQLResult:
    client = genai.Client()  # reads GEMINI_API_KEY from env

    sql = generate_sql(question, client)
    validate_sql(sql)
    result_df = run_query(sql)
    explanation = explain_results(question, sql, result_df, client)

    return NLToSQLResult(
        question=question,
        sql=sql,
        result_df=result_df,
        explanation=explanation,
    )