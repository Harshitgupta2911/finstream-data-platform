"""
FinStream — AI Incident Summarizer.

Isolation Forest detects anomalies.
Gemini explains the detected anomaly using the actual data
and baseline statistics from normal records.

The LLM does NOT perform anomaly detection.
It only explains anomalies that have already been detected.
"""

from __future__ import annotations

import os
import time

import pandas as pd
from dotenv import load_dotenv
from google import genai
from google.genai import types


# ---------------------------------------------------------
# Environment
# ---------------------------------------------------------

load_dotenv()

GEMINI_MODEL = os.getenv(
    "GEMINI_MODEL",
    "gemini-3.6-flash",
)


# ---------------------------------------------------------
# Retry helper
# ---------------------------------------------------------

def _call_with_retry(
    fn,
    max_attempts: int = 3,
    base_delay_seconds: float = 2.0,
):
    """
    Retry transient Gemini API failures.
    """

    last_error = None

    for attempt in range(max_attempts):

        try:
            return fn()

        except Exception as e:

            last_error = e

            is_transient = (
                "503" in str(e)
                or "UNAVAILABLE" in str(e)
                or "overloaded" in str(e).lower()
            )

            if (
                not is_transient
                or attempt == max_attempts - 1
            ):
                raise

            time.sleep(
                base_delay_seconds * (2 ** attempt)
            )

    raise last_error


# ---------------------------------------------------------
# Safe value conversion
# ---------------------------------------------------------

def _safe_value(value):
    """
    Convert pandas/numpy values into normal Python values
    suitable for the Gemini context.
    """

    if pd.isna(value):
        return None

    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass

    if isinstance(
        value,
        (int, float, bool, str),
    ):
        return value

    return str(value)


# ---------------------------------------------------------
# Build incident context
# ---------------------------------------------------------

def build_incident_context(
    dimension: str,
    row: pd.Series,
    full_df: pd.DataFrame,
) -> dict:
    """
    Build grounded context for one flagged anomaly.

    Baseline statistics are calculated only from normal rows.
    """

    normal_rows = full_df[
        ~full_df["is_anomaly"]
    ].copy()

    numeric_cols = [
        column
        for column in full_df.columns
        if column not in (
            "is_anomaly",
            "anomaly_score",
        )
        and pd.api.types.is_numeric_dtype(
            full_df[column]
        )
    ]

    baseline = {}

    for column in numeric_cols:

        series = pd.to_numeric(
            normal_rows[column],
            errors="coerce",
        ).dropna()

        if series.empty:
            continue

        baseline[column] = {
            "mean": round(
                float(series.mean()),
                2,
            ),
            "median": round(
                float(series.median()),
                2,
            ),
            "std": round(
                float(series.std()),
                2,
            ) if len(series) > 1 else 0.0,
        }

    flagged_row = {
        column: _safe_value(value)
        for column, value in row.items()
    }

    flagged_values = {
        column: _safe_value(row[column])
        for column in numeric_cols
        if column in row
    }

    anomaly_score = row.get(
        "anomaly_score",
        0,
    )

    anomaly_score = _safe_value(
        anomaly_score
    )

    if anomaly_score is None:
        anomaly_score = 0.0

    return {
        "dimension": dimension,
        "flagged_row": flagged_row,
        "flagged_values": flagged_values,
        "baseline_from_normal_data": baseline,
        "anomaly_score": round(
            float(anomaly_score),
            4,
        ),
    }


# ---------------------------------------------------------
# Gemini summarizer
# ---------------------------------------------------------

def summarize_incident(
    context: dict,
    client: genai.Client,
) -> str:
    """
    Generate a grounded incident explanation.
    """

    system_prompt = """
You are a senior data engineer explaining a statistically
flagged anomaly in the FinStream financial data platform.

The anomaly was detected by Isolation Forest.

You will receive:

- The dimension being monitored
- The actual flagged row
- Numeric values from that row
- Baseline statistics calculated from NON-ANOMALOUS records
- The Isolation Forest anomaly score

IMPORTANT RULES:

1. Base your explanation ONLY on the supplied data.

2. Do NOT invent causes that are not supported by the
   supplied data.

3. Do NOT claim that an API failure, database failure,
   timeout, fraud event, deployment, or business event
   occurred unless that information is explicitly present.

4. You MAY suggest 1-2 plausible causes, but clearly label
   them as hypotheses to investigate.

5. Quantify the deviation using the supplied baseline
   statistics whenever possible.

6. Explain what changed, how unusual it is, and what should
   be checked next.

7. Keep the response to 3-4 sentences.

8. If the evidence is weak or ambiguous, say so honestly.

9. Do not manufacture a dramatic explanation.
"""

    user_prompt = f"""
Anomaly context:

{context}

Explain this anomaly in plain English for another data engineer.
"""

    response = _call_with_retry(
        lambda: client.models.generate_content(
            model=GEMINI_MODEL,
            contents=user_prompt,
            config=types.GenerateContentConfig(
                system_instruction=system_prompt,
                temperature=0.2,
            ),
        )
    )

    if not response.text:
        raise RuntimeError(
            "Gemini returned an empty response."
        )

    return response.text.strip()


# ---------------------------------------------------------
# Public function
# ---------------------------------------------------------

def summarize_anomaly_row(
    dimension: str,
    row: pd.Series,
    full_df: pd.DataFrame,
) -> str:
    """
    Build the incident context and generate the Gemini summary.
    """

    client = genai.Client()

    context = build_incident_context(
        dimension=dimension,
        row=row,
        full_df=full_df,
    )

    return summarize_incident(
        context=context,
        client=client,
    )