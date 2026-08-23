"""
FinStream — AI Incident Summary Dashboard.

Architecture:

    Databricks Gold
          |
          v
    gold.fact_transaction
          |
          v
    Isolation Forest
          |
          v
    Flagged anomalies
          |
          v
    Incident Context
          |
          v
        Gemini
          |
          v
    AI Incident Summary
"""

from __future__ import annotations

import os

import pandas as pd
import streamlit as st
from databricks import sql
from dotenv import load_dotenv
from sklearn.ensemble import IsolationForest

from finstream_data_platform.Ai_incident_summerizer.incident_summarizer import (
    summarize_anomaly_row,
)


# ---------------------------------------------------------
# Environment
# ---------------------------------------------------------

load_dotenv()


# ---------------------------------------------------------
# Streamlit configuration
# ---------------------------------------------------------

st.set_page_config(
    page_title="FinStream — AI Incident Summary",
    layout="wide",
)


# ---------------------------------------------------------
# Header
# ---------------------------------------------------------

st.title("AI Incident Summary")

st.caption(
    "Isolation Forest detects unusual transaction behaviour. "
    "Gemini explains the detected anomaly using real FinStream data."
)


# ---------------------------------------------------------
# Databricks connection
# ---------------------------------------------------------

def get_databricks_connection():
    """
    Create a Databricks SQL connection using credentials
    stored in the .env file.

    Supported hostname variables:
        DATABRICKS_SERVER_HOSTNAME
        DATABRICKS_HOST
    """

    server_hostname = os.getenv(
        "DATABRICKS_SERVER_HOSTNAME"
    )

    if not server_hostname:
        server_hostname = os.getenv(
            "DATABRICKS_HOST"
        )

    http_path = os.getenv(
        "DATABRICKS_HTTP_PATH"
    )

    access_token = os.getenv(
        "DATABRICKS_TOKEN"
    )

    missing = []

    if not server_hostname:
        missing.append(
            "DATABRICKS_SERVER_HOSTNAME"
        )

    if not http_path:
        missing.append(
            "DATABRICKS_HTTP_PATH"
        )

    if not access_token:
        missing.append(
            "DATABRICKS_TOKEN"
        )

    if missing:

        raise RuntimeError(
            "Missing Databricks environment variables: "
            + ", ".join(missing)
        )

    return sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token,
    )


# ---------------------------------------------------------
# Load Gold transaction data
# ---------------------------------------------------------

@st.cache_data(ttl=300)
def load_transaction_data() -> pd.DataFrame:
    """
    Read the FinStream Gold fact_transaction table.
    """

    connection = get_databricks_connection()

    try:

        query = """
            SELECT *
            FROM finstream_data_pipeline.gold.fact_transaction
        """

        df = pd.read_sql(
            query,
            connection,
        )

        return df

    finally:

        connection.close()


# ---------------------------------------------------------
# Load data
# ---------------------------------------------------------

try:

    with st.spinner(
        "Loading Gold transaction data from Databricks..."
    ):

        data = load_transaction_data()

except Exception as e:

    st.error(
        "Unable to load Gold transaction data from Databricks."
    )

    st.exception(e)

    st.stop()


# ---------------------------------------------------------
# Validate data
# ---------------------------------------------------------

if data.empty:

    st.warning(
        "gold.fact_transaction contains no records."
    )

    st.stop()


st.success(
    f"Loaded {len(data):,} transaction records from Databricks Gold."
)


# ---------------------------------------------------------
# Dataset information
# ---------------------------------------------------------

with st.expander(
    "Dataset information"
):

    col1, col2, col3 = st.columns(3)

    with col1:

        st.metric(
            "Rows",
            f"{len(data):,}",
        )

    with col2:

        st.metric(
            "Columns",
            len(data.columns),
        )

    with col3:

        st.metric(
            "Numeric columns",
            sum(
                pd.api.types.is_numeric_dtype(
                    data[column]
                )
                for column in data.columns
            ),
        )

    st.write(
        "Gold table: `gold.fact_transaction`"
    )

    st.write(
        "Columns:"
    )

    st.write(
        list(data.columns)
    )


# ---------------------------------------------------------
# Identify numeric columns
# ---------------------------------------------------------

numeric_columns = [
    column
    for column in data.columns
    if pd.api.types.is_numeric_dtype(
        data[column]
    )
]


# ---------------------------------------------------------
# Remove ID-like columns
# ---------------------------------------------------------

def is_id_like(column: str) -> bool:

    name = column.lower()

    id_keywords = [
        "id",
        "key",
    ]

    return any(
        keyword in name
        for keyword in id_keywords
    )


recommended_columns = [
    column
    for column in numeric_columns
    if not is_id_like(column)
]


# If everything numeric looks like an ID,
# allow the user to choose manually.
if not recommended_columns:

    recommended_columns = numeric_columns


# ---------------------------------------------------------
# Feature selection
# ---------------------------------------------------------

st.subheader(
    "1. Select transaction features"
)

selected_columns = st.multiselect(
    "Numeric features used by Isolation Forest",
    options=numeric_columns,
    default=recommended_columns[
        : min(3, len(recommended_columns))
    ],
    help=(
        "Avoid ID/key columns because they usually do not "
        "represent transaction behaviour."
    ),
)


if not selected_columns:

    st.info(
        "Select at least one numeric feature."
    )

    st.stop()


# ---------------------------------------------------------
# Prepare detection dataset
# ---------------------------------------------------------

detection_df = data[
    selected_columns
].copy()


# Replace infinite values
detection_df = detection_df.replace(
    [float("inf"), float("-inf")],
    pd.NA,
)


# Remove rows where all selected features are missing
valid_mask = detection_df.notna().any(
    axis=1
)


working_df = data.loc[
    valid_mask
].copy()


detection_df = detection_df.loc[
    valid_mask
].copy()


# ---------------------------------------------------------
# Fill missing values
# ---------------------------------------------------------

for column in selected_columns:

    detection_df[column] = pd.to_numeric(
        detection_df[column],
        errors="coerce",
    )

    median_value = detection_df[
        column
    ].median()

    if pd.isna(median_value):
        median_value = 0

    detection_df[
        column
    ] = detection_df[
        column
    ].fillna(median_value)


# ---------------------------------------------------------
# Minimum dataset check
# ---------------------------------------------------------

if len(detection_df) < 10:

    st.error(
        "Not enough valid records for Isolation Forest. "
        "At least 10 records are required."
    )

    st.stop()


# ---------------------------------------------------------
# Detection configuration
# ---------------------------------------------------------

st.subheader(
    "2. Anomaly detection settings"
)

contamination = st.slider(
    "Expected anomaly proportion",
    min_value=0.01,
    max_value=0.20,
    value=0.05,
    step=0.01,
    help=(
        "The approximate proportion of transactions "
        "expected to be anomalous."
    ),
)


# ---------------------------------------------------------
# Run Isolation Forest
# ---------------------------------------------------------

if st.button(
    "Run Detection",
    type="primary",
):

    with st.spinner(
        "Running Isolation Forest..."
    ):

        model = IsolationForest(
            n_estimators=200,
            contamination=contamination,
            random_state=42,
        )

        predictions = model.fit_predict(
            detection_df
        )

        anomaly_scores = (
            model.decision_function(
                detection_df
            )
        )

        result = working_df.copy()

        result[
            "anomaly_score"
        ] = anomaly_scores

        result[
            "is_anomaly"
        ] = predictions == -1

        # Lower score = more anomalous.
        result = result.sort_values(
            "anomaly_score",
            ascending=True,
        )

        # Save result for this Streamlit session.
        st.session_state[
            "incident_detection_result"
        ] = result

        st.session_state[
            "incident_detection_features"
        ] = selected_columns

        st.success(
            "Isolation Forest detection completed."
        )


# ---------------------------------------------------------
# Retrieve latest detection
# ---------------------------------------------------------

result = st.session_state.get(
    "incident_detection_result"
)


if result is None:

    st.info(
        "Click **Run Detection** to detect anomalies."
    )

    st.stop()


# ---------------------------------------------------------
# Detection metrics
# ---------------------------------------------------------

st.subheader(
    "3. Detection results"
)

total_records = len(result)

anomaly_count = int(
    result["is_anomaly"].sum()
)

anomaly_rate = (
    anomaly_count / total_records * 100
)


col1, col2, col3 = st.columns(3)


with col1:

    st.metric(
        "Records analysed",
        f"{total_records:,}",
    )


with col2:

    st.metric(
        "Anomalies detected",
        f"{anomaly_count:,}",
    )


with col3:

    st.metric(
        "Anomaly rate",
        f"{anomaly_rate:.2f}%",
    )


# ---------------------------------------------------------
# Flagged anomalies
# ---------------------------------------------------------

flagged = result[
    result["is_anomaly"]
].copy()


if flagged.empty:

    st.success(
        "No anomalies were detected."
    )

    st.stop()


st.subheader(
    "Flagged anomalies"
)

st.caption(
    "Rows are ordered from most anomalous to least anomalous "
    "according to the Isolation Forest score."
)

st.dataframe(
    flagged,
    use_container_width=True,
)


# ---------------------------------------------------------
# Select anomaly
# ---------------------------------------------------------

st.subheader(
    "4. AI investigation"
)


row_index = st.selectbox(
    "Select a flagged transaction",
    options=list(flagged.index),
    format_func=lambda index: (
        f"Row {index} — "
        f"anomaly score: "
        f"{flagged.loc[index, 'anomaly_score']:.4f}"
    ),
)


# ---------------------------------------------------------
# Generate Gemini summary
# ---------------------------------------------------------

if st.button(
    "Generate Incident Summary",
    type="primary",
):

    selected_row = flagged.loc[
        row_index
    ]

    features = st.session_state.get(
        "incident_detection_features",
        selected_columns,
    )

    dimension = (
        "Transaction anomaly based on: "
        + ", ".join(features)
    )

    with st.spinner(
        "Gemini is analysing the detected transaction..."
    ):

        try:

            summary = summarize_anomaly_row(
                dimension=dimension,
                row=selected_row,
                full_df=result,
            )

        except Exception as e:

            st.error(
                "Failed to generate AI incident summary."
            )

            st.exception(e)

        else:

            st.subheader(
                "AI Incident Summary"
            )

            st.info(
                summary
            )


# ---------------------------------------------------------
# Selected transaction details
# ---------------------------------------------------------

with st.expander(
    "View selected transaction details"
):

    selected_row = flagged.loc[
        row_index
    ]

    details = pd.DataFrame(
        {
            "Field": selected_row.index,
            "Value": selected_row.values,
        }
    )

    st.dataframe(
        details,
        use_container_width=True,
        hide_index=True,
    )