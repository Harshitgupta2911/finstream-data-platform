"""
FinStream NL-to-SQL — Streamlit interface.

Run with:
    streamlit run monitoring/streamlit_app/app.py
"""

import streamlit as st

from finstream_data_platform.NL_to_SQL.nl_to_sql import UnsafeSQLError, answer_question

st.set_page_config(page_title="FinStream — Ask Your Data", layout="wide")

st.title("FinStream — Ask Your Data")
st.caption("Ask a question in plain English about transactions, customers, or merchants.")

example_questions = [
    "Which merchants had the highest payment failure rate this week?",
    "What is the total transaction volume by day for the last 30 days?",
    "Who are the top 10 customers by total spend?",
]
st.markdown("**Try asking:** " + " · ".join(f"_{q}_" for q in example_questions))

question = st.text_input("Your question", placeholder="e.g. " + example_questions[0])
submitted = st.button("Ask", type="primary")

if submitted and question.strip():
    with st.spinner("Generating SQL and querying FinStream..."):
        try:
            result = answer_question(question)
        except UnsafeSQLError as e:
            st.error(f"Generated SQL was rejected for safety reasons: {e}")
        except Exception as e:
            st.error(f"Something went wrong: {e}")
        else:
            st.subheader("Answer")
            st.write(result.explanation)

            with st.expander("Generated SQL"):
                st.code(result.sql, language="sql")

            st.subheader("Results")
            st.dataframe(result.result_df, use_container_width=True)

elif submitted:
    st.warning("Enter a question first.")