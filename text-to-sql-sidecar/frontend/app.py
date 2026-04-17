import streamlit as st
import requests

API_URL = "http://localhost:8000"  # Adjust if backend runs elsewhere

st.title("Text-to-SQL Sidecar UI")

# Fetch databases
try:
    dbs = requests.get(f"{API_URL}/databases").json()
    db_keys = list(dbs.keys())
except Exception as e:
    st.error(f"Failed to fetch databases: {e}")
    st.stop()

selected_db = st.selectbox("Select a database", db_keys)

# Fetch tables for selected DB
if selected_db:
    try:
        tables = requests.get(f"{API_URL}/tables", params={"db": selected_db}).json()["tables"]
    except Exception as e:
        st.error(f"Failed to fetch tables: {e}")
        tables = {}
    st.write("### Tables and columns:")
    for table, cols in tables.items():
        st.write(f"- **{table}**: {', '.join(cols)}")

    question = st.text_input("Ask a question (natural language or SQL)")
    if st.button("Submit Query") and question:
        with st.spinner("Querying..."):
            payload = {"db_key": selected_db, "question": question}
            try:
                resp = requests.post(f"{API_URL}/query", json=payload)
                data = resp.json()
                if resp.status_code == 200:
                    st.code(data["sql"], language="sql")
                    st.write("### Results:")
                    st.write(data["results"])
                else:
                    st.error(data.get("detail", "Unknown error"))
            except Exception as e:
                st.error(f"Query failed: {e}")
