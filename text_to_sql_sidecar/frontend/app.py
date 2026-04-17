import streamlit as st
import requests

API_URL = "http://localhost:8000"

# Page configuration
st.set_page_config(page_title="Text-to-SQL Sidecar", layout="wide", initial_sidebar_state="expanded")

# Custom CSS styling
st.markdown("""
<style>
    [data-testid="stAppViewContainer"] {
        background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
    }
    .main {
        background-color: #f8f9fa;
    }
    h1 {
        color: #ffffff;
        text-align: center;
        padding: 20px;
        font-size: 2.5em;
        font-weight: bold;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 10px;
        margin-bottom: 30px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .section-header {
        color: #2c3e50;
        font-size: 1.3em;
        font-weight: bold;
        margin-top: 20px;
        margin-bottom: 15px;
        padding: 10px;
        border-left: 4px solid #667eea;
        background-color: #f0f4ff;
        border-radius: 5px;
    }
    .table-card {
        background-color: #ffffff;
        border-radius: 8px;
        padding: 15px;
        margin: 10px 0;
        border-left: 5px solid #667eea;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
    }
    .table-name {
        color: #667eea;
        font-weight: bold;
        font-size: 1.1em;
        margin-bottom: 8px;
    }
    .table-columns {
        color: #555;
        font-size: 0.95em;
        line-height: 1.6;
    }
</style>
""", unsafe_allow_html=True)

# Title
st.markdown("# 🚀 Text-to-SQL Sidecar")

# Sidebar
with st.sidebar:
    st.markdown("## ⚙️ Configuration")
    st.info("Select a database and ask questions in natural language. The AI will generate SQL for you.")

try:
    dbs = requests.get(f"{API_URL}/databases").json()
    db_keys = list(dbs.keys())
except Exception as e:
    st.error(f"❌ Failed to fetch databases: {e}")
    st.stop()

# Database selector
col1, col2 = st.columns([2, 1])
with col1:
    selected_db = st.selectbox("📊 Select a database", db_keys, help="Choose a database to query")
with col2:
    st.empty()

# Show tables and schema
if selected_db:
    try:
        tables = requests.get(f"{API_URL}/tables", params={"db": selected_db}).json()["tables"]
    except Exception as e:
        st.error(f"❌ Failed to fetch tables: {e}")
        tables = {}
    
    # Display tables in cards
    st.markdown("<div class='section-header'>📋 Tables and Columns</div>", unsafe_allow_html=True)
    
    if tables:
        col1, col2 = st.columns(2)
        for idx, (table, cols) in enumerate(tables.items()):
            with col1 if idx % 2 == 0 else col2:
                st.markdown(f"""
                <div class='table-card'>
                    <div class='table-name'>📁 {table}</div>
                    <div class='table-columns'>
                        {', '.join([f'<code>{col}</code>' for col in cols])}
                    </div>
                </div>
                """, unsafe_allow_html=True)
    else:
        st.warning("No tables found in this database.")

    # Query input section
    st.markdown("<div class='section-header'>❓ Ask a Question</div>", unsafe_allow_html=True)
    
    question = st.text_area(
        "Enter your query",
        placeholder="e.g., Show me all products with their prices\nor: SELECT * FROM sales_ LIMIT 10",
        height=100,
        help="Write in natural language or SQL"
    )
    
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        submit_btn = st.button("🔍 Submit Query", use_container_width=True)
    with col2:
        st.empty()
    
    if submit_btn and question:
        with st.spinner("⏳ Generating SQL and fetching results..."):
            payload = {"db_key": selected_db, "question": question}
            try:
                resp = requests.post(f"{API_URL}/query", json=payload)
                data = resp.json()
                if resp.status_code == 200:
                    st.markdown("<div class='section-header'>✅ Generated SQL</div>", unsafe_allow_html=True)
                    st.code(data["sql"], language="sql")
                    
                    st.markdown("<div class='section-header'>📊 Results</div>", unsafe_allow_html=True)
                    if data["results"]:
                        st.dataframe(data["results"], use_container_width=True)
                    else:
                        st.info("Query executed successfully, but no results returned.")
                else:
                    st.error(f"❌ Error: {data.get('detail', 'Unknown error')}")
            except Exception as e:
                st.error(f"❌ Query failed: {e}")
