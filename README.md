# Text-to-SQL Sidecar

A secure, multi-database, AI-powered text-to-SQL API and UI for Apache Superset and beyond.


## Features
- Connects to multiple databases (Postgres, MySQL, BigQuery, etc.)
- Secure SQL validation (read-only, table whitelist, enforced LIMIT)
- Schema reflection and caching per database
- FastAPI backend with REST endpoints
- React frontend for production (with schema explorer)
- Hybrid schema selection: choose a specific schema or "All" for broad search
- Query timer: see how long each query takes to generate and retrieve
- Ready for LLM integration (Ollama, SQLCoder, etc.)

## Project Structure
```
text-to-sql-sidecar/
├── .env                  # DB connection strings and secrets
├── db_registry.py        # Loads and manages DB URIs
├── schema_cache.py       # Reflects and caches DB schemas
├── validator.py          # Strict SQL validation (sqlglot)
├── main.py               # FastAPI app (API endpoints)
├── executor.py           # (To be implemented) Executes validated SQL
├── prompt_builder.py     # (Optional) Builds LLM prompts with schema
├── llm_client.py         # (Optional) Handles LLM calls
└── frontend/
    └── app.py            # Streamlit UI
```

## Quickstart
1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   pip install streamlit fastapi uvicorn sqlalchemy sqlglot python-dotenv requests
   ```
2. **Configure your databases:**
   - Add DB URIs to `.env` as `DB_PROD_WAREHOUSE=...`, `DB_CRM_DB=...`, etc.
3. **Run the backend:**
   ```bash
   uvicorn main:app --reload
   ```
4. **Run the frontend:**
   ```bash
   streamlit run frontend/app.py
   ```
5. **Open the UI:**
   - Go to `http://localhost:8501` in your browser.


## API Endpoints
- `GET  /databases` — List all registered databases
- `GET  /tables?db=...` — List tables/columns for a database
- `POST /query` — Generate and execute a query (body: `{db_key, question, schema?}`)
   - Optional `schema` parameter: If provided, only that schema is used for prompt generation. If omitted or set to "All", all schemas are considered (with relevance filtering).


## UI Usage
- **Schema Explorer:** Use the dropdown to select a schema or "All". Only table names are shown for clarity.
- **Hybrid Search:**
   - If a schema is selected, only that schema is used for prompt generation (faster, more focused).
   - If "All" is selected, the system searches across all schemas (slower, but comprehensive).
- **Query Timer:** The UI displays the time taken for each query, helping you evaluate retrieval speed.

## Security
- Only allows SELECT queries
- Only whitelisted tables per DB
- Enforces LIMIT on all queries
- Read-only DB connections where possible

## Next Steps
- Integrate LLM for text-to-SQL
- Implement executor.py for query execution
- Upgrade frontend to React for production

---
MIT License
