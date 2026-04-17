# Text-to-SQL Sidecar

A secure, multi-database, AI-powered text-to-SQL API and UI for Apache Superset and beyond.

## Features
- Connects to multiple databases (Postgres, MySQL, BigQuery, etc.)
- Secure SQL validation (read-only, table whitelist, enforced LIMIT)
- Schema reflection and caching per database
- FastAPI backend with REST endpoints
- Streamlit frontend for rapid prototyping
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
- `POST /query` — Validate and execute a query (body: `{db_key, question}`)

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
