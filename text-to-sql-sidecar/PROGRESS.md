# Progress Documentation: Text-to-SQL Sidecar

## Overview
This document tracks the development progress of the text-to-sql-sidecar project, which provides a secure, multi-database, AI-powered text-to-SQL API and UI for Apache Superset and similar platforms.

---

## Completed Steps

### 1. Project Structure & Planning
- Defined a modular architecture for multi-database, secure text-to-SQL translation.
- Outlined a clear project structure with backend (FastAPI), schema caching, SQL validation, and a frontend UI.

### 2. Backend Implementation
- **db_registry.py**: Loads and manages database URIs from environment variables (.env), supports multiple DBs.
- **validator.py**: Implements strict SQL validation using sqlglot, enforcing SELECT-only, table whitelisting, and LIMIT.
- **schema_cache.py**: Reflects and caches table/column schemas per database using SQLAlchemy and lru_cache.
- **main.py**: FastAPI app exposing endpoints:
  - `/databases`: List all registered databases
  - `/tables?db=...`: List tables/columns for a selected database (now returns real schema)
  - `/query`: Validates and (stub) executes queries, returns SQL and placeholder results

### 3. Frontend Implementation
- **frontend/app.py**: Streamlit UI for rapid prototyping:
  - Lets user select a database
  - Displays tables and columns
  - Accepts natural language or SQL queries
  - Shows generated SQL and results from backend

### 4. Documentation
- **README.md**: Created with setup instructions, project structure, API endpoints, and security notes.
- **.gitignore**: Updated to exclude venv and other unnecessary files.

---

## Next Steps
- Implement executor.py for secure, read-only SQL execution
- Integrate LLM for text-to-SQL conversion
- Expand prompt_builder.py and llm_client.py as needed
- Upgrade frontend to React for production use

---

_Last updated: April 17, 2026_
