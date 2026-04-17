from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List, Dict, Any

from text-to-sql-sidecar.db_registry import list_databases, get_db_uri
from text-to-sql-sidecar.validator import validate_sql, set_allowed_tables
from text-to-sql-sidecar.schema_cache import get_schema
# from text-to-sql-sidecar.executor import execute_query  # To be implemented

app = FastAPI()

# Dummy allowed tables for demo; replace with dynamic logic as needed
set_allowed_tables({
    "prod-warehouse": {"product_master", "inventory", "orders"},
    "crm-db": {"customers", "contacts"},
})

class QueryRequest(BaseModel):
    db_key: str
    question: str
    sql: str = None  # Optionally allow direct SQL for testing

@app.get("/databases")
def get_databases():
    """List all registered databases."""
    return list_databases()


@app.get("/tables")
def get_tables(db: str = Query(..., description="Database key")):
    """List tables and columns for a given database (real schema)."""
    try:
        schema = get_schema(db)
        return {"tables": schema}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/query")
def post_query(req: QueryRequest):
    """Validate and execute a query (stub)."""
    try:
        sql = req.sql or f"SELECT * FROM {req.question}"  # Placeholder for LLM
        validated_sql = validate_sql(sql, req.db_key)
        # results = execute_query(validated_sql, req.db_key)  # To be implemented
        results = [{"demo": "result"}]  # Placeholder
        return {"sql": validated_sql, "results": results}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
