from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any
from text_to_sql_sidecar.db_registry import list_databases, get_db_uri
from text_to_sql_sidecar.validator import validate_sql, set_allowed_tables
from text_to_sql_sidecar.schema_cache import get_schema
from text_to_sql_sidecar.executor import execute_query
from text_to_sql_sidecar.llm_client import generate_sql

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Load allowed tables from actual database schemas
allowed_tables = {}
for db_key in list_databases().keys():
    try:
        schema = get_schema(db_key)
        allowed_tables[db_key] = set(schema.keys())
    except Exception as e:
        print(f"[WARN] Could not load schema for {db_key}: {e}")

set_allowed_tables(allowed_tables)

class QueryRequest(BaseModel):
    db_key: str
    question: str
    sql: str = None

@app.get("/databases")
def get_databases():
    return list_databases()

@app.get("/tables")
def get_tables(db: str = Query(..., description="Database key")):
    try:
        schema = get_schema(db)
        print(f"[DEBUG] Schema for {db}: {schema}")
        return {"tables": schema}
    except Exception as e:
        import traceback
        print(f"[ERROR] Exception in /tables: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/query")
def post_query(req: QueryRequest):
    try:
        if req.sql:
            sql = req.sql
        else:
            schema = get_schema(req.db_key)
            schema_str = "\n".join([f"Table {t}: {', '.join(cols)}" for t, cols in schema.items()])
            sql = generate_sql(schema_str, req.question)
        validated_sql = validate_sql(sql, req.db_key)
        results = execute_query(req.db_key, validated_sql)
        return {"sql": validated_sql, "results": results}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
