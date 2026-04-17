from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any
from text_to_sql_sidecar.db_registry import list_databases, get_db_uri
from text_to_sql_sidecar.validator import validate_sql, set_allowed_tables
from text_to_sql_sidecar.schema_cache import get_schema
#from text_to_sql_sidecar.executor import execute_query  # To be implemented
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

set_allowed_tables({
    "postgres-local": {"your_table1", "your_table2"},
})

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
        # results = execute_query(validated_sql, req.db_key)  # To be implemented
        results = [{"demo": "result"}]
        return {"sql": validated_sql, "results": results}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
