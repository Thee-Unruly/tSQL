from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any
from text_to_sql_sidecar.db_registry import list_databases, get_db_uri
from text_to_sql_sidecar.validator import validate_sql, set_allowed_tables
from text_to_sql_sidecar.schema_cache import get_schema, get_schema_with_types
from text_to_sql_sidecar.executor import execute_query
from text_to_sql_sidecar.llm_client import generate_sql, generate_sql_with_reasoning
from text_to_sql_sidecar.schema_filter import filter_schema_by_relevance

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Load allowed tables from actual database schemas (schema.table format)
allowed_tables = {}
for db_key in list_databases().keys():
    try:
        schema = get_schema(db_key)  # {schema: {table: [...]}}
        allowed = set()
        for schema_name, tables in schema.items():
            for table_name in tables.keys():
                allowed.add(f"{schema_name.lower()}.{table_name.lower()}")
        allowed_tables[db_key] = allowed
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
        # schema is already {schema: {table: [columns...]}}
        return {"schemas": schema}
    except Exception as e:
        import traceback
        print(f"[ERROR] Exception in /tables: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/query")
def post_query(req: QueryRequest):
    try:
        reasoning = None
        if req.sql:
            sql = req.sql
        else:
            schema = get_schema(req.db_key)
            schema_types = get_schema_with_types(req.db_key)
            filtered_schema = filter_schema_by_relevance(req.question, schema)
            # Build schema string with schema and table names for LLM
            schema_lines = []
            for schema_name, tables in filtered_schema.items():
                for table_name, cols in tables.items():
                    col_type_map = schema_types.get(schema_name, {}).get(table_name, {})
                    col_strs = [f"{c} ({col_type_map.get(c, 'unknown')})" for c in cols]
                    schema_lines.append(f"Schema {schema_name}, Table {table_name}: {', '.join(col_strs)}")
            schema_str = "\n".join(schema_lines)
            reasoning, sql = generate_sql_with_reasoning(schema_str, req.question)
            print(f"[DEBUG] Reasoning: {reasoning}")
        validated_sql = validate_sql(sql, req.db_key)
        results = execute_query(req.db_key, validated_sql)
        response = {"sql": validated_sql, "results": results}
        if reasoning:
            response["reasoning"] = reasoning
        return response
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
