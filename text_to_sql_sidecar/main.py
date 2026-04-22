from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any
from text_to_sql_sidecar.db_registry import list_databases, get_db_uri
from text_to_sql_sidecar.validator import validate_sql, set_allowed_tables
from text_to_sql_sidecar.schema_cache import get_schema, get_schema_with_types, get_engine_dialect
from text_to_sql_sidecar.executor import execute_query
from text_to_sql_sidecar.llm_client import generate_sql, generate_sql_with_reasoning, generate_sql_with_reasoning_streaming, generate_sql_with_retry
from text_to_sql_sidecar.schema_filter import filter_schema_by_relevance
import json

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
    schema_name: str = None

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
            # Hybrid logic: if schema param is provided and not 'All', use only that schema
            if req.schema_name and req.schema_name != 'All' and req.schema_name in schema:
                filtered_schema = {req.schema_name: schema[req.schema_name]}
            else:
                # Use all schemas, but filter by relevance to the question
                filtered_schema = filter_schema_by_relevance(req.question, schema)
            # Build schema string with schema and table names for LLM
            schema_lines = []
            for schema_name, tables in filtered_schema.items():
                for table_name, cols in tables.items():
                    col_type_map = schema_types.get(schema_name, {}).get(table_name, {})
                    col_strs = [f"{c} ({col_type_map.get(c, 'unknown')})" for c in cols]
                    schema_lines.append(f"Schema {schema_name}, Table {table_name}: {', '.join(col_strs)}")
            schema_str = "\n".join(schema_lines)
            db_type = get_engine_dialect(req.db_key)
            reasoning, sql = generate_sql_with_retry(
                schema=schema_str,
                question=req.question,
                model="gemma2-9b",
                db_type=db_type,
                db_key=req.db_key,
                schema_name=req.schema_name,
                max_retries=2,
            )
            print(f"[DEBUG] Reasoning: {reasoning}")
        validated_sql = validate_sql(sql, req.db_key, req.schema_name)
        results = execute_query(req.db_key, validated_sql)
        response = {"sql": validated_sql, "results": results}
        if reasoning:
            response["reasoning"] = reasoning
        return response
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/query-stream")
def post_query_stream(req: QueryRequest):
    """
    Streaming endpoint that returns reasoning chunks in real-time via SSE.
    """
    async def event_generator():
        try:
            schema = get_schema(req.db_key)
            schema_types = get_schema_with_types(req.db_key)
            
            # Hybrid logic: if schema param is provided and not 'All', use only that schema
            if req.schema_name and req.schema_name != 'All' and req.schema_name in schema:
                filtered_schema = {req.schema_name: schema[req.schema_name]}
            else:
                # Use all schemas, but filter by relevance to the question
                filtered_schema = filter_schema_by_relevance(req.question, schema)
            
            # Build schema string with schema and table names for LLM
            schema_lines = []
            for schema_name, tables in filtered_schema.items():
                for table_name, cols in tables.items():
                    col_type_map = schema_types.get(schema_name, {}).get(table_name, {})
                    col_strs = [f"{c} ({col_type_map.get(c, 'unknown')})" for c in cols]
                    schema_lines.append(f"Schema {schema_name}, Table {table_name}: {', '.join(col_strs)}")
            schema_str = "\n".join(schema_lines)

            # Use retry loop for validation before streaming results
            db_type = get_engine_dialect(req.db_key)
            reasoning, sql = generate_sql_with_retry(
                schema=schema_str,
                question=req.question,
                model="gemma2-9b",
                db_type=db_type,
                db_key=req.db_key,
                schema_name=req.schema_name,
                max_retries=2,
            )
            yield f"data: {json.dumps({'type': 'reasoning_complete', 'content': reasoning})}\n\n"
            yield f"data: {json.dumps({'type': 'sql_generated', 'content': sql})}\n\n"
            
            # Validate and execute SQL
            try:
                validated_sql = validate_sql(sql, req.db_key, req.schema_name)
                results = execute_query(req.db_key, validated_sql)
                yield f"data: {json.dumps({'type': 'results', 'sql': validated_sql, 'results': results})}\n\n"
            except Exception as e:
                yield f"data: {json.dumps({'type': 'error', 'content': str(e)})}\n\n"
            
            yield "data: {\"type\": \"complete\"}\n\n"
            
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'content': str(e)})}\n\n"
    
    return StreamingResponse(event_generator(), media_type="text/event-stream")
