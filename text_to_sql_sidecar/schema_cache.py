
from sqlalchemy import create_engine, inspect
from functools import lru_cache
from typing import Dict, Any
from text_to_sql_sidecar.db_registry import get_db_uri

@lru_cache(maxsize=10)
def get_schema(db_key: str) -> Dict[str, Dict[str, list]]:
    """
    Returns: {schema_name: {table_name: [columns...]}}
    """
    uri = get_db_uri(db_key)
    engine = create_engine(uri)
    inspector = inspect(engine)
    schema_dict = {}
    for schema_name in inspector.get_schema_names():
        # Optionally skip system schemas
        if schema_name in ("information_schema", "pg_catalog"): continue
        tables = {}
        for table in inspector.get_table_names(schema=schema_name):
            cols = [col["name"] for col in inspector.get_columns(table, schema=schema_name)]
            tables[table] = cols
        if tables:
            schema_dict[schema_name] = tables
    return schema_dict

@lru_cache(maxsize=10)
def get_schema_with_types(db_key: str) -> Dict[str, Dict[str, dict]]:
    """
    Returns: {schema_name: {table_name: {col_name: col_type}}}
    """
    uri = get_db_uri(db_key)
    engine = create_engine(uri)
    inspector = inspect(engine)
    schema_dict = {}
    for schema_name in inspector.get_schema_names():
        if schema_name in ("information_schema", "pg_catalog"): continue
        tables = {}
        for table in inspector.get_table_names(schema=schema_name):
            cols = {col["name"]: str(col["type"]) for col in inspector.get_columns(table, schema=schema_name)}
            tables[table] = cols
        if tables:
            schema_dict[schema_name] = tables
    return schema_dict
