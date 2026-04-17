from sqlalchemy import create_engine, inspect
from functools import lru_cache
from typing import Dict, Any
from text_to_sql_sidecar.db_registry import get_db_uri

@lru_cache(maxsize=10)
def get_schema(db_key: str) -> Dict[str, Any]:
    uri = get_db_uri(db_key)
    engine = create_engine(uri)
    inspector = inspect(engine)
    schema = {}
    for table in inspector.get_table_names():
        cols = [col["name"] for col in inspector.get_columns(table)]
        schema[table] = cols
    return schema

@lru_cache(maxsize=10)
def get_schema_with_types(db_key: str) -> Dict[str, Any]:
    """Returns schema with column types included."""
    uri = get_db_uri(db_key)
    engine = create_engine(uri)
    inspector = inspect(engine)
    schema = {}
    for table in inspector.get_table_names():
        cols = {col["name"]: str(col["type"]) for col in inspector.get_columns(table)}
        schema[table] = cols
    return schema
