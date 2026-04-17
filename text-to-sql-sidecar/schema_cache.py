from sqlalchemy import create_engine, inspect
from functools import lru_cache
from typing import Dict, Any
from text-to-sql-sidecar.db_registry import get_db_uri

@lru_cache(maxsize=10)
def get_schema(db_key: str) -> Dict[str, Any]:
    """
    Reflects and caches the schema for the given database key.
    Returns a dict: {table_name: [col1, col2, ...], ...}
    """
    uri = get_db_uri(db_key)
    engine = create_engine(uri)
    inspector = inspect(engine)
    schema = {}
    for table in inspector.get_table_names():
        cols = [col["name"] for col in inspector.get_columns(table)]
        schema[table] = cols
    return schema
