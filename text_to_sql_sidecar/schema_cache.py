

from sqlalchemy import create_engine, inspect
from functools import lru_cache
from typing import Dict, Optional
from text_to_sql_sidecar.db_registry import get_db_uri


def _skip_system_schema(dialect: str, schema_name: str) -> bool:
    if dialect == "postgresql" and schema_name in ("information_schema", "pg_catalog"):
        return True
    if dialect == "mysql" and schema_name in ("information_schema", "mysql", "performance_schema", "sys"):
        return True
    if dialect == "mssql" and schema_name in ("INFORMATION_SCHEMA", "sys"):
        return True
    if dialect == "oracle" and schema_name in ("SYS", "SYSTEM"):
        return True
    return False


@lru_cache(maxsize=10)
def get_engine_dialect(db_key: str) -> str:
    """Returns SQLAlchemy dialect name for db_key (e.g. 'postgresql', 'mysql', 'sqlite')."""
    uri = get_db_uri(db_key)
    engine = create_engine(uri)
    try:
        return engine.dialect.name
    finally:
        engine.dispose()


def invalidate_schema_cache(db_key: Optional[str] = None):
    """Clear all schema caches. Call after schema changes to avoid stale cached data."""
    get_schema.cache_clear()
    get_schema_with_types.cache_clear()
    get_engine_dialect.cache_clear()


@lru_cache(maxsize=10)
def get_schema(db_key: str) -> Dict[str, Dict[str, list]]:
    """
    Returns: {schema_name: {table_name: [columns...]}}
    Compatible with PostgreSQL, MySQL, SQLite, SQL Server, Oracle.
    """
    uri = get_db_uri(db_key)
    engine = create_engine(uri)
    try:
        inspector = inspect(engine)
        dialect = engine.dialect.name
        schema_dict = {}

        if dialect == "sqlite":
            tables = {}
            for table in inspector.get_table_names():
                cols = [col["name"] for col in inspector.get_columns(table)]
                tables[table] = cols
            schema_dict["main"] = tables
            return schema_dict

        for schema_name in inspector.get_schema_names():
            if _skip_system_schema(dialect, schema_name):
                continue
            tables = {}
            for table in inspector.get_table_names(schema=schema_name):
                cols = [col["name"] for col in inspector.get_columns(table, schema=schema_name)]
                tables[table] = cols
            if tables:
                schema_dict[schema_name] = tables
        return schema_dict
    finally:
        engine.dispose()


@lru_cache(maxsize=10)
def get_schema_with_types(db_key: str) -> Dict[str, Dict[str, dict]]:
    """
    Returns: {schema_name: {table_name: {col_name: col_type}}}
    Compatible with PostgreSQL, MySQL, SQLite, SQL Server, Oracle.
    """
    uri = get_db_uri(db_key)
    engine = create_engine(uri)
    try:
        inspector = inspect(engine)
        dialect = engine.dialect.name
        schema_dict = {}

        if dialect == "sqlite":
            tables = {}
            for table in inspector.get_table_names():
                cols = {col["name"]: str(col["type"]) for col in inspector.get_columns(table)}
                tables[table] = cols
            schema_dict["main"] = tables
            return schema_dict

        for schema_name in inspector.get_schema_names():
            if _skip_system_schema(dialect, schema_name):
                continue
            tables = {}
            for table in inspector.get_table_names(schema=schema_name):
                cols = {col["name"]: str(col["type"]) for col in inspector.get_columns(table, schema=schema_name)}
                tables[table] = cols
            if tables:
                schema_dict[schema_name] = tables
        return schema_dict
    finally:
        engine.dispose()
