

from sqlalchemy import create_engine, inspect
from functools import lru_cache
from typing import Dict, Any
from text_to_sql_sidecar.db_registry import get_db_uri

@lru_cache(maxsize=10)
def get_schema(db_key: str) -> Dict[str, Dict[str, list]]:
    """
    Returns: {schema_name: {table_name: [columns...]}}
    Compatible with PostgreSQL, MySQL, SQLite, SQL Server, Oracle.
    """
    uri = get_db_uri(db_key)
    engine = create_engine(uri)
    inspector = inspect(engine)
    dialect = engine.dialect.name
    schema_dict = {}

    if dialect == "sqlite":
        # SQLite: no schemas, just tables
        tables = {}
        for table in inspector.get_table_names():
            cols = [col["name"] for col in inspector.get_columns(table)]
            tables[table] = cols
        schema_dict["main"] = tables
        return schema_dict

    # For MySQL, treat each database as a schema
    if dialect == "mysql":
        for schema_name in inspector.get_schema_names():
            # Optionally skip system schemas
            if schema_name in ("information_schema", "mysql", "performance_schema", "sys"): continue
            tables = {}
            for table in inspector.get_table_names(schema=schema_name):
                cols = [col["name"] for col in inspector.get_columns(table, schema=schema_name)]
                tables[table] = cols
            if tables:
                schema_dict[schema_name] = tables
        return schema_dict

    # For PostgreSQL, SQL Server, Oracle (schemas supported)
    for schema_name in inspector.get_schema_names():
        # Skip system schemas for each DB
        if dialect == "postgresql" and schema_name in ("information_schema", "pg_catalog"): continue
        if dialect == "mssql" and schema_name in ("INFORMATION_SCHEMA", "sys"): continue
        if dialect == "oracle" and schema_name in ("SYS", "SYSTEM"): continue
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
    Compatible with PostgreSQL, MySQL, SQLite, SQL Server, Oracle.
    """
    uri = get_db_uri(db_key)
    engine = create_engine(uri)
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

    if dialect == "mysql":
        for schema_name in inspector.get_schema_names():
            if schema_name in ("information_schema", "mysql", "performance_schema", "sys"): continue
            tables = {}
            for table in inspector.get_table_names(schema=schema_name):
                cols = {col["name"]: str(col["type"]) for col in inspector.get_columns(table, schema=schema_name)}
                tables[table] = cols
            if tables:
                schema_dict[schema_name] = tables
        return schema_dict

    for schema_name in inspector.get_schema_names():
        if dialect == "postgresql" and schema_name in ("information_schema", "pg_catalog"): continue
        if dialect == "mssql" and schema_name in ("INFORMATION_SCHEMA", "sys"): continue
        if dialect == "oracle" and schema_name in ("SYS", "SYSTEM"): continue
        tables = {}
        for table in inspector.get_table_names(schema=schema_name):
            cols = {col["name"]: str(col["type"]) for col in inspector.get_columns(table, schema=schema_name)}
            tables[table] = cols
        if tables:
            schema_dict[schema_name] = tables
    return schema_dict
