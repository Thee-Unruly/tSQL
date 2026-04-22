import sqlglot
import sqlglot.expressions as exp
from typing import Set, Dict, Optional

# Maps SQLAlchemy dialect names to sqlglot dialect names
_DIALECT_MAP = {
    "postgresql": "postgres",
    "mysql": "mysql",
    "sqlite": "sqlite",
    "mssql": "tsql",
    "oracle": "oracle",
}

def _get_sqlglot_dialect(db_key: str) -> str:
    from text_to_sql_sidecar.schema_cache import get_engine_dialect
    sa_dialect = get_engine_dialect(db_key)
    return _DIALECT_MAP.get(sa_dialect, "postgres")


def qualify_sql_tables(sql: str, db_key: str, schema: Optional[str] = None) -> str:
    """
    Rewrite unqualified table names in SQL to schema-qualified names using schema cache.
    Applies when schema is 'All', a specific schema name, or not specified.
    """
    from text_to_sql_sidecar.schema_cache import get_schema
    sqlglot_dialect = _get_sqlglot_dialect(db_key)
    try:
        parsed = sqlglot.parse_one(sql, dialect=sqlglot_dialect)
        schema_dict = get_schema(db_key)
    except Exception as e:
        print(f"[DEBUG] qualify_sql_tables error: {e}")
        return sql

    # Build a mapping: table_name -> schema_name (first match, warn on ambiguity)
    table_to_schema = {}
    for schema_name, tables in schema_dict.items():
        for table_name in tables.keys():
            if table_name.lower() in table_to_schema:
                print(f"[WARN] Ambiguous table '{table_name}' exists in multiple schemas: "
                      f"'{table_to_schema[table_name.lower()]}' and '{schema_name}'")
            else:
                table_to_schema[table_name.lower()] = schema_name

    print(f"[DEBUG] qualify_sql_tables: schema param={schema}, table_to_schema keys={list(table_to_schema.keys())[:5]}...")

    changed = False
    for t in parsed.find_all(exp.Table):
        table_name_str = t.name if isinstance(t.name, str) else str(t.name)
        table_name_lower = table_name_str.lower()

        if not t.db:
            if schema and schema != 'All':
                t.set('db', schema)
                changed = True
                print(f"[DEBUG] qualify_sql_tables: qualified {table_name_lower} -> {schema}.{table_name_lower} (specific schema)")
            elif schema == 'All' or schema is None:
                if table_name_lower in table_to_schema:
                    schema_name = table_to_schema[table_name_lower]
                    t.set('db', schema_name)
                    changed = True
                    print(f"[DEBUG] qualify_sql_tables: qualified {table_name_lower} -> {schema_name}.{table_name_lower} (from cache)")

    if changed:
        qualified = parsed.sql(dialect=sqlglot_dialect)
        print(f"[DEBUG] qualify_sql_tables output: {qualified}")
        return qualified

    print(f"[DEBUG] qualify_sql_tables: no changes needed")
    return sql


# ALLOWED_TABLES: {db_key: set of allowed schema.table names}
ALLOWED_TABLES: Dict[str, Set[str]] = {}

def set_allowed_tables(allowed: Dict[str, Set[str]]):
    global ALLOWED_TABLES
    ALLOWED_TABLES = allowed

def validate_sql(query: str, db_key: str, schema: Optional[str] = None) -> str:
    sqlglot_dialect = _get_sqlglot_dialect(db_key)

    # Rewrite SQL to qualify unqualified table names
    qualified_query = qualify_sql_tables(query, db_key, schema)
    try:
        parsed = sqlglot.parse_one(qualified_query, dialect=sqlglot_dialect)
    except Exception as e:
        raise ValueError(f"SQL parse failed: {e}")

    # Block DML/DDL
    for node in parsed.walk():
        if isinstance(node, (exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create)):
            raise ValueError(f"Forbidden operation: {type(node).__name__}")

    # Collect used tables
    used_tables = set()
    for t in parsed.find_all(exp.Table):
        if t.db:
            used_tables.add(f"{t.db.lower()}.{t.name.lower()}")
        elif schema and schema != 'All':
            used_tables.add(f"{schema.lower()}.{t.name.lower()}")
        else:
            used_tables.add(t.name.lower())

    # Fail-secure: require ALLOWED_TABLES to be explicitly configured per db_key
    allowed = ALLOWED_TABLES.get(db_key)
    if allowed is None:
        raise ValueError(f"No allowed tables configured for db_key: '{db_key}'. "
                         f"Ensure set_allowed_tables() is called on startup.")
    blocked = used_tables - allowed
    if blocked:
        raise ValueError(f"Unauthorized tables: {blocked}")

    # Add NULLS LAST to DESC ORDER BY via sqlglot (PostgreSQL only)
    from text_to_sql_sidecar.schema_cache import get_engine_dialect
    if get_engine_dialect(db_key) == "postgresql":
        modified = False
        for order in parsed.find_all(exp.Ordered):
            if order.args.get("desc") and not order.args.get("nulls_last"):
                order.set("nulls_last", True)
                modified = True
        if modified:
            qualified_query = parsed.sql(dialect=sqlglot_dialect)

    # Append LIMIT if missing
    if not parsed.find(exp.Limit):
        qualified_query = qualified_query.rstrip(';') + " LIMIT 100"

    return qualified_query
