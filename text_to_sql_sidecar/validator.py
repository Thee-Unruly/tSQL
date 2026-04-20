import sqlglot
import sqlglot.expressions as exp
from typing import Set, Dict


def qualify_sql_tables(sql: str, db_key: str, schema: str = None) -> str:
    """
    Rewrite unqualified table names in SQL to schema-qualified names using schema cache.
    Applies when schema is 'All', a specific schema name, or not specified.
    """
    from text_to_sql_sidecar.schema_cache import get_schema
    try:
        parsed = sqlglot.parse_one(sql, dialect='postgres')
        schema_dict = get_schema(db_key)
    except Exception as e:
        print(f"[DEBUG] qualify_sql_tables error: {e}")
        return sql
    
    # Build a mapping: table_name -> schema_name (first match)
    table_to_schema = {}
    for schema_name, tables in schema_dict.items():
        for table_name in tables.keys():
            if table_name.lower() not in table_to_schema:
                table_to_schema[table_name.lower()] = schema_name
    
    print(f"[DEBUG] qualify_sql_tables: schema param={schema}, table_to_schema keys={list(table_to_schema.keys())[:5]}...")
    
    changed = False
    for t in parsed.find_all(exp.Table):
        # Get the table name as a string
        table_name_str = t.name if isinstance(t.name, str) else str(t.name)
        table_name_lower = table_name_str.lower()
        
        # Check if table is unqualified and should be qualified
        if not t.db:
            if schema and schema != 'All':
                # Specific schema selected: qualify with that schema
                t.set('db', schema)
                changed = True
                print(f"[DEBUG] qualify_sql_tables: qualified {table_name_lower} -> {schema}.{table_name_lower} (specific schema)")
            elif schema == 'All' or schema is None:
                # All schemas or no schema: look up table in cache
                if table_name_lower in table_to_schema:
                    schema_name = table_to_schema[table_name_lower]
                    t.set('db', schema_name)
                    changed = True
                    print(f"[DEBUG] qualify_sql_tables: qualified {table_name_lower} -> {schema_name}.{table_name_lower} (from cache)")
    
    if changed:
        qualified = parsed.sql(dialect='postgres')
        print(f"[DEBUG] qualify_sql_tables output: {qualified}")
        return qualified
    
    print(f"[DEBUG] qualify_sql_tables: no changes needed")
    return sql


# ALLOWED_TABLES: {db_key: set of allowed schema.table names}
ALLOWED_TABLES: Dict[str, Set[str]] = {}

def set_allowed_tables(allowed: Dict[str, Set[str]]):
    global ALLOWED_TABLES
    # allowed: {db_key: {schema.table, ...}}
    ALLOWED_TABLES = allowed

def validate_sql(query: str, db_key: str, schema: str = None) -> str:
    # Rewrite SQL to qualify unqualified table names if needed
    qualified_query = qualify_sql_tables(query, db_key, schema)
    try:
        parsed = sqlglot.parse_one(qualified_query, dialect='postgres')
    except Exception as e:
        raise ValueError(f"SQL parse failed: {e}")

    for node in parsed.walk():
        if isinstance(node, (exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create)):
            raise ValueError(f"Forbidden operation: {type(node).__name__}")

    # Support schema-qualified table names only now (all unqualified should be rewritten)
    used_tables = set()
    for t in parsed.find_all(exp.Table):
        if t.db:
            used_tables.add(f"{t.db.lower()}.{t.name.lower()}")
        elif schema and schema != 'All':
            used_tables.add(f"{schema.lower()}.{t.name.lower()}")
        else:
            used_tables.add(t.name.lower())
    allowed = ALLOWED_TABLES.get(db_key, set())
    blocked = used_tables - allowed
    if blocked:
        raise ValueError(f"Unauthorized tables: {blocked}")

    if not parsed.find(exp.Limit):
        qualified_query = qualified_query.rstrip(';') + " LIMIT 100"

    # Add NULLS LAST to all DESC order expressions so NULLs/empty values don't float to the top
    if "ORDER BY" in qualified_query.upper() and "DESC" in qualified_query.upper():
        import re
        qualified_query = re.sub(
            r'(ORDER BY\s+.+?\s+DESC)(?!\s+NULLS)',
            r'\1 NULLS LAST',
            qualified_query,
            flags=re.IGNORECASE
        )

    return qualified_query
