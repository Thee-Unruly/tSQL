import sqlglot
import sqlglot.expressions as exp
from typing import Set, Dict


# ALLOWED_TABLES: {db_key: set of allowed schema.table names}
ALLOWED_TABLES: Dict[str, Set[str]] = {}

def set_allowed_tables(allowed: Dict[str, Set[str]]):
    global ALLOWED_TABLES
    # allowed: {db_key: {schema.table, ...}}
    ALLOWED_TABLES = allowed

def validate_sql(query: str, db_key: str, schema: str = None) -> str:
    try:
        parsed = sqlglot.parse_one(query, dialect='postgres')
    except Exception as e:
        raise ValueError(f"SQL parse failed: {e}")

    for node in parsed.walk():
        if isinstance(node, (exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create)):
            raise ValueError(f"Forbidden operation: {type(node).__name__}")

    # Support schema-qualified table names and resolve unqualified names when schema is 'All'
    used_tables = set()
    unqualified_tables = set()
    for t in parsed.find_all(exp.Table):
        if t.db:  # schema-qualified
            used_tables.add(f"{t.db.lower()}.{t.name.lower()}")
        elif schema and schema != 'All':
            used_tables.add(f"{schema.lower()}.{t.name.lower()}")
        else:
            # Unqualified table name, need to resolve across all schemas
            unqualified_tables.add(t.name.lower())

    allowed = ALLOWED_TABLES.get(db_key, set())

    # If schema is 'All', try to resolve unqualified tables to a schema
    if schema == 'All' or not schema:
        for tbl in unqualified_tables:
            # Find a matching schema.table in allowed
            matches = [at for at in allowed if at.endswith(f'.{tbl}')]
            if matches:
                used_tables.add(matches[0])  # Use the first match
            else:
                used_tables.add(tbl)  # Will be blocked below

    blocked = used_tables - allowed
    if blocked:
        raise ValueError(f"Unauthorized tables: {blocked}")

    if not parsed.find(exp.Limit):
        query = query.rstrip(';') + " LIMIT 100"

    # Add NULLS LAST to all DESC order expressions so NULLs/empty values don't float to the top
    if "ORDER BY" in query.upper() and "DESC" in query.upper():
        import re
        query = re.sub(
            r'(ORDER BY\s+.+?\s+DESC)(?!\s+NULLS)',
            r'\1 NULLS LAST',
            query,
            flags=re.IGNORECASE
        )

    return query
