import sqlglot
import sqlglot.expressions as exp
from typing import Set, Dict

ALLOWED_TABLES: Dict[str, Set[str]] = {}

def set_allowed_tables(allowed: Dict[str, Set[str]]):
    global ALLOWED_TABLES
    ALLOWED_TABLES = allowed

def validate_sql(query: str, db_key: str) -> str:
    try:
        parsed = sqlglot.parse_one(query, dialect='postgres')
    except Exception as e:
        raise ValueError(f"SQL parse failed: {e}")

    for node in parsed.walk():
        if isinstance(node, (exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create)):
            raise ValueError(f"Forbidden operation: {type(node).__name__}")

    used_tables = {t.name.lower() for t in parsed.find_all(exp.Table)}
    allowed = ALLOWED_TABLES.get(db_key, set())
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
