import sqlglot
import sqlglot.expressions as exp
from typing import Set, Dict

# Example: ALLOWED_TABLES = { 'prod-warehouse': {'product_master', 'inventory'}, ... }
ALLOWED_TABLES: Dict[str, Set[str]] = {}

def set_allowed_tables(allowed: Dict[str, Set[str]]):
    global ALLOWED_TABLES
    ALLOWED_TABLES = allowed

def validate_sql(query: str, db_key: str) -> str:
    """
    Validates the SQL query for safety and allowed tables.
    - Only SELECT statements at all levels
    - Only whitelisted tables
    - Enforces LIMIT if missing
    Returns the (possibly modified) query.
    Raises ValueError if validation fails.
    """
    try:
        parsed = sqlglot.parse_one(query)
    except Exception as e:
        raise ValueError(f"SQL parse failed: {e}")

    # 1. Only SELECT at every level
    for node in parsed.walk():
        if isinstance(node, (exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create)):
            raise ValueError(f"Forbidden operation: {type(node).__name__}")

    # 2. Only whitelisted tables
    used_tables = {t.name.lower() for t in parsed.find_all(exp.Table)}
    allowed = ALLOWED_TABLES.get(db_key, set())
    blocked = used_tables - allowed
    if blocked:
        raise ValueError(f"Unauthorized tables: {blocked}")

    # 3. Enforce LIMIT
    if not parsed.find(exp.Limit):
        query = query.rstrip(';') + " LIMIT 100"

    return query
