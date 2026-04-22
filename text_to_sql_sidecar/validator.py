import sqlglot
import sqlglot.expressions as exp
from typing import Set, Dict, Optional

# Maps SQLAlchemy dialect names to sqlglot dialect names
_DIALECT_MAP = {
    "postgresql": "postgres",
    "mysql":      "mysql",
    "sqlite":     "sqlite",
    "mssql":      "tsql",
    "oracle":     "oracle",
}

# Maps dialect to the sqlglot Limit expression style
_LIMIT_STYLE = {
    "postgres": "postgres",
    "mysql":    "mysql",
    "sqlite":   "sqlite",
    "tsql":     "tsql",   # uses TOP N, sqlglot handles this
    "oracle":   "oracle", # uses FETCH FIRST, sqlglot handles this
}


def _get_sqlglot_dialect(db_key: str) -> str:
    from text_to_sql_sidecar.schema_cache import get_engine_dialect
    sa_dialect = get_engine_dialect(db_key)
    return _DIALECT_MAP.get(sa_dialect, "postgres")


def _get_cte_names(parsed: exp.Expression) -> set:
    """Return a set of all CTE alias names (lowercased) defined in a WITH clause."""
    cte_names = set()
    for cte in parsed.find_all(exp.CTE):
        if cte.alias:
            cte_names.add(cte.alias.lower())
    return cte_names


def _get_derived_table_aliases(parsed: exp.Expression) -> set:
    """Return aliases of subquery-derived tables so we don't flag them as real tables."""
    aliases = set()
    for subquery in parsed.find_all(exp.Subquery):
        if subquery.alias:
            aliases.add(subquery.alias.lower())
    return aliases


def _collect_used_tables(parsed: exp.Expression, schema: Optional[str], cte_names: set, derived_aliases: set) -> set:
    """
    Collect all real table references from the parsed SQL.
    Excludes CTEs and derived table aliases.
    Returns a set of 'schema.table' or 'table' strings (lowercased).
    """
    used_tables = set()
    for t in parsed.find_all(exp.Table):
        table_name = t.name if isinstance(t.name, str) else str(t.name)
        table_name_lower = table_name.lower()

        # Skip CTEs and derived tables — they are not real DB tables
        if table_name_lower in cte_names or table_name_lower in derived_aliases:
            continue

        if t.db:
            used_tables.add(f"{t.db.lower()}.{table_name_lower}")
        elif schema and schema != "All":
            used_tables.add(f"{schema.lower()}.{table_name_lower}")
        else:
            used_tables.add(table_name_lower)

    return used_tables


def qualify_sql_tables(sql: str, db_key: str, schema: Optional[str] = None) -> str:
    """
    Rewrite unqualified table names in SQL to schema-qualified names using the schema cache.
    Skips CTE names and derived table aliases — they are not real tables.
    """
    from text_to_sql_sidecar.schema_cache import get_schema
    sqlglot_dialect = _get_sqlglot_dialect(db_key)

    try:
        parsed = sqlglot.parse_one(sql, dialect=sqlglot_dialect)
        schema_dict = get_schema(db_key)
    except Exception as e:
        print(f"[DEBUG] qualify_sql_tables parse error: {e}")
        return sql

    # Build table_name -> schema_name mapping (warn on ambiguity)
    table_to_schema = {}
    for schema_name, tables in schema_dict.items():
        for table_name in tables.keys():
            key = table_name.lower()
            if key in table_to_schema:
                print(f"[WARN] Ambiguous table '{table_name}' found in schemas "
                      f"'{table_to_schema[key]}' and '{schema_name}' — using first match")
            else:
                table_to_schema[key] = schema_name

    cte_names = _get_cte_names(parsed)
    derived_aliases = _get_derived_table_aliases(parsed)
    skip_names = cte_names | derived_aliases

    print(f"[DEBUG] qualify_sql_tables: schema={schema}, "
          f"known tables={list(table_to_schema.keys())[:5]}..., "
          f"CTEs={cte_names}, derived={derived_aliases}")

    changed = False
    for t in parsed.find_all(exp.Table):
        table_name_str = t.name if isinstance(t.name, str) else str(t.name)
        table_name_lower = table_name_str.lower()

        # Don't qualify CTEs or derived table aliases
        if table_name_lower in skip_names:
            continue

        if not t.db:
            if schema and schema != "All":
                t.set("db", schema)
                changed = True
                print(f"[DEBUG] Qualified: {table_name_lower} -> {schema}.{table_name_lower}")
            elif table_name_lower in table_to_schema:
                resolved_schema = table_to_schema[table_name_lower]
                t.set("db", resolved_schema)
                changed = True
                print(f"[DEBUG] Qualified: {table_name_lower} -> {resolved_schema}.{table_name_lower}")

    if changed:
        qualified = parsed.sql(dialect=sqlglot_dialect)
        print(f"[DEBUG] qualify_sql_tables output: {qualified}")
        return qualified

    print(f"[DEBUG] qualify_sql_tables: no changes needed")
    return sql


# ALLOWED_TABLES: {db_key: set of allowed 'schema.table' strings}
ALLOWED_TABLES: Dict[str, Set[str]] = {}


def set_allowed_tables(allowed: Dict[str, Set[str]]):
    global ALLOWED_TABLES
    ALLOWED_TABLES = allowed


def validate_sql(query: str, db_key: str, schema: Optional[str] = None) -> str:
    """
    Validate and rewrite a SQL query:
    - Qualifies unqualified table names
    - Blocks DML/DDL operations
    - Enforces ALLOWED_TABLES whitelist (fail-secure)
    - Adds NULLS LAST to DESC ORDER BY (PostgreSQL only)
    - Appends LIMIT if missing (dialect-aware)

    Returns the validated, rewritten SQL string.
    Raises ValueError on any violation.
    """
    from text_to_sql_sidecar.schema_cache import get_engine_dialect
    sqlglot_dialect = _get_sqlglot_dialect(db_key)

    # Step 1: Qualify unqualified table names
    qualified_query = qualify_sql_tables(query, db_key, schema)

    # Step 2: Parse the qualified SQL
    try:
        parsed = sqlglot.parse_one(qualified_query, dialect=sqlglot_dialect)
    except Exception as e:
        raise ValueError(f"SQL parse failed: {e}")

    # Step 3: Block DML / DDL
    for node in parsed.walk():
        if isinstance(node, (exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create)):
            raise ValueError(f"Forbidden operation: {type(node).__name__}")

    # Step 4: Collect real table references (exclude CTEs and derived tables)
    cte_names = _get_cte_names(parsed)
    derived_aliases = _get_derived_table_aliases(parsed)
    used_tables = _collect_used_tables(parsed, schema, cte_names, derived_aliases)

    # Step 5: Enforce ALLOWED_TABLES whitelist (fail-secure)
    allowed = ALLOWED_TABLES.get(db_key)
    if allowed is None:
        raise ValueError(
            f"No allowed tables configured for db_key: '{db_key}'. "
            f"Call set_allowed_tables() on startup."
        )
    blocked = used_tables - allowed
    if blocked:
        raise ValueError(f"Unauthorized tables: {blocked}")

    # Step 6: Add NULLS LAST to DESC ORDER BY (PostgreSQL only, via sqlglot)
    sa_dialect = get_engine_dialect(db_key)
    if sa_dialect == "postgresql":
        nulls_modified = False
        for order in parsed.find_all(exp.Ordered):
            if order.args.get("desc") and not order.args.get("nulls_last"):
                order.set("nulls_last", True)
                nulls_modified = True
        if nulls_modified:
            qualified_query = parsed.sql(dialect=sqlglot_dialect)
            # Re-parse after modification so LIMIT check is accurate
            try:
                parsed = sqlglot.parse_one(qualified_query, dialect=sqlglot_dialect)
            except Exception:
                pass  # non-critical, proceed with string

    # Step 7: Append LIMIT if missing (dialect-aware via sqlglot)
    if not parsed.find(exp.Limit):
        if sa_dialect == "mssql":
            # MSSQL uses TOP N — inject it into the SELECT
            select = parsed.find(exp.Select)
            if select and not select.args.get("top"):
                select.set("top", exp.Top(this=exp.Literal.number(100)))
                qualified_query = parsed.sql(dialect=sqlglot_dialect)
            else:
                qualified_query = qualified_query.rstrip(";") + " LIMIT 100"
        elif sa_dialect == "oracle":
            # Oracle 12c+ uses FETCH FIRST
            qualified_query = qualified_query.rstrip(";") + " FETCH FIRST 100 ROWS ONLY"
        else:
            # PostgreSQL, MySQL, SQLite
            qualified_query = qualified_query.rstrip(";") + " LIMIT 100"

    return qualified_query