from sqlalchemy import create_engine, text
from text_to_sql_sidecar.db_registry import get_db_uri

def execute_query(db_key: str, sql: str):
    """Execute a validated SQL query and return results."""
    uri = get_db_uri(db_key)
    engine = create_engine(uri)
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        rows = result.fetchall()
        # Convert rows to list of dicts
        columns = result.keys()
        return [dict(zip(columns, row)) for row in rows]
