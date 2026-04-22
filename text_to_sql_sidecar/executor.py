from sqlalchemy import create_engine, text
from text_to_sql_sidecar.db_registry import get_db_uri

def execute_query(db_key: str, sql: str):
    """Execute a validated SQL query and return results."""
    uri = get_db_uri(db_key)
    engine = create_engine(uri)
    import decimal
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        rows = result.fetchall()
        columns = result.keys()
        def convert_value(val):
            if isinstance(val, decimal.Decimal):
                # Convert to float if possible, else str
                try:
                    return float(val)
                except Exception:
                    return str(val)
            return val
        return [
            {col: convert_value(val) for col, val in zip(columns, row)}
            for row in rows
        ]
