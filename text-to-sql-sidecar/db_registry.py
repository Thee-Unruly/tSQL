import os
from typing import Dict
from dotenv import load_dotenv

load_dotenv()

# Example: DB_REGISTRY = { 'prod-warehouse': 'postgresql+psycopg2://user:pass@host/dbname', ... }

def load_db_registry() -> Dict[str, str]:
    """
    Loads database connection URIs from environment variables or a config file.
    Expects variables like DB_PROD_WAREHOUSE, DB_CRM_DB, etc.
    Returns a dict mapping db_key to sqlalchemy_uri.
    """
    db_registry = {}
    for key, value in os.environ.items():
        if key.startswith('DB_'):
            db_key = key[3:].lower().replace('_', '-')
            db_registry[db_key] = value
    return db_registry

DB_REGISTRY = load_db_registry()

def get_db_uri(db_key: str) -> str:
    uri = DB_REGISTRY.get(db_key)
    if not uri:
        raise ValueError(f"Database key '{db_key}' not found in registry.")
    return uri

def list_databases() -> Dict[str, str]:
    """
    Returns the full DB registry for API listing.
    """
    return DB_REGISTRY
