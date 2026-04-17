import os
from typing import Dict
from dotenv import load_dotenv

print("[DEBUG] Loading .env from:", os.path.join(os.path.dirname(__file__), '.env'))
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
load_dotenv()

def load_db_registry() -> Dict[str, str]:
    db_registry = {}
    print("[DEBUG] Environment variables:")
    for key, value in os.environ.items():
        if key.startswith('DB_'):
            db_key = key[3:].lower().replace('_', '-')
            db_registry[db_key] = value
            print(f"[DEBUG] Found DB: {db_key} -> {value}")
    print(f"[DEBUG] Final DB_REGISTRY: {db_registry}")
    return db_registry

DB_REGISTRY = load_db_registry()

def get_db_uri(db_key: str) -> str:
    uri = DB_REGISTRY.get(db_key)
    if not uri:
        raise ValueError(f"Database key '{db_key}' not found in registry.")
    return uri

def list_databases() -> Dict[str, str]:
    return DB_REGISTRY
