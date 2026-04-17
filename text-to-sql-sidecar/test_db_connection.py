import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv

load_dotenv()

uri = os.getenv("DB_POSTGRES_LOCAL")

if not uri:
    print("DB_POSTGRES_LOCAL not set in .env")
    exit(1)

try:
    engine = create_engine(uri)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1;"))
        print("Connection successful! Result:", result.scalar())
except OperationalError as e:
    print("Connection failed:", e)
except Exception as e:
    print("Unexpected error:", e)
