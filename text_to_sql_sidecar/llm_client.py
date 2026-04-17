import os
import requests

LITELLM_API_URL = os.getenv("LITELLM_API_URL", "http://localhost:7072/v1/chat/completions")
LITELLM_API_KEY = os.getenv("LITELLM_API_KEY", "sk-A2PqYnyEcNmuRSMQerWgiDIs")

PROMPT_TEMPLATE = """
You are an expert data analyst. Given the following database schema and a question, write a safe, efficient SQL query that answers the question. Only use the tables and columns provided.

Schema:
{schema}

Question:
{question}

SQL:
"""

def build_prompt(schema: str, question: str) -> str:
    return PROMPT_TEMPLATE.format(schema=schema, question=question)

def generate_sql(schema: str, question: str, model: str = "gemma2-9b") -> str:
    prompt = build_prompt(schema, question)
    headers = {
        "Authorization": f"Bearer {LITELLM_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are a helpful assistant that writes SQL queries."},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": 512,
        "temperature": 0.0
    }
    resp = requests.post(LITELLM_API_URL, headers=headers, json=payload)
    resp.raise_for_status()
    sql = resp.json()["choices"][0]["message"]["content"].strip()
    return sql
