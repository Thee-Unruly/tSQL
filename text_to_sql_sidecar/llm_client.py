import os
import requests
from typing import Tuple

LITELLM_API_URL = os.getenv("LITELLM_API_URL", "http://localhost:7072/v1/chat/completions")
LITELLM_API_KEY = os.getenv("LITELLM_API_KEY", "sk-A2PqYnyEcNmuRSMQerWgiDIs")

PROMPT_TEMPLATE = """
You are an expert PostgreSQL data analyst. 

Given the following database schema and a question, first reason through the problem, then write a valid PostgreSQL SQL query.

Schema:
{schema}

Question:
{question}

Follow these steps:
1. REASONING: Analyze the question and identify which tables and columns are needed
2. SQL: Write the valid PostgreSQL SQL query

Format your response as:
REASONING:
[Your analysis here]

SQL:
[Your SQL query here]
"""

def build_prompt(schema: str, question: str) -> str:
    return PROMPT_TEMPLATE.format(schema=schema, question=question)

def generate_sql_with_reasoning(schema: str, question: str, model: str = "gemma2-9b") -> Tuple[str, str]:
    """
    Generate SQL with reasoning step.
    Returns: (reasoning, sql)
    """
    prompt = build_prompt(schema, question)
    headers = {
        "Authorization": f"Bearer {LITELLM_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are a helpful assistant that writes SQL queries. Think through the problem carefully."},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": 1024,
        "temperature": 0.0
    }
    resp = requests.post(LITELLM_API_URL, headers=headers, json=payload)
    resp.raise_for_status()
    content = resp.json()["choices"][0]["message"]["content"].strip()
    
    # Parse reasoning and SQL from response
    reasoning = ""
    sql = ""
    
    if "REASONING:" in content and "SQL:" in content:
        parts = content.split("SQL:")
        reasoning_part = parts[0].replace("REASONING:", "").strip()
        sql_part = parts[1].strip() if len(parts) > 1 else ""
        reasoning = reasoning_part
        sql = sql_part
    else:
        # Fallback: assume entire content is SQL
        sql = content
        reasoning = "No explicit reasoning provided"
    
    # Strip markdown code fences from SQL
    if "```" in sql:
        lines = sql.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        sql = "\n".join(lines).strip()
    
    # Remove any remaining backticks
    sql = sql.replace("`", "")
    
    return reasoning, sql

def generate_sql(schema: str, question: str, model: str = "gemma2-9b") -> str:
    """Backward compatible wrapper that returns only SQL"""
    _, sql = generate_sql_with_reasoning(schema, question, model)
    return sql
