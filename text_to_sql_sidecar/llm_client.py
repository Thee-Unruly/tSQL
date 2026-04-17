import os
import requests
from typing import Tuple

LITELLM_API_URL = os.getenv("LITELLM_API_URL", "http://localhost:7072/v1/chat/completions")
LITELLM_API_KEY = os.getenv("LITELLM_API_KEY", "sk-A2PqYnyEcNmuRSMQerWgiDIs")

PROMPT_TEMPLATE = """
You are an expert PostgreSQL data analyst. Write ONLY valid, executable PostgreSQL SQL.

Schema:
{schema}

Question:
{question}

CRITICAL SQL RULES:
1. For "top N by column" queries: SELECT * FROM table ORDER BY column DESC LIMIT N
   - Example: "top 10 products in ratings" → ORDER BY rating DESC LIMIT 10
   - Do NOT calculate averages, divisions, or aggregates unless the question explicitly asks for them
2. Do NOT use aggregate functions (SUM, COUNT, AVG) unless the question says "total", "count", "average", "sum"
3. Do NOT invent calculations or columns that don't exist
4. Column names are case-sensitive - match the schema exactly
5. Use CAST when needed: CAST(column AS numeric)
6. Always validate that columns exist in the schema before using them

SAFE PATTERN FOR "TOP N":
- Question: "What top 10 products in ratings?"
- Answer: SELECT * FROM sales_ ORDER BY rating DESC LIMIT 10;

Follow these steps:
1. REASONING: Is this a "top N" query? If yes, just ORDER BY DESC LIMIT N. If it needs aggregation, be explicit.
2. SQL: Write the simplest valid query that answers the question.

Format:
REASONING:
[One sentence: what columns to use and why]

SQL:
[Single SQL statement only]
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
