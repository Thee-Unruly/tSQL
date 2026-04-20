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

POSTGRES SQL RULES:
1. Use schema.table notation for all unqualified table names
2. For COUNT/SUM/AVG/MIN/MAX: explicitly use aggregate functions
3. For TOP N / ranking: ORDER BY column DESC/ASC LIMIT N
4. For filtering: use WHERE clauses with proper column types
5. For numeric columns in TEXT: use CAST(column AS numeric)
6. For NULLs in ORDER BY: use NULLS LAST (DESC) or NULLS FIRST (ASC)
7. Always validate columns exist in schema before using them
8. JOIN tables only if multiple tables are needed

QUERY PATTERN EXAMPLES:
- COUNT: "how many X" → SELECT COUNT(*) FROM table
- AGGREGATE: "total/sum/average of X" → SELECT SUM(column) FROM table GROUP BY ...
- TOP N: "top 10 X by Y" → SELECT * FROM table ORDER BY column DESC LIMIT 10
- FILTER: "X where Y = Z" → SELECT * FROM table WHERE condition
- RELATIONSHIP: "X and Y together" → SELECT ... FROM table1 JOIN table2 ON ...

REASONING INSTRUCTIONS:
Analyze the question and explain your approach. Include:
1. What tables/columns you're using and WHY
2. What type of query this is (COUNT, filter, aggregation, ranking, JOIN, etc.)
3. Any transformations or conditions needed
4. Key assumptions you made

Format your response exactly as:
REASONING:
[2-3 sentences explaining your approach and why]

SQL:
[Single valid PostgreSQL statement]
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
