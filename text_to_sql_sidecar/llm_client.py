import os
import requests
from typing import Tuple, Generator
import json

LITELLM_API_URL = os.getenv("LITELLM_API_URL", "http://localhost:7072/v1/chat/completions")
LITELLM_API_KEY = os.getenv("LITELLM_API_KEY")
if not LITELLM_API_KEY:
    raise RuntimeError("LITELLM_API_KEY environment variable not set")

# Per-dialect SQL rules injected into the prompt
DB_RULES: dict = {
    "postgresql": """\
- Use schema.table notation for all unqualified table names
- CAST(column AS numeric) for text-to-number coercion
- NULLS LAST / NULLS FIRST in ORDER BY for null handling
- For TOP N: ORDER BY column DESC LIMIT N
- Window functions: ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)""",
    "mysql": """\
- Use CONVERT(column, DECIMAL) for text-to-number coercion
- No NULLS LAST — use: ORDER BY (col IS NULL), col DESC
- Use backticks for reserved words: `column`
- For TOP N: ORDER BY column DESC LIMIT N
- No FULL OUTER JOIN — use UNION of LEFT JOIN and RIGHT JOIN""",
    "sqlite": """\
- CAST(column AS REAL) for text-to-number coercion
- No schema prefix needed — use table names directly
- For TOP N: ORDER BY column DESC LIMIT N""",
    "mssql": """\
- Use schema.table notation (e.g. dbo.table)
- Use TRY_CAST(column AS DECIMAL) for type coercion
- For TOP N: SELECT TOP N ... ORDER BY column DESC
- NULLS LAST not supported — use: ORDER BY CASE WHEN col IS NULL THEN 1 ELSE 0 END, col DESC""",
    "oracle": """\
- Use schema.table notation
- CAST(column AS NUMBER) for type coercion
- For TOP N: FETCH FIRST N ROWS ONLY (Oracle 12c+)
- NULLS LAST / NULLS FIRST supported in ORDER BY
- Use NVL() instead of COALESCE""",
}

_DB_DISPLAY_NAMES: dict = {
    "postgresql": "PostgreSQL",
    "mysql": "MySQL",
    "sqlite": "SQLite",
    "mssql": "SQL Server",
    "oracle": "Oracle",
}

PROMPT_TEMPLATE = """
You are an expert {db_type} data analyst. Write ONLY valid, executable {db_type} SQL.

Schema:
{schema}

Question:
{question}

SQL RULES FOR {db_type}:
{db_rules}

GENERAL RULES:
1. Always validate columns exist in schema before using them
2. JOIN tables only if multiple tables are needed
3. For COUNT/SUM/AVG/MIN/MAX: explicitly use aggregate functions

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
[Single valid {db_type} SQL statement]
"""

def build_prompt(schema: str, question: str, db_type: str = "postgresql") -> str:
    rules = DB_RULES.get(db_type, DB_RULES["postgresql"])
    display_name = _DB_DISPLAY_NAMES.get(db_type, "PostgreSQL")
    return PROMPT_TEMPLATE.format(schema=schema, question=question, db_rules=rules, db_type=display_name)

def generate_sql_with_reasoning(schema: str, question: str, model: str = "gemma2-9b", db_type: str = "postgresql") -> Tuple[str, str]:
    """
    Generate SQL with reasoning step.
    Returns: (reasoning, sql)
    """
    prompt = build_prompt(schema, question, db_type)
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

def generate_sql(schema: str, question: str, model: str = "gemma2-9b", db_type: str = "postgresql") -> str:
    """Backward compatible wrapper that returns only SQL"""
    _, sql = generate_sql_with_reasoning(schema, question, model, db_type)
    return sql

def generate_sql_with_reasoning_streaming(schema: str, question: str, model: str = "gemma2-9b", db_type: str = "postgresql") -> Generator[Tuple[str, str], None, None]:
    """
    Generate SQL with reasoning step using streaming.
    Yields (event_type, data) tuples as the LLM responds.
    event_type can be: "reasoning_chunk", "reasoning_complete", "sql", "error"
    """
    prompt = build_prompt(schema, question, db_type)
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
        "temperature": 0.0,
        "stream": True
    }
    
    try:
        resp = requests.post(LITELLM_API_URL, headers=headers, json=payload, stream=True)
        resp.raise_for_status()
        
        full_content = ""
        reasoning = ""
        sql = ""
        
        # Stream the response chunks
        for line in resp.iter_lines():
            if not line:
                continue
            
            line = line.decode('utf-8') if isinstance(line, bytes) else line
            if line.startswith('data: '):
                line = line[6:]
            
            if line.strip() == '[DONE]':
                break
            
            try:
                chunk_data = json.loads(line)
                delta = chunk_data.get('choices', [{}])[0].get('delta', {})
                content = delta.get('content', '')
                
                if content:
                    full_content += content
                    # Stream reasoning chunks as they arrive
                    if "REASONING:" in full_content and "SQL:" not in full_content:
                        yield ("reasoning_chunk", content)
            except json.JSONDecodeError:
                continue
        
        # Parse final content
        if "REASONING:" in full_content and "SQL:" in full_content:
            parts = full_content.split("SQL:")
            reasoning = parts[0].replace("REASONING:", "").strip()
            sql_part = parts[1].strip() if len(parts) > 1 else ""
            
            # Clean SQL
            if "```" in sql_part:
                lines = sql_part.split("\n")
                lines = [l for l in lines if not l.strip().startswith("```")]
                sql = "\n".join(lines).strip()
            else:
                sql = sql_part
            
            sql = sql.replace("`", "")
        else:
            sql = full_content
            reasoning = "No explicit reasoning provided"
        
        # Yield completion markers
        yield ("reasoning_complete", reasoning)
        yield ("sql", sql)
        
    except Exception as e:
        yield ("error", str(e))
