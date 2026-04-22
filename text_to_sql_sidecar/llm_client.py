import os
import requests
from typing import Tuple, Generator
import json

LITELLM_API_URL = os.getenv("LITELLM_API_URL", "http://localhost:7072/v1/chat/completions")
LITELLM_API_KEY = os.getenv("LITELLM_API_KEY", "")

if not LITELLM_API_KEY:
    print("[WARN] LITELLM_API_KEY environment variable not set. LLM queries will fail until it is configured.")

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

# Complex query patterns with generic placeholders
# The LLM will substitute real table/column names from the schema
COMPLEX_PATTERNS = {
    "top_n_per_group": {
        "keywords": ["top", "each", "per category", "per group", "within each", "in each"],
        "example": """\
-- Pattern: Top N rows per group (e.g. top 5 products per category)
-- Use ROW_NUMBER() OVER (PARTITION BY ...) — never use LIMIT alone for per-group ranking
WITH ranked AS (
    SELECT
        <group_column>,
        <name_column>,
        ROUND(AVG(CAST(<numeric_text_column> AS DECIMAL)), 2) AS avg_value,
        CAST(<count_text_column> AS INTEGER) AS total_count,
        ROW_NUMBER() OVER (
            PARTITION BY <group_column>
            ORDER BY AVG(CAST(<numeric_text_column> AS DECIMAL)) DESC
        ) AS rnk
    FROM <schema>.<table>
    WHERE CAST(<count_text_column> AS INTEGER) > <threshold>
    GROUP BY <group_column>, <name_column>, <count_text_column>
)
SELECT <group_column>, <name_column>, avg_value, total_count
FROM ranked
WHERE rnk <= <N>
ORDER BY <group_column>, avg_value DESC NULLS LAST;"""
    },
    "running_total": {
        "keywords": ["running total", "cumulative", "so far", "to date", "over time"],
        "example": """\
-- Pattern: Running/cumulative total using window function
SELECT
    <date_column>,
    <value_column>,
    SUM(<value_column>) OVER (ORDER BY <date_column>) AS running_total
FROM <schema>.<table>
ORDER BY <date_column>;"""
    },
    "conditional_agg": {
        "keywords": ["percentage", "ratio", "of total", "breakdown", "share"],
        "example": """\
-- Pattern: Percentage share per group
SELECT
    <group_column>,
    COUNT(*) AS group_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM <schema>.<table>
GROUP BY <group_column>;"""
    },
    "compare_to_average": {
        "keywords": ["above average", "below average", "compared to", "vs average", "better than average", "difference from average", "exceed"],
        "example": """\
-- Pattern: Items that exceed their group average rating
-- CRITICAL: This requires TWO separate CTEs with GROUP BY, then JOIN + filter
-- DO NOT mix window functions (OVER) with GROUP BY in same SELECT

WITH group_avg AS (
    -- CTE 1: Calculate true average per group (e.g. per category)
    -- Group by ONLY the grouping column
    SELECT
        <group_column>,
        ROUND(AVG(CAST(<numeric_column> AS DECIMAL)), 2) AS group_avg_value
    FROM <schema>.<table>
    WHERE <count_column> ~ '^\\d+$'
      AND CAST(<count_column> AS INTEGER) >= <min_reviews>  -- ALWAYS cast text count columns, never compare text to integer directly
    GROUP BY <group_column>
),
item_avg AS (
    -- CTE 2: Calculate average per individual item (e.g. per product)
    -- Group by BOTH the grouping column AND the item column
    SELECT
        <group_column>,
        <name_column>,
        COUNT(*) AS review_count,
        ROUND(AVG(CAST(<numeric_column> AS DECIMAL)), 2) AS item_avg_value
    FROM <schema>.<table>
    WHERE <count_column> ~ '^\\d+$'
      AND CAST(<count_column> AS INTEGER) >= <min_reviews>  -- ALWAYS cast text count columns, never compare text to integer directly
    GROUP BY <group_column>, <name_column>
    HAVING COUNT(*) >= <min_reviews>  -- Additional filter after aggregation
)
SELECT
    i.<group_column>,
    i.<name_column>,
    i.item_avg_value                          AS item_avg_rating,
    g.group_avg_value                         AS group_avg_rating,
    ROUND(i.item_avg_value - g.group_avg_value, 2)  AS rating_difference
FROM item_avg i
JOIN group_avg g ON i.<group_column> = g.<group_column>
WHERE i.item_avg_value > g.group_avg_value  -- Filter items above group average
ORDER BY i.<group_column>, rating_difference DESC NULLS LAST;"""
    },
    "deduplication": {
        "keywords": ["latest", "most recent", "first", "deduplicate", "unique per"],
        "example": """\
-- Pattern: Most recent / first record per entity
WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY <entity_column>
            ORDER BY <date_column> DESC
        ) AS rnk
    FROM <schema>.<table>
)
SELECT * FROM ranked WHERE rnk = 1;"""
    },
    "pivot_breakdown": {
        "keywords": ["pivot", "by month", "by year", "by quarter", "side by side", "columns per"],
        "example": """\
-- Pattern: Conditional aggregation as pivot
SELECT
    <group_column>,
    SUM(CASE WHEN <pivot_column> = '<value1>' THEN <measure> ELSE 0 END) AS <value1>_total,
    SUM(CASE WHEN <pivot_column> = '<value2>' THEN <measure> ELSE 0 END) AS <value2>_total
FROM <schema>.<table>
GROUP BY <group_column>
ORDER BY <group_column>;"""
    },
}


def get_relevant_examples(question: str) -> str:
    """
    Match the question against known complex query patterns and
    return relevant SQL examples to inject into the prompt.
    """
    question_lower = question.lower()
    matched = []
    for pattern_name, pattern in COMPLEX_PATTERNS.items():
        if any(kw in question_lower for kw in pattern["keywords"]):
            matched.append(pattern["example"])
            print(f"[DEBUG] Matched complex pattern: {pattern_name}")
    if matched:
        return (
            "RELEVANT QUERY PATTERNS — follow the structure of these examples closely,\n"
            "substituting the real table and column names from the schema above:\n\n"
            + "\n\n".join(matched)
        )
    return ""


PROMPT_TEMPLATE = """
You are an expert {db_type} data analyst. Write ONLY valid, executable {db_type} SQL.

Schema:
{schema}

{examples}
Question:
{question}

SQL RULES FOR {db_type}:
{db_rules}

GENERAL RULES:
1. Always validate columns exist in the schema before using them — never invent column names
2. JOIN tables only if multiple tables are needed
3. For COUNT/SUM/AVG/MIN/MAX: explicitly use aggregate functions
4. When using GROUP BY, only select columns that are in the GROUP BY clause or inside an aggregate function.
    IMPORTANT: Do NOT add a column to GROUP BY just because you used CAST() on it in WHERE.
    Filtering with CAST(rating_count AS INTEGER) >= 5 in WHERE does NOT mean rating_count belongs in GROUP BY.
    Wrong:  GROUP BY category, product_name, rating_count
    Right:  GROUP BY category, product_name
5. For TEXT columns storing numeric values, ALWAYS cast before ANY comparison or arithmetic.
    NEVER write: WHERE rating_count >= 5
    ALWAYS write: WHERE CAST(rating_count AS INTEGER) >= 5
    If the column might contain non-numeric text, guard with: WHERE column ~ '^\\d+$' AND CAST(column AS INTEGER) >= 5
    This applies to every query, every time, no exceptions.
6. Use WHERE for filtering BEFORE aggregation; use HAVING for filtering AFTER aggregation
7. For "top N per group" queries, ALWAYS use ROW_NUMBER() OVER (PARTITION BY ...) — never use LIMIT alone as it applies globally, not per group
8. NEVER use window functions (OVER clause) in the same SELECT that has GROUP BY — they are mutually exclusive
   - For comparing items to their group average: use two separate CTEs (one with GROUP BY for the group average, one with GROUP BY for the item average)
   - Then JOIN the CTEs and filter on the comparison
   - See the "compare_to_average" pattern example above for the exact structure

QUERY PATTERN EXAMPLES:
- COUNT:             "how many X"                → SELECT COUNT(*) FROM <schema>.<table>
- AGGREGATE:         "total/sum/average of X"    → SELECT SUM(col) FROM <schema>.<table> GROUP BY ...
- TOP N GLOBAL:      "top 10 X by Y"             → SELECT * FROM <schema>.<table> ORDER BY col DESC LIMIT 10
- TOP N PER GROUP:   "top 5 X per category"      → WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY <group> ORDER BY <col> DESC) AS rnk FROM <schema>.<table>) SELECT * FROM ranked WHERE rnk <= 5
- FILTER:            "X where Y = Z"             → SELECT * FROM <schema>.<table> WHERE condition
- JOIN:              "X and Y together"           → SELECT ... FROM <schema>.<table1> JOIN <schema>.<table2> ON ...

REASONING INSTRUCTIONS:
Analyze the question and explain your approach. Include:
1. What tables/columns you are using and WHY
2. What type of query this is (COUNT, filter, aggregation, ranking, JOIN, etc.)
3. Any type casting or transformations needed
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
    examples = get_relevant_examples(question)
    return PROMPT_TEMPLATE.format(
        schema=schema,
        question=question,
        db_rules=rules,
        db_type=display_name,
        examples=examples,
    )


def _parse_reasoning_and_sql(content: str) -> Tuple[str, str]:
    """
    Parse LLM response into (reasoning, sql).
    Handles missing sections gracefully.
    """
    reasoning = ""
    sql = ""

    if "REASONING:" in content and "SQL:" in content:
        parts = content.split("SQL:")
        reasoning = parts[0].replace("REASONING:", "").strip()
        sql = parts[1].strip() if len(parts) > 1 else ""
    else:
        # Fallback: treat entire response as SQL
        sql = content
        reasoning = "No explicit reasoning provided"

    # Strip markdown code fences
    if "```" in sql:
        lines = sql.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        sql = "\n".join(lines).strip()

    # Remove stray backticks
    sql = sql.replace("`", "")

    return reasoning, sql


def generate_sql_with_reasoning(
    schema: str,
    question: str,
    model: str = "gemma2-9b",
    db_type: str = "postgresql"
) -> Tuple[str, str]:
    """
    Generate SQL with reasoning step.
    Returns: (reasoning, sql)
    """
    if not LITELLM_API_KEY:
        raise RuntimeError("LITELLM_API_KEY environment variable must be set before making LLM requests")

    prompt = build_prompt(schema, question, db_type)
    headers = {
        "api-key": LITELLM_API_KEY,
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are a helpful assistant that writes SQL queries. Think through the problem carefully."},
            {"role": "user", "content": prompt},
        ],
        "max_tokens": 1024,
        "temperature": 0.0,
    }

    resp = requests.post(LITELLM_API_URL, headers=headers, json=payload)
    resp.raise_for_status()
    content = resp.json()["choices"][0]["message"]["content"].strip()
    return _parse_reasoning_and_sql(content)


def generate_sql_with_retry(
    schema: str,
    question: str,
    model: str = "gemma2-9b",
    db_type: str = "postgresql",
    db_key: str = None,
    schema_name: str = None,
    max_retries: int = 2,
) -> Tuple[str, str]:
    """
    Generate SQL with automatic retry on validation failure.
    Feeds the error and bad SQL back to the LLM as a hint on each retry.
    Returns: (reasoning, sql)

    If db_key and schema_name are provided, runs validate_sql() on each attempt.
    Otherwise falls back to basic SELECT check only.
    """
    from text_to_sql_sidecar.validator import validate_sql

    current_question = question
    last_reasoning = ""
    last_sql = ""

    for attempt in range(max_retries + 1):
        reasoning, sql = generate_sql_with_reasoning(schema, current_question, model, db_type)
        last_reasoning = reasoning
        last_sql = sql

        # Run validation if db_key provided, else basic check
        try:
            if db_key:
                validate_sql(sql, db_key, schema_name)
            elif not sql.strip().upper().startswith("SELECT"):
                raise ValueError("Query must be a SELECT statement")

            print(f"[DEBUG] SQL validated successfully on attempt {attempt + 1}")
            return reasoning, sql

        except ValueError as e:
            error_msg = str(e)
            print(f"[DEBUG] Attempt {attempt + 1} failed: {error_msg}")

            if attempt < max_retries:
                current_question = (
                    f"{question}\n\n"
                    f"Your previous attempt produced invalid SQL.\n"
                    f"Error: {error_msg}\n"
                    f"Bad SQL:\n{sql}\n\n"
                    f"Fix the error. Do not repeat the same mistake."
                )

    # Return best effort after all retries exhausted
    print(f"[WARN] All {max_retries + 1} attempts failed. Returning last generated SQL.")
    return last_reasoning, last_sql


def generate_sql(
    schema: str,
    question: str,
    model: str = "gemma2-9b",
    db_type: str = "postgresql"
) -> str:
    """Backward compatible wrapper that returns only SQL."""
    _, sql = generate_sql_with_reasoning(schema, question, model, db_type)
    return sql


def generate_sql_with_reasoning_streaming(
    schema: str,
    question: str,
    model: str = "gemma2-9b",
    db_type: str = "postgresql"
) -> Generator[Tuple[str, str], None, None]:
    """
    Generate SQL with reasoning step using streaming.
    Yields (event_type, data) tuples as the LLM responds.
    event_type can be: "reasoning_chunk", "reasoning_complete", "sql", "error"
    """
    prompt = build_prompt(schema, question, db_type)
    headers = {
        "api-key": LITELLM_API_KEY,
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are a helpful assistant that writes SQL queries. Think through the problem carefully."},
            {"role": "user", "content": prompt},
        ],
        "max_tokens": 1024,
        "temperature": 0.0,
        "stream": True,
    }

    try:
        resp = requests.post(LITELLM_API_URL, headers=headers, json=payload, stream=True)
        resp.raise_for_status()

        full_content = ""

        for line in resp.iter_lines():
            if not line:
                continue

            line = line.decode("utf-8") if isinstance(line, bytes) else line
            if line.startswith("data: "):
                line = line[6:]
            if line.strip() == "[DONE]":
                break

            try:
                chunk_data = json.loads(line)
                delta = chunk_data.get("choices", [{}])[0].get("delta", {})
                content = delta.get("content", "")

                if content:
                    full_content += content
                    # Stream reasoning chunks as they arrive (before SQL section)
                    if "REASONING:" in full_content and "SQL:" not in full_content:
                        yield ("reasoning_chunk", content)

            except json.JSONDecodeError:
                continue

        # Parse and yield final results
        reasoning, sql = _parse_reasoning_and_sql(full_content)
        yield ("reasoning_complete", reasoning)
        yield ("sql", sql)

    except Exception as e:
        yield ("error", str(e))