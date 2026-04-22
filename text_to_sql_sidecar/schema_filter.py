from typing import Dict, Any, List
import re


def filter_schema_by_relevance(question: str, schema: Dict[str, Dict[str, List[str]]]) -> Dict[str, Dict[str, List[str]]]:
    """
    Filter nested schema {schema: {table: [columns...]}} to only include relevant schemas/tables/columns.
    Uses keyword matching on schema, table, and column names.
    Falls back to returning all if no matches found.
    """
    question_lower = question.lower()
    stop_words = {"what", "which", "how", "many", "most", "the", "a", "an", "in", "top", "by", "with", "is", "are", "do", "does"}
    keywords = [w for w in re.findall(r'\w+', question_lower) if w not in stop_words and len(w) > 2]

    print(f"[DEBUG] Question: {question}")
    print(f"[DEBUG] Keywords: {keywords}")

    if not keywords:
        print(f"[DEBUG] No keywords, returning full schema")
        return schema

    relevant_schema = {}
    for schema_name, tables in schema.items():
        relevant_tables = {}
        for table_name, columns in tables.items():
            schema_match = any(kw in schema_name.lower() for kw in keywords)
            table_match = any(kw in table_name.lower() for kw in keywords)
            matching_cols = [col for col in columns if any(kw in col.lower() for kw in keywords)]
            if schema_match or table_match or matching_cols:
                # FIX: Always include all columns for relevant tables
                relevant_tables[table_name] = columns
        if relevant_tables:
            relevant_schema[schema_name] = relevant_tables

    print(f"[DEBUG] Filtered schema size: {sum(len(t) for t in relevant_schema.values())} tables")

    return relevant_schema if relevant_schema else schema
