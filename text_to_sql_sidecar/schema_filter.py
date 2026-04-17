from typing import Dict, Any, List
import re

def filter_schema_by_relevance(question: str, schema: Dict[str, List[str]]) -> Dict[str, List[str]]:
    """
    Filter schema to only include tables and columns relevant to the question.
    Uses keyword matching on table and column names.
    Falls back to returning all tables if no matches found.
    """
    question_lower = question.lower()
    
    # Extract keywords from question (remove common words)
    stop_words = {"what", "which", "how", "many", "most", "the", "a", "an", "in", "top", "by", "with", "is", "are", "do", "does"}
    keywords = [w for w in re.findall(r'\w+', question_lower) if w not in stop_words and len(w) > 2]
    
    print(f"[DEBUG] Question: {question}")
    print(f"[DEBUG] Keywords: {keywords}")
    
    if not keywords:
        print(f"[DEBUG] No keywords, returning full schema")
        return schema  # No keywords, return all
    
    relevant_schema = {}
    
    for table_name, columns in schema.items():
        # Check if table name matches any keyword
        table_match = any(kw in table_name.lower() for kw in keywords)
        
        # Check if any column matches keywords
        matching_cols = [
            col for col in columns 
            if any(kw in col.lower() for kw in keywords)
        ]
        
        print(f"[DEBUG] Table '{table_name}': match={table_match}, cols_matched={len(matching_cols)}")
        
        # Include table if:
        # 1. Table name matches, or
        # 2. At least one column matches
        if table_match or matching_cols:
            # If table matches, include all columns; if only columns match, include matched ones + id/key cols
            if table_match:
                relevant_schema[table_name] = columns
            else:
                # Include matched columns + common key columns
                key_cols = [c for c in columns if 'id' in c.lower() or 'key' in c.lower()]
                relevant_schema[table_name] = list(set(matching_cols + key_cols))
    
    print(f"[DEBUG] Filtered schema size: {len(relevant_schema)} tables (from {len(schema)})")
    
    # Fallback: if no tables matched, return all (safety)
    if not relevant_schema:
        print(f"[DEBUG] No matches, returning full schema (fallback)")
        return schema
    
    return relevant_schema
