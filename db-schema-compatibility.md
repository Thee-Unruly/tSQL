# Database Schema Reflection Compatibility Table

This table summarizes how to reflect schemas, tables, and columns for major databases using SQLAlchemy's inspector, and notes any special handling required.

| Database      | Schema Support | Inspector Method                | Notes/Special Handling                         |
|--------------|---------------|----------------------------------|------------------------------------------------|
| PostgreSQL   | Yes           | get_schema_names(), get_table_names(schema=...), get_columns(table, schema=...) | Skip 'information_schema', 'pg_catalog'        |
| MySQL        | Yes (as databases) | get_schema_names(), get_table_names(schema=...), get_columns(table, schema=...) | Each database is a schema; may need to connect to each separately |
| SQLite       | No            | get_table_names(), get_columns(table) | No schemas; just list tables                  |
| SQL Server   | Yes           | get_schema_names(), get_table_names(schema=...), get_columns(table, schema=...) | System schemas: 'INFORMATION_SCHEMA', 'sys'   |
| Oracle       | Yes           | get_schema_names(), get_table_names(schema=...), get_columns(table, schema=...) | Schema = user; may need to filter system schemas |

## General Approach
- Use inspector.get_schema_names() to list schemas (if supported).
- For each schema, use inspector.get_table_names(schema=schema_name).
- For each table, use inspector.get_columns(table, schema=schema_name).
- For databases without schemas (e.g., SQLite), just list tables and columns.
- Always test with your target DB and handle system schemas or quirks as needed.

---

This table can be referenced when adding support for new databases in your Text-to-SQL Sidecar project.
