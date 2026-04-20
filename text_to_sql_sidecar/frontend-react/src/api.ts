const API_URL = "http://localhost:8001";

export interface Database {
    [key: string]: string;
}


// New Schema type: { [schema: string]: { [table: string]: string[] } }
export interface Schema {
    [schema: string]: {
        [table: string]: string[];
    };
}

export interface QueryResult {
    sql: string;
    results: Record<string, unknown>[];
    reasoning?: string;
}

export const fetchDatabases = async (): Promise<Database> => {
    const res = await fetch(`${API_URL}/databases`);
    if (!res.ok) throw new Error("Failed to fetch databases");
    return res.json();
};

export const fetchTables = async (db: string): Promise<Schema> => {
    const res = await fetch(`${API_URL}/tables?db=${encodeURIComponent(db)}`);
    if (!res.ok) throw new Error("Failed to fetch tables");
    const data = await res.json();
    // The backend now returns { schemas: { ... } }
    return data.schemas;
};

export const submitQuery = async (db_key: string, question: string, schema?: string): Promise<QueryResult> => {
    const body: any = { db_key, question };
    if (schema && schema !== 'All') body.schema = schema;
    const res = await fetch(`${API_URL}/query`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
    });
    if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || "Query failed");
    }
    return res.json();
};
