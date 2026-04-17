const API_URL = "http://localhost:8001";

export interface Database {
    [key: string]: string;
}

export interface Schema {
    [table: string]: string[];
}

export interface QueryResult {
    sql: string;
    results: Record<string, unknown>[];
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
    return data.tables;
};

export const submitQuery = async (db_key: string, question: string): Promise<QueryResult> => {
    const res = await fetch(`${API_URL}/query`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ db_key, question }),
    });
    if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || "Query failed");
    }
    return res.json();
};
