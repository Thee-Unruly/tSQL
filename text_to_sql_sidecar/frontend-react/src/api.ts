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

export const submitQueryStream = async (
    db_key: string,
    question: string,
    onEvent: (type: string, data: unknown) => void,
    schema?: string
): Promise<void> => {
    const body: any = { db_key, question };
    if (schema && schema !== 'All') body.schema = schema;

    const res = await fetch(`${API_URL}/query-stream`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
    });

    if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || "Query failed");
    }

    const reader = res.body?.getReader();
    if (!reader) throw new Error("No response body");

    const decoder = new TextDecoder();
    let buffer = "";

    while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");

        // Keep the last incomplete line in the buffer
        buffer = lines[lines.length - 1];

        for (let i = 0; i < lines.length - 1; i++) {
            const line = lines[i];
            if (line.startsWith("data: ")) {
                try {
                    const event = JSON.parse(line.slice(6));
                    onEvent(event.type, event.content || event);
                } catch (e) {
                    console.error("Failed to parse event:", line, e);
                }
            }
        }
    }
};
