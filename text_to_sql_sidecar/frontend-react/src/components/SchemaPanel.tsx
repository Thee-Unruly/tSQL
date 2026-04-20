
import type { Schema } from "../api";

interface Props {
    schema: Schema;
    selectedSchema: string;
    setSelectedSchema: (schema: string) => void;
}

export default function SchemaPanel({ schema, selectedSchema, setSelectedSchema }: Props) {
    const schemaEntries = Object.entries(schema);

    if (schemaEntries.length === 0)
        return <p className="no-tables">No schemas found in this database.</p>;

    // If selectedSchema is not in schema, reset to 'All' or first schema
    if (selectedSchema !== "All" && selectedSchema && !schema[selectedSchema]) {
        setSelectedSchema("All");
    }

    return (
        <div className="schema-panel">
            <h3 className="panel-title">📋 Schemas & Tables</h3>
            <div className="schema-dropdown-row">
                <label htmlFor="schema-select">Schema:</label>
                <select
                    id="schema-select"
                    className="schema-select"
                    value={selectedSchema}
                    onChange={e => setSelectedSchema(e.target.value)}
                >
                    <option value="All">All</option>
                    {schemaEntries.map(([schemaName]) => (
                        <option key={schemaName} value={schemaName}>{schemaName}</option>
                    ))}
                </select>
            </div>
            {selectedSchema === "All" ? (
                <div className="no-tables">Searching all schemas—may be slower.</div>
            ) : (
                <div className="table-list">
                    {selectedSchema && schema[selectedSchema] && (
                        Object.keys(schema[selectedSchema]).length > 0 ? (
                            Object.keys(schema[selectedSchema]).map(table => (
                                <div key={table} className="table-card">
                                    <div className="table-name">
                                        <img src="/table-icon.svg" alt="table icon" style={{ width: 18, height: 18, verticalAlign: 'middle', marginRight: 6 }} />
                                        {table}
                                    </div>
                                </div>
                            ))
                        ) : (
                            <div className="no-tables">No tables in this schema.</div>
                        )
                    )}
                </div>
            )}
        </div>
    );
}
