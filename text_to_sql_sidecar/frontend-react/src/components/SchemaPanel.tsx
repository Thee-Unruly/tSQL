
import { useState } from "react";
import type { Schema } from "../api";

interface Props {
    schema: Schema;
}

export default function SchemaPanel({ schema }: Props) {
    const schemaEntries = Object.entries(schema);
    const [selectedSchema, setSelectedSchema] = useState<string>(schemaEntries[0]?.[0] || "");

    if (schemaEntries.length === 0)
        return <p className="no-tables">No schemas found in this database.</p>;

    // Update selectedSchema if schema changes
    if (selectedSchema && !schema[selectedSchema]) {
        setSelectedSchema(schemaEntries[0][0]);
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
                    {schemaEntries.map(([schemaName]) => (
                        <option key={schemaName} value={schemaName}>{schemaName}</option>
                    ))}
                </select>
            </div>
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
        </div>
    );
}
