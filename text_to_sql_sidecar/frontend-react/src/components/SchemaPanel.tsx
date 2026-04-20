
import type { Schema } from "../api";

interface Props {
    schema: Schema;
}

export default function SchemaPanel({ schema }: Props) {
    const schemaEntries = Object.entries(schema);

    if (schemaEntries.length === 0)
        return <p className="no-tables">No schemas found in this database.</p>;

    return (
        <div className="schema-panel">
            <h3 className="panel-title">📋 Schemas, Tables & Columns</h3>
            <div className="schema-tree">
                {schemaEntries.map(([schemaName, tables]) => (
                    <div key={schemaName} className="schema-block">
                        <div className="schema-name">🏷️ <b>{schemaName}</b></div>
                        <div className="table-list">
                            {Object.entries(tables).map(([table, cols]) => (
                                <div key={table} className="table-card">
                                    <div className="table-name">📁 {table}</div>
                                    <div className="table-cols">
                                        {cols.map((col) => (
                                            <span key={col} className="col-badge">{col}</span>
                                        ))}
                                    </div>
                                </div>
                            ))}
                        </div>
                    </div>
                ))}
            </div>
        </div>
    );
}
