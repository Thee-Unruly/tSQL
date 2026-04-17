import { Schema } from "../api";

interface Props {
    schema: Schema;
}

export default function SchemaPanel({ schema }: Props) {
    const tables = Object.entries(schema);

    if (tables.length === 0)
        return <p className="no-tables">No tables found in this database.</p>;

    return (
        <div className="schema-panel">
            <h3 className="panel-title">📋 Tables & Columns</h3>
            <div className="table-grid">
                {tables.map(([table, cols]) => (
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
    );
}
