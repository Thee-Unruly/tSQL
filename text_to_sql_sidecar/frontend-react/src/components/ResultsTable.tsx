import { QueryResult } from "../api";

interface Props {
    result: QueryResult | null;
    error: string | null;
}

export default function ResultsTable({ result, error }: Props) {
    if (error)
        return <div className="error-box">❌ {error}</div>;

    if (!result) return null;

    const columns = result.results.length > 0 ? Object.keys(result.results[0]) : [];

    return (
        <div className="results-panel">
            <h3 className="panel-title">✅ Generated SQL</h3>
            <pre className="sql-block">{result.sql}</pre>

            <h3 className="panel-title">📊 Results</h3>
            {result.results.length === 0 ? (
                <p className="no-tables">Query executed successfully, but no results returned.</p>
            ) : (
                <div className="table-wrapper">
                    <table className="results-table">
                        <thead>
                            <tr>
                                {columns.map((col) => (
                                    <th key={col}>{col}</th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {result.results.map((row, i) => (
                                <tr key={i}>
                                    {columns.map((col) => (
                                        <td key={col}>{String(row[col] ?? "")}</td>
                                    ))}
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            )}
        </div>
    );
}
