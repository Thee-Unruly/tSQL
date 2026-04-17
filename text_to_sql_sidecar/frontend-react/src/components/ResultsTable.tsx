import { useState } from "react";
import type { QueryResult } from "../api";

const TRUNCATE_LENGTH = 60;

interface CellPopupProps {
    value: string;
    onClose: () => void;
    anchorRect: DOMRect;
}

function CellPopup({ value, onClose, anchorRect }: CellPopupProps) {
    const top = anchorRect.top + window.scrollY - 12;
    const left = anchorRect.left + window.scrollX;
    return (
        <>
            <div className="cell-popup-overlay" onClick={onClose} />
            <div
                className="cell-popup"
                style={{ top, left, maxWidth: Math.min(420, window.innerWidth - left - 16) }}
            >
                <button className="cell-popup-close" onClick={onClose}>✕</button>
                <p className="cell-popup-text">{value}</p>
            </div>
        </>
    );
}

interface Props {
    result: QueryResult | null;
    error: string | null;
}

export default function ResultsTable({ result, error }: Props) {
    const [popup, setPopup] = useState<{ value: string; rect: DOMRect } | null>(null);

    if (error)
        return <div className="error-box">❌ {error}</div>;

    if (!result) return null;

    const columns = result.results.length > 0 ? Object.keys(result.results[0]) : [];

    const handleCellClick = (e: React.MouseEvent<HTMLTableCellElement>, value: string) => {
        if (value.length <= TRUNCATE_LENGTH) return;
        setPopup({ value, rect: e.currentTarget.getBoundingClientRect() });
    };

    return (
        <div className="results-panel">
            {popup && (
                <CellPopup
                    value={popup.value}
                    anchorRect={popup.rect}
                    onClose={() => setPopup(null)}
                />
            )}
            {result.reasoning && (
                <>
                    <span className="reasoning-label">LLM Reasoning</span>
                    <p className="reasoning-text">{result.reasoning}</p>
                </>
            )}
            <span className="sql-label">Generated SQL</span>
            <pre className="sql-block">{result.sql}</pre>

            <span className="results-label">Results</span>
            {result.results.length === 0 ? (
                <p className="empty-results">Query executed successfully, but no rows returned.</p>
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
                                    {columns.map((col) => {
                                        const raw = String(row[col] ?? "");
                                        const isTruncated = raw.length > TRUNCATE_LENGTH;
                                        const display = isTruncated ? raw.slice(0, TRUNCATE_LENGTH) + "…" : raw;
                                        return (
                                            <td
                                                key={col}
                                                className={isTruncated ? "td-truncated" : ""}
                                                onClick={(e) => handleCellClick(e, raw)}
                                                title={isTruncated ? "Click to expand" : undefined}
                                            >
                                                {display}
                                            </td>
                                        );
                                    })}
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            )}
        </div>
    );
}
