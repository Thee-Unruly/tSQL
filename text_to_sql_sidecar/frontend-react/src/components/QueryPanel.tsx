import { useState } from "react";

interface Props {
    onSubmit: (question: string) => void;
    loading: boolean;
}

export default function QueryPanel({ onSubmit, loading }: Props) {
    const [question, setQuestion] = useState("");

    return (
        <div className="query-panel">
            <h3 className="panel-title">❓ Ask a Question</h3>
            <textarea
                className="query-input"
                rows={4}
                placeholder="e.g. Show me all products with rating above 4&#10;or: SELECT * FROM sales_ LIMIT 10"
                value={question}
                onChange={(e) => setQuestion(e.target.value)}
            />
            <button
                className="submit-btn"
                disabled={loading || !question.trim()}
                onClick={() => onSubmit(question)}
            >
                {loading ? "⏳ Generating..." : "🔍 Submit Query"}
            </button>
        </div>
    );
}
