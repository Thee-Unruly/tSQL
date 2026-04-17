import { useEffect, useState } from 'react';
import { fetchDatabases, fetchTables, submitQuery } from './api';
import type { Schema, QueryResult } from './api';
import SchemaPanel from './components/SchemaPanel';
import QueryPanel from './components/QueryPanel';
import ResultsTable from './components/ResultsTable';
import './App.css';

export default function App() {
  const [databases, setDatabases] = useState<string[]>([]);
  const [selectedDb, setSelectedDb] = useState<string>('');
  const [schema, setSchema] = useState<Schema>({});
  const [result, setResult] = useState<QueryResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [dbError, setDbError] = useState<string | null>(null);

  useEffect(() => {
    fetchDatabases()
      .then((dbs) => {
        const keys = Object.keys(dbs);
        setDatabases(keys);
        if (keys.length > 0) setSelectedDb(keys[0]);
      })
      .catch((e) => setDbError(e.message));
  }, []);

  useEffect(() => {
    if (!selectedDb) return;
    setSchema({});
    setResult(null);
    setError(null);
    fetchTables(selectedDb)
      .then(setSchema)
      .catch((e) => setError(e.message));
  }, [selectedDb]);

  const handleQuery = async (question: string) => {
    setLoading(true);
    setResult(null);
    setError(null);
    try {
      const res = await submitQuery(selectedDb, question);
      setResult(res);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Unknown error');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="app">
      <aside className="sidebar">
        <div className="sidebar-logo">🚀</div>
        <h2 className="sidebar-title">Text-to-SQL<br />Sidecar</h2>
        <p className="sidebar-desc">Select a database and ask questions in natural language. AI generates the SQL for you.</p>
        <hr className="sidebar-divider" />
        <label className="db-label">📊 Database</label>
        {dbError ? (
          <div className="error-box">{dbError}</div>
        ) : (
          <select
            className="db-select"
            value={selectedDb}
            onChange={(e) => setSelectedDb(e.target.value)}
          >
            {databases.map((db) => (
              <option key={db} value={db}>{db}</option>
            ))}
          </select>
        )}
      </aside>

      <main className="main-content">
        <header className="top-bar">
          <h1 className="page-title">Text-to-SQL <span className="highlight">Sidecar</span></h1>
          {selectedDb && <span className="db-badge">🗄️ {selectedDb}</span>}
        </header>
        <div className="content-grid">
          <SchemaPanel schema={schema} />
          <div className="right-col">
            <QueryPanel onSubmit={handleQuery} loading={loading} />
            <ResultsTable result={result} error={error} />
          </div>
        </div>
      </main>
    </div>
  );
}
