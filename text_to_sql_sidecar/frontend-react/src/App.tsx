import { useEffect, useRef, useState } from 'react';
import { fetchDatabases, fetchTables, submitQuery } from './api';
import type { Schema, QueryResult } from './api';
import SchemaPanel from './components/SchemaPanel';
import ResultsTable from './components/ResultsTable';

interface Message {
  role: 'user' | 'assistant';
  text?: string;
  result?: QueryResult;
  error?: string;
}

export default function App() {
  const [databases, setDatabases] = useState<string[]>([]);
  const [selectedDb, setSelectedDb] = useState<string>('');
  const [schema, setSchema] = useState<Schema>({});
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [dbError, setDbError] = useState<string | null>(null);
  const [schemaOpen, setSchemaOpen] = useState(false);
  const [selectedSchema, setSelectedSchema] = useState<string>('All');
  const bottomRef = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

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
    setMessages([]);
    setSelectedSchema('All');
    fetchTables(selectedDb).then(setSchema).catch(() => { });
  }, [selectedDb]);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, loading]);

  const handleSend = async () => {
    if (!input.trim() || loading || !selectedDb) return;
    const question = input.trim();
    setInput('');
    if (textareaRef.current) textareaRef.current.style.height = 'auto';
    setMessages((prev) => [...prev, { role: 'user', text: question }]);
    setLoading(true);
    try {
      const res = await submitQuery(selectedDb, question, selectedSchema);
      setMessages((prev) => [...prev, { role: 'assistant', result: res }]);
    } catch (e: unknown) {
      setMessages((prev) => [
        ...prev,
        { role: 'assistant', error: e instanceof Error ? e.message : 'Unknown error' },
      ]);
    } finally {
      setLoading(false);
    }
  };

  const handleKey = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  const handleInputChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    setInput(e.target.value);
    e.target.style.height = 'auto';
    e.target.style.height = Math.min(e.target.scrollHeight, 180) + 'px';
  };

  return (
    <div className="app">
      <aside className="sidebar">
        <div className="sidebar-header">
          <span className="logo-icon">⚡</span>
          <span className="logo-text">Text-to-SQL</span>
        </div>

        <div className="sidebar-section">
          <span className="section-label">Database</span>
          {dbError ? (
            <div className="db-error">Failed to fetch</div>
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
        </div>

        <div className="sidebar-section">
          <button className="schema-toggle" onClick={() => setSchemaOpen((o) => !o)}>
            <span>Schema Explorer</span>
            <span className="toggle-arrow">{schemaOpen ? '▲' : '▼'}</span>
          </button>
          {schemaOpen && (
            <SchemaPanel
              schema={schema}
              selectedSchema={selectedSchema}
              setSelectedSchema={setSelectedSchema}
            />
          )}
        </div>
      </aside>

      <main className="main">
        <div className="main-header">
          <span className="main-header-breadcrumb">Text-to-SQL //</span>
          <span className="main-header-title">
            {selectedDb ? selectedDb : 'Select a database'}
          </span>
        </div>

        <div className="card-wrap">
          <div className="white-card">
            <div className="chat-area">
              {messages.length === 0 && (
                <div className="welcome">
                  <div className="welcome-icon">⚡</div>
                  <h2>Text-to-SQL Sidecar</h2>
                  <p>Ask a question about your data in plain English.</p>
                  <div className="example-pills">
                    {['Show me all tables', 'Top 10 rows from orders', 'Count rows grouped by status'].map((ex) => (
                      <button key={ex} className="example-pill" onClick={() => setInput(ex)}>
                        {ex}
                      </button>
                    ))}
                  </div>
                </div>
              )}

              {messages.map((msg, i) => (
                <div key={i} className={`message ${msg.role}`}>
                  <div className="avatar">{msg.role === 'user' ? 'U' : '⚡'}</div>
                  <div className="message-content">
                    {msg.role === 'user' && <p className="user-text">{msg.text}</p>}
                    {msg.role === 'assistant' && (
                      <ResultsTable result={msg.result ?? null} error={msg.error ?? null} />
                    )}
                  </div>
                </div>
              ))}

              {loading && (
                <div className="message assistant">
                  <div className="avatar">⚡</div>
                  <div className="message-content">
                    <div className="typing-dots"><span /><span /><span /></div>
                  </div>
                </div>
              )}
              <div ref={bottomRef} />
            </div>

            <div className="input-bar">
              <div className="input-context-row">
                <div className="context-left">
                  <span className="context-db-btn">
                    {selectedDb || 'No database'} ↓
                  </span>
                </div>
                <span className="context-right">Enter ↵ to send</span>
              </div>

              <div className="input-box">
                <textarea
                  ref={textareaRef}
                  className="chat-input"
                  rows={1}
                  placeholder="Ask anything about your data. Type a question or raw SQL..."
                  value={input}
                  onChange={handleInputChange}
                  onKeyDown={handleKey}
                />
                <button
                  className="send-btn"
                  onClick={handleSend}
                  disabled={loading || !input.trim() || !selectedDb}
                  title="Send (Enter)"
                >
                  ↑
                </button>
              </div>

              <div className="input-footer">
                <div className="input-footer-left">
                  <button className="footer-btn">📎 Attach</button>
                  <button className="footer-btn">🗂 Schema</button>
                </div>
                <span className="input-hint">Shift+Enter for newline</span>
              </div>
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}
