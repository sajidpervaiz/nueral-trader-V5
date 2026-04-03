import { FormEvent, useEffect, useMemo, useState } from "react";

type LatestEvent = {
  request_id: string;
  model: string;
  selected_tool: string;
  latency_ms: number;
  status: string;
  created_at: string;
} | null;

const apiBase = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8080";
const pollMs = Number.parseInt(import.meta.env.VITE_POLL_MS ?? "3000", 10);

export function App() {
  const [requestId, setRequestId] = useState("");
  const [status, setStatus] = useState("idle");
  const [latest, setLatest] = useState<LatestEvent>(null);
  const [error, setError] = useState<string | null>(null);

  const displayRequestId = useMemo(() => requestId || `req-${Date.now()}`, [requestId]);

  async function loadLatest() {
    try {
      const response = await fetch(`${apiBase}/api/tool-selection/latest`);
      if (!response.ok) throw new Error(`latest fetch failed (${response.status})`);
      const data = await response.json();
      setLatest(data);
      setError(null);
    } catch (err: any) {
      setError(err?.message ?? "unknown error");
    }
  }

  useEffect(() => {
    void loadLatest();
    const timer = setInterval(() => void loadLatest(), pollMs);
    return () => clearInterval(timer);
  }, []);

  async function onSubmit(e: FormEvent) {
    e.preventDefault();
    setStatus("submitting");
    try {
      const response = await fetch(`${apiBase}/api/tool-selection`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ request_id: displayRequestId }),
      });
      if (!response.ok) throw new Error(`submit failed (${response.status})`);
      const data = await response.json();
      setStatus(`submitted: ${data.status}`);
      setRequestId(data.request_id);
      await loadLatest();
    } catch (err: any) {
      setStatus("failed");
      setError(err?.message ?? "unknown error");
    }
  }

  return (
    <main style={{ maxWidth: 800, margin: "2rem auto", fontFamily: "sans-serif", padding: "0 1rem" }}>
      <h1>NUERAL-TRADER-5 UI</h1>
      <p>Minimal production UI over Bridge API.</p>

      <section style={{ margin: "1.5rem 0", padding: "1rem", border: "1px solid #ddd", borderRadius: 8 }}>
        <h2>Submit Tool Selection</h2>
        <form onSubmit={onSubmit}>
          <label htmlFor="requestId">Request ID</label>
          <input
            id="requestId"
            value={requestId}
            onChange={(e) => setRequestId(e.target.value)}
            placeholder="req-123"
            style={{ display: "block", width: "100%", marginTop: 8, marginBottom: 12, padding: 8 }}
          />
          <button type="submit">Submit</button>
        </form>
        <p>Status: <strong>{status}</strong></p>
        {error ? <p style={{ color: "crimson" }}>Error: {error}</p> : null}
      </section>

      <section style={{ margin: "1.5rem 0", padding: "1rem", border: "1px solid #ddd", borderRadius: 8 }}>
        <h2>Latest Event</h2>
        {latest ? (
          <ul>
            <li>request_id: {latest.request_id}</li>
            <li>model: {latest.model}</li>
            <li>selected_tool: {latest.selected_tool}</li>
            <li>latency_ms: {latest.latency_ms}</li>
            <li>status: {latest.status}</li>
            <li>created_at: {latest.created_at}</li>
          </ul>
        ) : (
          <p>No events yet.</p>
        )}
      </section>
    </main>
  );
}
