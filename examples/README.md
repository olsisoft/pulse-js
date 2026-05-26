# Pulse TypeScript SDK — Examples

Five runnable examples showing how an application drives the **StreamFlow event
mesh** through Pulse. The SDK *declares* the work; Pulse runs it on the cluster
(sharded, replicated) — `app → SDK → Pulse API → bridge → mesh`.

## Use cases

| # | File | What it shows |
|---|------|---------------|
| 1 | [`realtime-windowed-aggregation.ts`](realtime-windowed-aggregation.ts) | Per-merchant 1-minute tumbling-window rollup (`count`/`sum`/`avg`/`max`) → topic |
| 2 | [`events-live-and-replay.ts`](events-live-and-replay.ts) | Tail the live event stream (`for await`) **and** replay a key's state history (time-travel) |
| 3 | [`interactive-query.ts`](interactive-query.ts) | Interactive Query — `summary` / point `get` / bounded `scan` / filtered + grouped `query` |
| 4 | [`ai-enrichment-pipeline.ts`](ai-enrichment-pipeline.ts) | Agentic stream — LLM sentiment → `extract` structured fields → MCP CRM lookup |
| 5 | [`stream-to-connector.ts`](stream-to-connector.ts) | Discover sink connectors, then `filter` → sink a stream to a ClickHouse connector |

## Prerequisites

- **Node 18+** and the SDK: `npm install @olsisoft/pulse-client`.
- A reachable **Pulse** instance — embedded mesh, or attached to a StreamFlow
  cluster (Settings → Data Plane → REMOTE).

## Run

```bash
export PULSE_URL=http://localhost:9090      # your Pulse base URL
export PULSE_TOKEN=...                       # only if your Pulse requires auth

npx tsx examples/realtime-windowed-aggregation.ts
npx tsx examples/events-live-and-replay.ts
npx tsx examples/interactive-query.ts
npx tsx examples/ai-enrichment-pipeline.ts
npx tsx examples/stream-to-connector.ts
```

## Type-check

The examples type-check against the SDK source via their own tsconfig:

```bash
npx tsc --noEmit -p examples/tsconfig.json
```
