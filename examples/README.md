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

## Use-case ladder (simplest → most complex)

Five graduated examples that share ONE domain — **card-payments fraud
monitoring**. The event topic is `card-authorizations` carrying
`{cardId, merchantId, amount, ts}`; the fraud rule is *more than 5
authorizations on one card in a 60s tumbling window*. Each example builds on
the previous one, from a connectivity smoke test up to a synchronous
ALLOW/DENY decision channel.

| # | File | What it shows |
|---|------|---------------|
| 1 | [`usecase-1-connect-and-list.ts`](usecase-1-connect-and-list.ts) | Connect, read `version()`, optionally log in, then list pipelines + connectors (hello-world) |
| 2 | [`usecase-2-deploy-velocity-pipeline.ts`](usecase-2-deploy-velocity-pipeline.ts) | Build `card-velocity-60s` with the StreamBuilder DSL — 60s tumbling window, `txCount > 5` → `fraud-alerts`; compile offline then `deploy()` |
| 3 | [`usecase-3-interactive-query.ts`](usecase-3-interactive-query.ts) | IQ the agent state — summary, filtered query (`txCount > 5`), point get, with caller-side rate-limit retry |
| 4 | [`usecase-4-events-and-replay.ts`](usecase-4-events-and-replay.ts) | Tail live `fraud-alert` events (bounded via `AbortController`) then replay one card's state history |
| 5 | [`usecase-5-synchronous-decision.ts`](usecase-5-synchronous-decision.ts) | Open a duplex channel to `fraud-decider`, send charges, recv ALLOW/DENY + correlation id (B-114) |

These examples talk to a **live Pulse** at `PULSE_URL` (default
`http://localhost:9090`); set `PULSE_TOKEN`, or `PULSE_USER` + `PULSE_PASSWORD`,
for the authenticated steps (deploy / IQ / events / duplex). Run them in order:

```bash
npm install && npm run build                 # the examples import the built @olsisoft/pulse-client

export PULSE_URL=http://localhost:9090
export PULSE_TOKEN=...                       # or PULSE_USER + PULSE_PASSWORD

npx tsx examples/usecase-1-connect-and-list.ts
npx tsx examples/usecase-2-deploy-velocity-pipeline.ts
npx tsx examples/usecase-3-interactive-query.ts
npx tsx examples/usecase-4-events-and-replay.ts
npx tsx examples/usecase-5-synchronous-decision.ts
```

## Type-check

The examples type-check against the SDK source via their own tsconfig:

```bash
npx tsc --noEmit -p examples/tsconfig.json
```
