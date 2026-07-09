# @olsisoft/pulse-client — JavaScript / TypeScript SDK for StreamFlow Pulse

Official client for the [Pulse](https://github.com/olsisoft/pulse-js) AI Agent Platform.

```ts
import { PulseClient } from '@olsisoft/pulse-client';

const client = new PulseClient({ baseUrl: 'http://localhost:9090' });
await client.auth.login('alice', 'secret');

for (const pipeline of await client.pipelines.list()) {
  console.log(pipeline.name);
}
```

## Install

```bash
npm install @olsisoft/pulse-client
```

Works on **Node 20+** (native `fetch`) and **modern browsers**. Zero runtime dependencies. Ships ESM + CJS + `.d.ts` so TypeScript consumers get full type completion without installing `@types/anything`.

## Why @olsisoft/pulse-client

- **Environment-agnostic** — single package for Node services, serverless functions, Vite/Next.js apps, browser scripts. Pass the JWT as a constructor arg; no cookies / no `localStorage` coupling.
- **Lightweight** — pure TypeScript, single file at runtime (~9 kB ESM gzipped), zero peer deps. No Axios. No node-fetch polyfill. Just `fetch`.
- **Spec-aligned** — every method corresponds 1:1 to an endpoint in the [Pulse OpenAPI 3.1 spec](../streamflow-pulse/src/main/resources/openapi/openapi.yaml). Drift caught at PR time by the in-tree invariant tests (B-103).
- **Typed errors** — `PulseAuthError` (401), `PulseNotFoundError` (404), `PulseValidationError` (400), `PulseRateLimitError` (429, with `.retryAfterSeconds`), `PulseAPIError` (everything else). All extend `PulseClientError`.

## Quick start

```ts
import { PulseClient, PulseAuthError } from '@olsisoft/pulse-client';

const client = new PulseClient({ baseUrl: 'http://localhost:9090' });

// Authenticate — JWT is cached on the client automatically
try {
  const response = await client.auth.login('alice', 'secret');
  console.log(`Logged in as ${response.user?.username}`);
} catch (e) {
  if (e instanceof PulseAuthError) console.error('Login failed:', e.message);
  else throw e;
}

// List + inspect
for (const pipeline of await client.pipelines.list()) {
  console.log(pipeline.name, pipeline.status);
}

// Create a pipeline from a template
const newPipeline = await client.pipelines.create({
  name: 'my-fraud-detector',
  templateId: 'fintech-fraud-detection-realtime',
  nodes: [
    { id: 'source', type: 'source', subType: 'kafka-source' },
    { id: 'agent',  type: 'agent',  subType: 'streaming' },
    { id: 'sink',   type: 'sink',   subType: 'telegram' },
  ],
});
console.log('created', newPipeline.id);

// Inspect deployed agents
for (const agent of await client.agents.list()) {
  console.log(`  ${agent.name} — ${agent.engineType} — ${agent.status}`);
}
```

## Supported surfaces (v2.7.x)

| Resource | Methods | Notes |
|---|---|---|
| `client.auth` | `login()`, `refresh()`, `organizations()`, `switchOrg()` | Auto-caches JWT after `login` / `refresh` / `switchOrg`. |
| `client.pipelines` | `list()`, `get(id)`, `create(definition)`, `delete(id)` | `definition` follows the `CreatePipelineRequest` schema (see OpenAPI spec). |
| `client.agents` | `list()`, `get(id)` | Read-only — agents are owned by pipelines. |
| `client.templates` | `list()` | The 223+ first-party templates. |
| `client.users` | `list()` | Requires `USERS_LIST` permission (Owner / Platform Admin personas). |
| `client.version()` | top-level | Public — no JWT required. |

The full ~112-endpoint surface is documented in the OpenAPI spec at `<pulse-server>/api-docs`. SDK methods for the rest land opportunistically as user-facing demand surfaces.

## Embedded ML inference & duplex

Score events with an uploaded ONNX model in-process (B-112), and open a
bidirectional duplex channel for synchronous decisions (B-114). Full guide:
[ML inference & duplex](https://github.com/olsisoft/pulse-js/blob/dev/docs/SDK-ML-INFERENCE-AND-DUPLEX.md).

```ts
// Upload + score with an ONNX model (no model-server hop)
await client.models.upload({ name: 'fraud', path: './fraud.onnx',
  inputSchema: { amount: 'float', country: 'float' } });
builder.fromTopic('transactions')
  .mlPredict({ model: 'fraud', inputFields: ['amount', 'country'], outputField: 'prediction' })
  .filter('prediction.fraud_score > 0.8').toTopic('flagged');

// Duplex: one connection, send in / receive the correlated output
// (global WebSocket on Node 22+/browsers; `npm i ws` on Node < 22)
const ch = await client.duplex('fraud-detector');
await ch.send({ amount: 5000 }, 'tx-1');
const signal = await ch.recv();   // signal.correlationId === 'tx-1'
await ch.close();
```

## Sandboxed WASM operators

Run an uploaded WebAssembly module over each event (B-110), sandboxed in
pure-Java Chicory on the engine — no host syscalls, bounded linear memory.
Any `wasm32` toolchain (Rust, TinyGo, AssemblyScript, C) can author a module
against the alloc/process ABI; upload / delete require the ADMIN role.

```ts
// Upload a module, then run it inline in a stream
await client.wasm.upload({ name: 'pii-redactor', path: './redactor.wasm' });
builder.fromTopic('events')
  .wasm({ module: 'pii-redactor', parallelism: 4, onFailure: 'PASS_THROUGH' })
  .toTopic('clean');
```

**Legacy formats & protocols — the headline use case.** Compile *any* existing
parser to `wasm32` and drop it in as a single-message transform to bring legacy
data into the pipeline — **COBOL copybooks**, FIX, HL7, EDI X12, ASN.1, Modbus, …
You don't rewrite the parser, you wrap it (see the `pulse-wasm-guest` guest SDK for
the Rust/TinyGo/AssemblyScript/C operator ABI). Pair it with `.mlPredict()` (ONNX
above) to parse *and* score each event in-stream, with no external service.

## Authentication

### Where credentials come from

The SDK authenticates as a **Pulse user** — there are no separate API keys to
provision. A username + password (or a JWT minted from them) is all you need,
and they live in **your own Pulse instance**, not on streamflowmesh.io.

1. **First run → bootstrap admin.** The very first account is created either by
   the first-run screen of the Pulse web/desktop app, or by a single
   *unauthenticated* `POST /api/auth/register` with a `{"username","password"}`
   body **while no user exists yet**. That first user is granted **ADMIN**. As
   soon as any user exists, `/api/auth/register` locks down and requires an admin
   JWT — so the open bootstrap can only ever mint the very first account.
2. **Additional users.** An admin creates more accounts from **Settings → Users**
   in the Pulse UI (or an admin-authenticated `register` call). Give each CI job
   or service integration its own dedicated user rather than sharing the admin.
3. **Exchange for a token.** `auth.login(username, password)` returns a
   short-lived **access JWT** (~1 h TTL) plus a **refresh token**; the client
   caches the access token automatically. In CI, either call `login` at startup,
   or pass a pre-minted JWT (pattern 2 below) and refresh it before it expires.

`baseUrl` points at *your* Pulse server — `http://localhost:9090` for a local
`pulse --headless` or desktop install, or your deployed Pulse URL.

### Passing the token to the client

```ts
// 1. Username + password (interactive / CLI tools)
const client = new PulseClient({ baseUrl: 'http://localhost:9090' });
await client.auth.login('alice', 'secret');

// 2. Pre-minted JWT (CI / service accounts)
const client = new PulseClient({
  baseUrl: 'http://localhost:9090',
  token: 'ey...',
});

// 3. JWT from environment (12-factor apps)
const client = new PulseClient({
  baseUrl: process.env.PULSE_URL!,
  token: process.env.PULSE_TOKEN,
});
```

For long-running daemons, store the `refreshToken` from `login()` and call `client.auth.refresh(refreshToken)` when the JWT nears expiry (default 1 h TTL).

## Error handling

```ts
import {
  PulseClientError,    // base — catches every client-side error
  PulseAuthError,      // 401 — invalid / missing / expired JWT
  PulseNotFoundError,  // 404
  PulseValidationError,// 400 — malformed request
  PulseRateLimitError, // 429 — carries .retryAfterSeconds
  PulseAPIError,       // everything else (5xx, etc.)
} from '@olsisoft/pulse-client';

try {
  await client.pipelines.get('nope');
} catch (e) {
  if (e instanceof PulseNotFoundError) {
    console.log("Doesn't exist — fine");
  } else if (e instanceof PulseRateLimitError) {
    const wait = (e.retryAfterSeconds ?? 60) * 1000;
    console.log(`Backing off ${wait}ms`);
    await new Promise((r) => setTimeout(r, wait));
  } else if (e instanceof PulseClientError) {
    console.error('Pulse error:', e.message);
  } else {
    throw e;
  }
}
```

Every exception carries `.statusCode`, `.path`, and `.body` so log lines + bug reports are actionable.

## Browser / Edge runtime usage

The package ships ESM as the primary build, so modern bundlers (Vite, Next.js, esbuild, webpack 5+) tree-shake unused resources cleanly. No node-specific imports → works in:

- Browser (via bundler — Vite, Next.js, etc.)
- Cloudflare Workers / Pages
- Deno (via `npm:` specifier or `esm.sh`)
- Bun
- Node 20+

If you're on a runtime that doesn't expose a global `fetch`, pass your own:

```ts
import nodeFetch from 'node-fetch';
const client = new PulseClient({
  baseUrl: 'http://localhost:9090',
  fetch: nodeFetch as unknown as typeof fetch,
});
```

## Development

```bash
git clone https://github.com/olsisoft/pulse-js.git
cd pulse-js

npm install
npm run typecheck    # tsc --noEmit
npm run test         # vitest run
npm run test:watch   # vitest in watch mode
npm run build        # tsup → dist/ (ESM + CJS + .d.ts)
```

CI runs the same on every push touching `pulse-js/` — see `.github/workflows/pulse-js.yaml`.

## Automatic retry (opt-in)

Off by default — one attempt per request. Enable bounded, full-jitter
exponential-backoff retries via the constructor options:

```ts
const client = new PulseClient({
  baseUrl: 'http://localhost:9090',
  maxRetries: 3, // 0 = off (default)
});
```

429 (rate limited) is retried for any method, honouring `Retry-After`;
`retryOnStatus` 5xx (default `502/503/504`) and transport errors are retried only
for idempotent methods (GET/HEAD/PUT/DELETE) unless `retryNonIdempotent`; terminal
4xx are never retried.

## Local pipeline simulation (Python-only today)

The streams DSL is **client-side declaration, server-side execution**:
`streams.compile(builder)` builds the pipeline JSON locally (no network) and
`streams.deploy(builder)` runs it on the Pulse engine. This SDK has **no
in-process simulator** — to validate a pipeline before deploy, `compile()` and
inspect the JSON, or deploy to a dev Pulse.

> A local `TopologyTestDriver`-style executor that runs a streams pipeline
> in-process over sample events (`StreamBuilder.simulate(events)`) currently
> exists **only in the Python SDK** (`streamflow-pulse-client`). Cross-language
> parity is tracked as **B-169** (issue #311); until then, local simulation is a
> Python-exclusive capability.

## Roadmap

- **v2.5.x** — current API, 5 core resources (auth, pipelines, agents, templates, users), `version()`.
- **v2.6.x** — expanded resource coverage: backups, schedules, credentials, settings, approvals, chat.
- **v3.0** — event-stream consumer (`client.events.stream()` returns an async iterable wrapping the SSE endpoint at `/api/pulse/events/stream`).
- **B-098 satellite** — once `olsisoft/pulse-js` exists as its own repo, this in-tree code lifts out wholesale. `npm install` will switch to the satellite; in-tree continues to mirror for one release cycle so the migration is non-breaking.

Track progress in [`docs/STREAMFLOW-BACKLOG.md`](../docs/STREAMFLOW-BACKLOG.md) under item **B-098**.

## License

Apache 2.0 — same as the parent Pulse repository.
