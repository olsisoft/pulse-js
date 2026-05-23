# @olsisoft/pulse-client — JavaScript / TypeScript SDK for StreamFlow Pulse

Official client for the [Pulse](https://github.com/olsisoft/streamflow) AI Agent Platform.

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

## Supported surfaces (v2.5.8)

| Resource | Methods | Notes |
|---|---|---|
| `client.auth` | `login()`, `refresh()`, `organizations()`, `switchOrg()` | Auto-caches JWT after `login` / `refresh` / `switchOrg`. |
| `client.pipelines` | `list()`, `get(id)`, `create(definition)`, `delete(id)` | `definition` follows the `CreatePipelineRequest` schema (see OpenAPI spec). |
| `client.agents` | `list()`, `get(id)` | Read-only — agents are owned by pipelines. |
| `client.templates` | `list()` | The 223+ first-party templates. |
| `client.users` | `list()` | Requires `USERS_LIST` permission (Owner / Platform Admin personas). |
| `client.version()` | top-level | Public — no JWT required. |

The full ~112-endpoint surface is documented in the OpenAPI spec at `<pulse-server>/api-docs`. SDK methods for the rest land opportunistically as user-facing demand surfaces.

## Authentication

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
git clone https://github.com/olsisoft/streamflow.git
cd streamflow/pulse-js

npm install
npm run typecheck    # tsc --noEmit
npm run test         # vitest run
npm run test:watch   # vitest in watch mode
npm run build        # tsup → dist/ (ESM + CJS + .d.ts)
```

CI runs the same on every push touching `pulse-js/` — see `.github/workflows/pulse-js.yaml`.

## Roadmap

- **v2.5.x** — current API, 5 core resources (auth, pipelines, agents, templates, users), `version()`.
- **v2.6.x** — expanded resource coverage: backups, schedules, credentials, settings, approvals, chat.
- **v3.0** — event-stream consumer (`client.events.stream()` returns an async iterable wrapping the SSE endpoint at `/api/pulse/events/stream`).
- **B-098 satellite** — once `olsisoft/pulse-js` exists as its own repo, this in-tree code lifts out wholesale. `npm install` will switch to the satellite; in-tree continues to mirror for one release cycle so the migration is non-breaking.

Track progress in [`docs/STREAMFLOW-BACKLOG.md`](../docs/STREAMFLOW-BACKLOG.md) under item **B-098**.

## License

Apache 2.0 — same as the parent Pulse repository.
