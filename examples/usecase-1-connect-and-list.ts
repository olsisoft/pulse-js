/**
 * Use-case ladder 1/5 — connect & list (card-payments fraud monitoring).
 *
 * What it shows: the hello-world handshake — construct the client, read the
 * server `version()`, optionally authenticate (degrading gracefully when no
 * credentials are set), then list the pipelines and connectors the Pulse
 * instance exposes. Connectivity smoke test for the rest of the ladder.
 *
 * Prerequisites:
 *   - A reachable Pulse at PULSE_URL (default http://localhost:9090).
 *   - Optional auth: PULSE_TOKEN, or PULSE_USER + PULSE_PASSWORD.
 *
 * Run:  PULSE_URL=http://localhost:9090 npx tsx examples/usecase-1-connect-and-list.ts
 */
import { PulseClient } from '@olsisoft/pulse-client';

async function main(): Promise<void> {
  const client = new PulseClient({
    baseUrl: process.env.PULSE_URL ?? 'http://localhost:9090',
    token: process.env.PULSE_TOKEN,
  });

  // version() is public — no JWT required. Good first call to prove reachability.
  console.log('Pulse version:', await client.version());

  // Authenticate if credentials are provided; otherwise degrade gracefully and
  // continue with whatever access an anonymous / token-bearing client has.
  const user = process.env.PULSE_USER;
  const password = process.env.PULSE_PASSWORD;
  if (!client.token && user && password) {
    const session = await client.auth.login(user, password);
    console.log('Logged in as', user, '— active org:', session.activeOrg ?? '(default)');
  } else if (client.token) {
    console.log('Using PULSE_TOKEN from the environment.');
  } else {
    console.log('No credentials set (PULSE_TOKEN or PULSE_USER/PULSE_PASSWORD) — anonymous mode.');
  }

  // List pipelines. Authenticated endpoints throw PulseAuthError without a
  // token; catch and report rather than crash so the smoke test stays useful.
  try {
    const pipelines = await client.pipelines.list();
    console.log(`\nPipelines (${pipelines.length}):`);
    for (const p of pipelines) {
      console.log('  -', p.name ?? p.id ?? p);
    }
  } catch (err) {
    console.log('\nCould not list pipelines:', (err as Error).message);
  }

  // List connectors — `{ sources: [...], sinks: [...] }`.
  try {
    const sinks = await client.connectors.sinks();
    const sources = await client.connectors.sources();
    console.log(`\nConnectors: ${sources.length} source(s), ${sinks.length} sink(s).`);
    for (const c of sinks.slice(0, 10)) {
      console.log('  sink:', c.subType ?? c.displayName ?? c);
    }
  } catch (err) {
    console.log('\nCould not list connectors:', (err as Error).message);
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
