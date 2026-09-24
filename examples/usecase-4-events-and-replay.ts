/**
 * Use-case ladder 4/5 — tail fraud alerts live, then replay one card's history.
 *
 * What it shows: subscribe to the live mesh event stream (SSE) and print a
 * handful of `fraud-alert` events — bounded with an AbortController so the
 * example terminates — then replay the committed state-change history for one
 * card (time-travel) and print it.
 *
 * Prerequisites:
 *   - The `card-velocity-60s` pipeline deployed (see use-case 2), emitting to
 *     the `fraud-alerts` topic.
 *   - Auth (events + replay are authenticated): PULSE_TOKEN, or PULSE_USER + PULSE_PASSWORD.
 *
 * Run:  PULSE_URL=http://localhost:9090 npx tsx examples/usecase-4-events-and-replay.ts
 */
import { PulseClient } from '@olsisoft/pulse-client';

const AGENT_ID = 'card-velocity-60s';
const MAX_ALERTS = 5;
const STREAM_TIMEOUT_MS = 15_000;

function isFraudAlert(event: Record<string, unknown>): boolean {
  const type = event.type ?? (event.payload as Record<string, unknown> | undefined)?.type;
  const topic = event.topic;
  return type === 'fraud-alert' || topic === 'fraud-alerts';
}

async function main(): Promise<void> {
  const client = new PulseClient({
    baseUrl: process.env.PULSE_URL ?? 'http://localhost:9090',
    token: process.env.PULSE_TOKEN,
  });

  const user = process.env.PULSE_USER;
  const password = process.env.PULSE_PASSWORD;
  if (!client.token && user && password) {
    await client.auth.login(user, password);
  }

  // Tail the live stream — stop after a few fraud-alert events OR the timeout.
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), STREAM_TIMEOUT_MS);
  let seen = 0;
  console.log(`Tailing live events for up to ${MAX_ALERTS} fraud-alert(s)…`);
  try {
    for await (const event of client.events.stream({ signal: ac.signal })) {
      if (!isFraudAlert(event)) continue;
      console.log('  fraud-alert:', event);
      if (++seen >= MAX_ALERTS) {
        ac.abort();
        break;
      }
    }
  } catch (err) {
    // AbortController.abort() surfaces as an AbortError — that's our stop signal.
    if ((err as Error).name !== 'AbortError') throw err;
  } finally {
    clearTimeout(timer);
  }
  console.log(`Saw ${seen} fraud-alert event(s).`);

  // Replay the committed state-change history for one card (time-travel).
  const changes = await client.events.replay({
    affectingState: AGENT_ID,
    key: 'card-007',
    from: '-1h',
    to: 'now',
    limit: 50,
  });
  console.log(`\nReplayed ${changes.length} state change(s) for card-007:`);
  for (const change of changes.slice(0, 5)) {
    console.log('  ', change);
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
