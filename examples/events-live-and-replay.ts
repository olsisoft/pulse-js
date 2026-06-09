/**
 * Use case 2 — consume live mesh events and replay state history.
 *
 *   * tail the live event stream (SSE) — bounded here to the first few events;
 *   * replay the committed state-change history for one key (time-travel).
 *
 * Run:  PULSE_URL=http://localhost:9090 npx tsx examples/events-live-and-replay.ts
 */
import { PulseClient } from '@olsisoft/pulse-client';

async function main(): Promise<void> {
  const client = new PulseClient({
    baseUrl: process.env.PULSE_URL ?? 'http://localhost:9090',
    token: process.env.PULSE_TOKEN,
  });

  // Replay the last hour of committed state changes for one account key.
  const changes = await client.events.replay({
    affectingState: 'balance',
    key: 'acct-42',
    from: '-1h',
    to: 'now',
    limit: 50,
  });
  console.log(`Replayed ${changes.length} state change(s) for acct-42`);
  for (const change of changes.slice(0, 5)) {
    console.log('  ', change);
  }

  // Tail the live event stream — stop after the first 10 events.
  const ac = new AbortController();
  let seen = 0;
  console.log('Tailing live events (first 10)…');
  for await (const event of client.events.stream({ signal: ac.signal })) {
    console.log('  event:', event);
    if (++seen >= 10) {
      ac.abort();
      break;
    }
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
