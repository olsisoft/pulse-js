/**
 * Use-case ladder 3/5 — interactive query the card-velocity state.
 *
 * What it shows: query the live, mesh-materialized state of the
 * `card-velocity-60s` agent like a database — a headline summary, a filtered
 * query for the cards currently over the fraud threshold (txCount > 5), and a
 * point get for one card. The IQ surface does NOT auto-retry on rate limits, so
 * the filtered query is wrapped in a small caller-side retry that honours
 * `retryAfterSeconds` from PulseRateLimitError.
 *
 * Prerequisites:
 *   - The `card-velocity-60s` pipeline deployed (see use-case 2).
 *   - Auth (IQ requires AGENT_READ): PULSE_TOKEN, or PULSE_USER + PULSE_PASSWORD.
 *
 * Run:  PULSE_URL=http://localhost:9090 npx tsx examples/usecase-3-interactive-query.ts
 */
import { PulseClient, PulseRateLimitError } from '@olsisoft/pulse-client';

const AGENT_ID = 'card-velocity-60s';

/** Sleep helper for the rate-limit backoff. */
function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Run `fn`, retrying on PulseRateLimitError up to `maxAttempts` times, waiting
 * the server-suggested `retryAfterSeconds` between tries. The SDK never retries
 * automatically — this is the explicit caller-side pattern.
 */
async function withRateLimitRetry<T>(fn: () => Promise<T>, maxAttempts = 3): Promise<T> {
  let attempt = 0;
  for (;;) {
    try {
      return await fn();
    } catch (err) {
      if (err instanceof PulseRateLimitError && attempt < maxAttempts - 1) {
        attempt += 1;
        const waitSec = err.retryAfterSeconds ?? 1;
        console.log(`  rate-limited — retrying in ${waitSec}s (attempt ${attempt})`);
        await delay(waitSec * 1000);
        continue;
      }
      throw err;
    }
  }
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

  // Headline summary of the agent's keyed state.
  console.log('Summary:', await client.iq.summary(AGENT_ID));

  // Filtered query: every card currently over the fraud threshold.
  const flagged = await withRateLimitRetry(() =>
    client.iq.query(AGENT_ID, {
      filter: { field: 'txCount', op: 'gt', value: 5 },
      limit: 20,
    }),
  );
  console.log('Cards over the velocity threshold (txCount > 5):', flagged);

  // Point get for one specific card's current window state.
  console.log('card-007:', await client.iq.get(AGENT_ID, 'card-007'));
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
