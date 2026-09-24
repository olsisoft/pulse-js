/**
 * Use-case ladder 5/5 — synchronous ALLOW/DENY decisions over a duplex channel.
 *
 * What it shows: the B-114 synchronous-decision path. Open ONE bidirectional
 * WebSocket to the `fraud-decider` agent, send a couple of charges in, and recv
 * the agent's correlated ALLOW/DENY decision back on the same connection — the
 * pattern a payment authorizer uses to gate a charge in-line, matched by
 * correlation id. One hot card (already over the velocity threshold) is expected
 * to DENY; one fresh card to ALLOW.
 *
 * Prerequisites:
 *   - A deployed `fraud-decider` decision agent reachable via duplex.
 *   - Node 22+ (global WebSocket), or `npm i ws` on older Node (optional dep).
 *   - Auth (duplex carries the JWT): PULSE_TOKEN, or PULSE_USER + PULSE_PASSWORD.
 *
 * Run:  PULSE_URL=http://localhost:9090 npx tsx examples/usecase-5-synchronous-decision.ts
 */
import { PulseClient } from '@olsisoft/pulse-client';

const AGENT_ID = 'fraud-decider';

interface Charge {
  cardId: string;
  amount: number;
}

/** The two charges we authorize: card-007 is hot (DENY), card-999 is fresh (ALLOW). */
const CHARGES: Charge[] = [
  { cardId: 'card-007', amount: 4200 },
  { cardId: 'card-999', amount: 19 },
];

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

  const channel = await client.duplex(AGENT_ID);
  try {
    for (const charge of CHARGES) {
      // Use the cardId as the correlation id so the reply is easy to match.
      const cid = await channel.send({ cardId: charge.cardId, amount: charge.amount }, charge.cardId);
      const signal = await channel.recv();

      const payload = (signal.payload ?? signal) as Record<string, unknown>;
      const decision = payload.decision ?? payload.action ?? '(unknown)';
      console.log(
        `charge ${charge.cardId} ($${charge.amount}) → ${decision}` +
          `  [sent cid=${cid}, reply cid=${signal.correlationId ?? '(none)'}]`,
      );
    }
  } finally {
    await channel.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
