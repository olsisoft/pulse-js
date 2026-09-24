/**
 * Use case 3 — Interactive Query over mesh-materialized agent state.
 *
 * Reads the live, queryable state an agent maintains on the mesh: a summary, a
 * point lookup by key, a bounded scan, and a filtered/grouped query.
 *
 * Run:  PULSE_URL=http://localhost:9090 npx tsx examples/interactive-query.ts
 */
import { PulseClient } from '@olsisoft/pulse-client';

const AGENT_ID = 'merchant-rollups-1m';

async function main(): Promise<void> {
  const client = new PulseClient({
    baseUrl: process.env.PULSE_URL ?? 'http://localhost:9090',
    token: process.env.PULSE_TOKEN,
  });

  console.log('Summary:', await client.iq.summary(AGENT_ID));

  // Point lookup for one merchant's current rollup.
  console.log('merchant-7:', await client.iq.get(AGENT_ID, 'merchant-7'));

  // Bounded scan of the keyspace.
  console.log('Scan (first 10):', await client.iq.scan(AGENT_ID, { limit: 10 }));

  // Filtered + grouped query.
  const result = await client.iq.query(AGENT_ID, {
    filter: { field: 'total_amount', op: 'gt', value: 1000 },
    groupBy: 'region',
    limit: 20,
  });
  console.log('High-volume merchants by region:', result);
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
