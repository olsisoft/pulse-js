/**
 * Use-case ladder 2/5 — deploy the card-velocity fraud pipeline.
 *
 * What it shows: build a streaming pipeline with the StreamBuilder DSL that
 * counts card authorizations in a 60s tumbling window and raises an alert when
 * one card sees more than 5 — the classic card-velocity fraud rule. Compiles
 * the pipeline offline (prints the JSON the server would receive) and then
 * deploys it to the mesh.
 *
 * Domain: topic `card-authorizations` carrying {cardId, merchantId, amount, ts}.
 * Rule: more than 5 authorizations on one card in any 60s tumbling window.
 *
 * Prerequisites:
 *   - A reachable Pulse at PULSE_URL (default http://localhost:9090).
 *   - Auth (deploy is an authenticated write): PULSE_TOKEN, or PULSE_USER + PULSE_PASSWORD.
 *
 * Run:  PULSE_URL=http://localhost:9090 npx tsx examples/usecase-2-deploy-velocity-pipeline.ts
 */
import { PulseClient, StreamBuilder, windows, aggs } from '@olsisoft/pulse-client';

export function buildPipeline(): StreamBuilder {
  return new StreamBuilder('card-velocity-60s', {
    description: 'Alert when one card sees > 5 authorizations in a 60s window',
  })
    .fromTopic('card-authorizations')
    .filter('amount > 0')
    .keyBy('cardId')
    .window(windows.tumbling('60s'), {
      aggregations: {
        txCount: aggs.count(),
        totalAmount: aggs.sum('amount'),
        maxAmount: aggs.max('amount'),
      },
    })
    .filter('txCount > 5')
    .toTopic('fraud-alerts', { sinkChannel: 'dashboard' });
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

  const builder = buildPipeline();

  // Compile offline first — no server call — to inspect the exact pipeline JSON.
  console.log('Compiled pipeline (offline):');
  console.log(JSON.stringify(client.streams.compile(builder), null, 2));

  // Deploy it to the mesh.
  console.log('\nDeploying…');
  console.log('Deployed:', await client.streams.deploy(builder));
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
