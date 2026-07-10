/**
 * Use case 1 — real-time windowed aggregation on the event mesh.
 *
 * Declares a streaming pipeline that rolls up payment transactions per merchant
 * in 1-minute tumbling windows and writes the rollups back to a mesh topic.
 * Deployed to a Pulse attached to a StreamFlow cluster, this runs on the mesh
 * (sharded, replicated) — the SDK only declares the flow.
 *
 * Run:  PULSE_URL=http://localhost:9090 npx tsx examples/realtime-windowed-aggregation.ts
 */
import { PulseClient, StreamBuilder, windows, aggs } from '@olsisoft/pulse-client';

export function buildPipeline(): StreamBuilder {
  return new StreamBuilder('merchant-rollups-1m', {
    description: 'Per-merchant 1m transaction rollups',
  })
    .fromTopic('transactions')
    .filter('amount > 0')
    .keyBy('merchant_id')
    .window(windows.tumbling('1m'), {
      aggregations: {
        txnCount: aggs.count(),
        totalAmount: aggs.sum('amount'),
        avgAmount: aggs.avg('amount'),
        maxAmount: aggs.max('amount'),
      },
    })
    .toTopic('merchant-rollups-1m');
}

async function main(): Promise<void> {
  const builder = buildPipeline();
  console.log('Pipeline spec:', builder.build());

  const client = new PulseClient({
    baseUrl: process.env.PULSE_URL ?? 'http://localhost:9090',
    token: process.env.PULSE_TOKEN,
  });
  console.log('Deployed:', await client.streams.deploy(builder));
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
