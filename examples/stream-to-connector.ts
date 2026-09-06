/**
 * Use case 5 — sink a mesh stream to an external connector.
 *
 * Discovers the available sink connectors, then declares a stream that delivers
 * the per-merchant rollups to a ClickHouse warehouse via a connector sink.
 *
 * Run:  PULSE_URL=http://localhost:9090 npx tsx examples/stream-to-connector.ts
 */
import { PulseClient, StreamBuilder } from '@olsisoft/pulse-client';

export function buildPipeline(): StreamBuilder {
  return new StreamBuilder('rollups-to-warehouse', {
    description: 'Ship 1m rollups to ClickHouse',
  })
    .fromTopic('merchant-rollups-1m')
    .filter('total_amount > 0')
    .toConnector('clickhouse', { url: 'http://clickhouse:8123', table: 'merchant_rollups' });
}

async function main(): Promise<void> {
  const client = new PulseClient({
    baseUrl: process.env.PULSE_URL ?? 'http://localhost:9090',
    token: process.env.PULSE_TOKEN,
  });

  const sinks = await client.connectors.sinks();
  console.log(`${sinks.length} sink connector(s) available`);

  const builder = buildPipeline();
  console.log('Pipeline spec:', builder.build());
  console.log('Deployed:', await client.streams.deploy(builder));
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
