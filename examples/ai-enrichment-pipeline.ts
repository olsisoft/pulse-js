/**
 * Use case 4 — agentic enrichment pipeline (LLM + extract + MCP) on the mesh.
 *
 * Enriches support tickets streaming through the mesh: classify sentiment with
 * an LLM, pull structured fields out of free text, then call an MCP tool to
 * look the customer up — a declarative stream that runs on the cluster.
 *
 * Run:  PULSE_URL=http://localhost:9090 npx tsx examples/ai-enrichment-pipeline.ts
 */
import { PulseClient, StreamBuilder } from '@olsisoft/pulse-client';

export function buildPipeline(): StreamBuilder {
  return new StreamBuilder('ticket-enrichment', {
    description: 'LLM + MCP enrichment of support tickets',
  })
    .fromTopic('support-tickets')
    .filter("priority != 'spam'")
    .mapLlm('Classify the ticket sentiment as positive, neutral, or negative.', {
      outputField: 'sentiment',
    })
    .extract({
      instruction: "Extract the product name and the customer's requested action.",
      schema: { product: 'string', requested_action: 'string' },
    })
    .mcpCall('crm.lookup_customer', {
      args: { email: '${customer_email}' },
      outputField: 'customer',
    })
    .toTopic('tickets-enriched');
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
