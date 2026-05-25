/**
 * Tests for B-107 streams DSL.
 *
 * Coverage strategy mirrors pulse-py's test_streams.py: every operator method
 * is exercised at three layers — constructor validation, compiled shape, and
 * one round-trip test that rebuilds the canonical iot-temperature-aggregator
 * template from scratch via the DSL.
 */

import { http, HttpResponse } from 'msw';
import { setupServer } from 'msw/node';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import {
  aggs,
  PulseClient,
  StreamBuilder,
  StreamsResource,
  WindowSpec,
  windows,
} from '../src/index.js';

const BASE_URL = 'http://pulse.test:9090';

const server = setupServer();
beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

function newClient(token?: string): PulseClient {
  return new PulseClient({ baseUrl: BASE_URL, token });
}

// ---------------------------------------------------------------------------
// Window-spec factories
// ---------------------------------------------------------------------------

describe('windows', () => {
  it('tumbling emits expected string', () => {
    expect(windows.tumbling('60s').spec).toBe('tumbling(60s)');
  });

  it('sliding emits expected string', () => {
    expect(windows.sliding('10m', '1m').spec).toBe('sliding(10m,1m)');
  });

  it('session emits expected string', () => {
    expect(windows.session('30s').spec).toBe('session(30s)');
  });

  it('globalWin emits expected string', () => {
    expect(windows.globalWin().spec).toBe('global');
  });

  it('count emits expected string', () => {
    expect(windows.count(100).spec).toBe('count(100)');
  });

  it('countSliding emits expected string', () => {
    expect(windows.countSliding(100, 10).spec).toBe('count_sliding(100,10)');
  });

  it('tumbling rejects blank', () => {
    expect(() => windows.tumbling('')).toThrow(/size/);
  });

  it('sliding rejects blank either arg', () => {
    expect(() => windows.sliding('10m', '')).toThrow(/slide/);
    expect(() => windows.sliding('   ', '1m')).toThrow(/size/);
  });

  it('count rejects non-positive or non-integer', () => {
    expect(() => windows.count(0)).toThrow(/positive/);
    expect(() => windows.count(-5)).toThrow(/positive/);
    expect(() => windows.count(1.5)).toThrow(/integer/);
  });

  it('countSliding rejects non-positive or non-integer', () => {
    expect(() => windows.countSliding(100, 0)).toThrow(/positive/);
    expect(() => windows.countSliding(0, 10)).toThrow(/positive/);
    expect(() => windows.countSliding(1.5, 10)).toThrow(/integer/);
  });

  it('WindowSpec toString returns spec', () => {
    expect(windows.tumbling('60s').toString()).toBe('tumbling(60s)');
  });

  it('WindowSpec rejects blank', () => {
    expect(() => new WindowSpec('')).toThrow(/non-empty/);
    expect(() => new WindowSpec('   ')).toThrow(/non-empty/);
  });
});

// ---------------------------------------------------------------------------
// Aggregator factories
// ---------------------------------------------------------------------------

describe('aggs', () => {
  it('count takes no field', () => {
    expect(aggs.count()).toBe('count()');
  });

  it('builders emit expected strings', () => {
    expect(aggs.sum('amount')).toBe('sum(amount)');
    expect(aggs.avg('price')).toBe('avg(price)');
    expect(aggs.min('latency')).toBe('min(latency)');
    expect(aggs.max('latency')).toBe('max(latency)');
    expect(aggs.collectList('sku')).toBe('collect_list(sku)');
    expect(aggs.distinctCount('user_id')).toBe('distinct_count(user_id)');
  });

  it('aggregators reject blank field', () => {
    const builders = [aggs.sum, aggs.avg, aggs.min, aggs.max, aggs.collectList, aggs.distinctCount];
    for (const fn of builders) {
      expect(() => fn('')).toThrow(/field/);
      expect(() => fn('   ')).toThrow(/field/);
    }
  });
});

// ---------------------------------------------------------------------------
// StreamBuilder — per-operator shape
// ---------------------------------------------------------------------------

describe('StreamBuilder operators', () => {
  it('filter emits validator shape', () => {
    const b = new StreamBuilder().fromTopic('in').filter('amount > 1000');
    expect(b.operators()).toEqual([{ type: 'filter', condition: 'amount > 1000' }]);
  });

  it('filter rejects blank condition', () => {
    expect(() => new StreamBuilder().fromTopic('in').filter('')).toThrow(/condition/);
  });

  it('map with fields only', () => {
    const b = new StreamBuilder().fromTopic('in').map({ fields: { alert: "concat(id, '!')" } });
    expect(b.operators()).toEqual([{ type: 'map', fields: { alert: "concat(id, '!')" } }]);
  });

  it('map with targetType only', () => {
    const b = new StreamBuilder().fromTopic('in').map({ targetType: 'alert' });
    expect(b.operators()).toEqual([{ type: 'map', targetType: 'alert' }]);
  });

  it('map with both', () => {
    const b = new StreamBuilder().fromTopic('in').map({ fields: { x: '1' }, targetType: 'alert' });
    expect(b.operators()).toEqual([
      { type: 'map', fields: { x: '1' }, targetType: 'alert' },
    ]);
  });

  it('map rejects empty', () => {
    expect(() => new StreamBuilder().fromTopic('in').map({})).toThrow(/does nothing/);
  });

  it('flatMap emits validator shape', () => {
    const b = new StreamBuilder().fromTopic('in').flatMap('items');
    expect(b.operators()).toEqual([{ type: 'flatMap', splitField: 'items' }]);
  });

  it('flatMap rejects blank', () => {
    expect(() => new StreamBuilder().fromTopic('in').flatMap('')).toThrow(/splitField/);
  });

  it('keyBy emits validator shape', () => {
    const b = new StreamBuilder().fromTopic('in').keyBy('deviceId');
    expect(b.operators()).toEqual([{ type: 'keyBy', field: 'deviceId' }]);
  });

  it('keyBy rejects blank', () => {
    expect(() => new StreamBuilder().fromTopic('in').keyBy('')).toThrow(/field/);
  });

  it('window with aggregations', () => {
    const b = new StreamBuilder()
      .fromTopic('in')
      .window(windows.tumbling('60s'), {
        aggregations: { avgTemp: aggs.avg('temperature') },
      });
    expect(b.operators()).toEqual([
      {
        type: 'window',
        spec: 'tumbling(60s)',
        aggregations: { avgTemp: 'avg(temperature)' },
      },
    ]);
  });

  it('window accepts raw string spec', () => {
    const b = new StreamBuilder().fromTopic('in').window('sliding(10m,1m)');
    expect(b.operators()).toEqual([{ type: 'window', spec: 'sliding(10m,1m)' }]);
  });

  it('window with outputTopic and trigger', () => {
    const b = new StreamBuilder().fromTopic('in').window(windows.tumbling('60s'), {
      outputTopic: 'late-data',
      trigger: { kind: 'earlyFire', afterEvents: 10 },
    });
    const op = b.operators()[0]!;
    expect(op.outputTopic).toBe('late-data');
    expect(op.trigger).toEqual({ kind: 'earlyFire', afterEvents: 10 });
  });

  it('window rejects blank spec string', () => {
    expect(() => new StreamBuilder().fromTopic('in').window('')).toThrow(/spec/);
  });

  it('branch emits validator shape', () => {
    const b = new StreamBuilder().fromTopic('in').branch([
      { condition: "tier == 'gold'", topic: 'vip-events' },
      { condition: "tier == 'silver'", topic: 'std-events' },
    ]);
    expect(b.operators()).toEqual([
      {
        type: 'branch',
        branches: [
          { condition: "tier == 'gold'", topic: 'vip-events' },
          { condition: "tier == 'silver'", topic: 'std-events' },
        ],
      },
    ]);
  });

  it('branch rejects empty', () => {
    expect(() => new StreamBuilder().fromTopic('in').branch([])).toThrow(/at least one/);
  });

  it('branch rejects missing fields', () => {
    expect(() =>
      new StreamBuilder().fromTopic('in').branch([{ topic: 'x' } as never]),
    ).toThrow(/condition/);
    expect(() =>
      new StreamBuilder().fromTopic('in').branch([{ condition: 'x > 0' } as never]),
    ).toThrow(/topic/);
  });

  it('enrich emits validator shape', () => {
    const b = new StreamBuilder()
      .fromTopic('in')
      .enrich({ lookupTopic: 'customers', keyField: 'customerId' });
    expect(b.operators()).toEqual([
      { type: 'enrich', lookupTopic: 'customers', keyField: 'customerId' },
    ]);
  });

  it('enrich rejects blank args', () => {
    expect(() =>
      new StreamBuilder().fromTopic('in').enrich({ lookupTopic: '', keyField: 'k' }),
    ).toThrow();
    expect(() =>
      new StreamBuilder().fromTopic('in').enrich({ lookupTopic: 't', keyField: '' }),
    ).toThrow();
  });

  it('enrichAsync full shape', () => {
    const b = new StreamBuilder().fromTopic('in').enrichAsync({
      url: 'https://x.example/lookup/{id}',
      parallelism: 8,
      queueSize: 128,
      timeoutMs: 5000,
      maxRetries: 3,
      retryBackoffMs: 200,
      ordering: 'PRESERVE_INPUT',
      onFailure: 'EMIT_ERROR',
    });
    expect(b.operators()).toEqual([
      {
        type: 'enrichAsync',
        url: 'https://x.example/lookup/{id}',
        parallelism: 8,
        queueSize: 128,
        timeoutMs: 5000,
        maxRetries: 3,
        retryBackoffMs: 200,
        ordering: 'PRESERVE_INPUT',
        onFailure: 'EMIT_ERROR',
      },
    ]);
  });

  it('cep emits validator shape', () => {
    const seq = [
      { name: 'add', match: "type == 'ADD_TO_CART'", within: '10m' },
      { name: 'view', match: "type == 'VIEW_CART'", follow: 'followedBy' },
    ];
    const b = new StreamBuilder()
      .fromTopic('in')
      .cep(seq, { within: '20m', name: 'cart-flow' });
    expect(b.operators()).toEqual([
      { type: 'cep', sequence: seq, within: '20m', name: 'cart-flow' },
    ]);
  });

  it('cep rejects empty sequence', () => {
    expect(() => new StreamBuilder().fromTopic('in').cep([])).toThrow(/non-empty sequence/);
  });

  // B-109 map_llm
  it('mapLlm full shape', () => {
    const b = new StreamBuilder().fromTopic('in').mapLlm('Summarise: {body}', {
      outputField: 'summary',
      model: 'gemma3:7b',
      temperature: 0.0,
      maxTokens: 64,
      parallelism: 8,
      ordering: 'UNORDERED',
      onFailure: 'PASS_THROUGH',
      maxCallsPerSec: 50,
    });
    expect(b.operators()).toEqual([
      {
        type: 'mapLlm',
        prompt: 'Summarise: {body}',
        outputField: 'summary',
        model: 'gemma3:7b',
        temperature: 0.0,
        maxTokens: 64,
        parallelism: 8,
        ordering: 'UNORDERED',
        onFailure: 'PASS_THROUGH',
        maxCallsPerSec: 50,
      },
    ]);
  });

  it('mapLlm rejects blank prompt / outputField', () => {
    expect(() => new StreamBuilder().fromTopic('in').mapLlm('', { outputField: 'x' })).toThrow(
      /prompt/,
    );
    expect(() =>
      new StreamBuilder().fromTopic('in').mapLlm('p', { outputField: '' }),
    ).toThrow(/outputField/);
  });

  // B-109 extract
  it('extract full shape', () => {
    const b = new StreamBuilder().fromTopic('in').extract({
      instruction: 'Extract intent and urgency',
      schema: { intent: 'string', urgency: 'int' },
      model: 'gemma3:7b',
      temperature: 0.0,
    });
    expect(b.operators()).toEqual([
      {
        type: 'extract',
        instruction: 'Extract intent and urgency',
        schema: { intent: 'string', urgency: 'int' },
        model: 'gemma3:7b',
        temperature: 0.0,
      },
    ]);
  });

  it('extract rejects empty schema', () => {
    expect(() =>
      new StreamBuilder().fromTopic('in').extract({ instruction: 'x', schema: {} }),
    ).toThrow(/schema/);
  });

  // B-109 Phase 2 mcp_call
  it('mcpCall full shape', () => {
    const b = new StreamBuilder().fromTopic('in').mcpCall('crm.lookup_customer', {
      args: { customer_id: '{customerId}' },
      outputField: 'customer',
      parallelism: 4,
      ordering: 'UNORDERED',
      onFailure: 'EMIT_ERROR',
    });
    expect(b.operators()).toEqual([
      {
        type: 'mcpCall',
        tool: 'crm.lookup_customer',
        args: { customer_id: '{customerId}' },
        outputField: 'customer',
        parallelism: 4,
        ordering: 'UNORDERED',
        onFailure: 'EMIT_ERROR',
      },
    ]);
  });

  it('mcpCall minimal fire-and-forget', () => {
    const b = new StreamBuilder().fromTopic('in').mcpCall('pagerduty.create_incident');
    expect(b.operators()).toEqual([{ type: 'mcpCall', tool: 'pagerduty.create_incident' }]);
  });

  it('mcpCall rejects blank tool', () => {
    expect(() => new StreamBuilder().fromTopic('in').mcpCall('')).toThrow(/tool/);
  });

  it('LLM/MCP operators chain with others', () => {
    const b = new StreamBuilder()
      .fromTopic('tickets')
      .mapLlm('Summarise: {body}', { outputField: 'summary' })
      .extract({ instruction: 'Classify', schema: { urgency: 'int' } })
      .filter('urgency >= 4')
      .mcpCall('pagerduty.create_incident', { args: { title: '{summary}' } });
    expect(b.operators().map((o) => o.type)).toEqual([
      'mapLlm',
      'extract',
      'filter',
      'mcpCall',
    ]);
  });

  it('broadcastJoin full shape', () => {
    const b = new StreamBuilder().fromTopic('in').broadcastJoin({
      joinKeyField: 'userId',
      streamingTopic: 'users-table',
      name: 'users-join',
      maxBytes: 10_000_000,
      refreshMode: 'cdc',
      intervalMillis: 30_000,
    });
    expect(b.operators()).toEqual([
      {
        type: 'broadcastJoin',
        joinKeyField: 'userId',
        streamingTopic: 'users-table',
        name: 'users-join',
        maxBytes: 10_000_000,
        refreshMode: 'cdc',
        intervalMillis: 30_000,
      },
    ]);
  });

  it('cdcJoin full shape', () => {
    const b = new StreamBuilder().fromTopic('in').cdcJoin({
      source: 'postgres://orders',
      joinKey: 'orderId',
      table: 'orders',
      stateBackend: 'rocksdb',
    });
    expect(b.operators()).toEqual([
      {
        type: 'cdcJoin',
        source: 'postgres://orders',
        joinKey: 'orderId',
        table: 'orders',
        stateBackend: 'rocksdb',
      },
    ]);
  });

  it('cdcJoin minimal shape', () => {
    const b = new StreamBuilder().fromTopic('in').cdcJoin({ source: 'postgres://orders' });
    expect(b.operators()).toEqual([{ type: 'cdcJoin', source: 'postgres://orders' }]);
  });
});

// ---------------------------------------------------------------------------
// StreamBuilder — full pipeline compilation
// ---------------------------------------------------------------------------

describe('StreamBuilder.build', () => {
  it('minimal pipeline builds', () => {
    const out = new StreamBuilder('p1').fromTopic('in').filter('x > 0').build();
    expect(out.name).toBe('p1');
    const nodes = out.nodes as Record<string, unknown>[];
    expect(nodes[0]!.type).toBe('source');
    expect((nodes[0]!.config as Record<string, unknown>).inputTopic).toBe('in');
    expect((nodes[0]!.config as Record<string, unknown>).engine).toBe('kafka');
    expect(nodes[1]!.type).toBe('agent');
    expect((nodes[1]!.config as Record<string, unknown>).engine).toBe('streaming');
    expect((nodes[1]!.config as Record<string, unknown>).operators).toEqual([
      { type: 'filter', condition: 'x > 0' },
    ]);
    expect(nodes.length).toBe(2);
  });

  it('name can be set via named()', () => {
    const out = new StreamBuilder().named('p2').fromTopic('in').filter('x > 0').build();
    expect(out.name).toBe('p2');
  });

  it('name passed to build overrides constructor', () => {
    const out = new StreamBuilder('ignored').fromTopic('in').filter('x > 0').build('actual');
    expect(out.name).toBe('actual');
  });

  it('description propagates', () => {
    const out = new StreamBuilder('p3', { description: 'my pipeline' })
      .fromTopic('in')
      .filter('x > 0')
      .build();
    expect(out.description).toBe('my pipeline');
  });

  it('describedAs setter', () => {
    const out = new StreamBuilder('p4').describedAs('desc').fromTopic('in').filter('x > 0').build();
    expect(out.description).toBe('desc');
  });

  it('agentLabel setter', () => {
    const out = new StreamBuilder('p5')
      .withAgentLabel('Per-Device Average')
      .fromTopic('in')
      .filter('x > 0')
      .build();
    const nodes = out.nodes as Record<string, unknown>[];
    expect(nodes[1]!.label).toBe('Per-Device Average');
  });

  it('emits sink when toTopic has channel', () => {
    const out = new StreamBuilder('p6')
      .fromTopic('in')
      .filter('x > 0')
      .toTopic('out', { sinkChannel: 'email' })
      .build();
    const nodes = out.nodes as Record<string, unknown>[];
    expect(nodes.length).toBe(3);
    expect(nodes[2]).toEqual({
      type: 'sink',
      label: 'email sink',
      config: { channel: 'email', inputTopic: 'out' },
    });
    expect((nodes[1]!.config as Record<string, unknown>).outputTopic).toBe('out');
  });

  it('skips sink when toTopic has no channel', () => {
    const out = new StreamBuilder('p7').fromTopic('in').filter('x > 0').toTopic('out').build();
    const nodes = out.nodes as Record<string, unknown>[];
    expect(nodes.length).toBe(2);
    expect((nodes[1]!.config as Record<string, unknown>).outputTopic).toBe('out');
  });

  it('toState clears output and sink', () => {
    const out = new StreamBuilder('p8')
      .fromTopic('in')
      .filter('x > 0')
      .toTopic('out', { sinkChannel: 'email' })
      .toState()
      .build();
    const nodes = out.nodes as Record<string, unknown>[];
    expect(nodes.length).toBe(2);
    expect((nodes[1]!.config as Record<string, unknown>).outputTopic).toBeUndefined();
  });

  it('source engine and label propagate', () => {
    const out = new StreamBuilder('p9')
      .fromTopic('in', {
        sourceEngine: 'mqtt',
        sourceConfig: { qos: 1 },
        label: 'Sensor readings',
      })
      .filter('x > 0')
      .build();
    const src = (out.nodes as Record<string, unknown>[])[0]!;
    expect(src.label).toBe('Sensor readings');
    expect(src.config).toEqual({ engine: 'mqtt', inputTopic: 'in', qos: 1 });
  });

  it('sink config merge', () => {
    const out = new StreamBuilder('p10')
      .fromTopic('in')
      .filter('x > 0')
      .toTopic('out', {
        sinkChannel: 'slack',
        sinkConfig: { webhookUrl: 'https://hooks.slack/...' },
        label: 'Heat Alert',
      })
      .build();
    const sink = (out.nodes as Record<string, unknown>[])[2]!;
    expect(sink.label).toBe('Heat Alert');
    expect(sink.config).toEqual({
      channel: 'slack',
      inputTopic: 'out',
      webhookUrl: 'https://hooks.slack/...',
    });
  });

  it('rejects missing name', () => {
    expect(() => new StreamBuilder().fromTopic('in').filter('x > 0').build()).toThrow(/name/);
  });

  it('rejects missing source', () => {
    expect(() => new StreamBuilder('p').filter('x > 0').build()).toThrow(/source/);
  });

  it('rejects empty operator chain', () => {
    expect(() => new StreamBuilder('p').fromTopic('in').build()).toThrow(/operators/);
  });

  it('constructor rejects blank name', () => {
    expect(() => new StreamBuilder('')).toThrow(/name/);
  });

  it('operators returns a copy, not the internal array', () => {
    const b = new StreamBuilder().fromTopic('in').filter('x > 0');
    const snapshot = b.operators();
    snapshot.push({ type: 'tampered' });
    expect(b.operators()).toEqual([{ type: 'filter', condition: 'x > 0' }]);
  });

  it('chain ordering preserved', () => {
    const out = new StreamBuilder('p11')
      .fromTopic('in')
      .filter('a > 0')
      .keyBy('k')
      .window(windows.tumbling('60s'), { aggregations: { cnt: aggs.count() } })
      .filter('cnt > 5')
      .map({ fields: { out: 'cnt' } })
      .build();
    const ops = (
      (out.nodes as Record<string, unknown>[])[1]!.config as Record<string, unknown>
    ).operators as Record<string, unknown>[];
    expect(ops.map((op) => op.type)).toEqual(['filter', 'keyBy', 'window', 'filter', 'map']);
  });

  it('iot template round-trip', () => {
    const out = new StreamBuilder('iot-temperature-aggregator')
      .withAgentLabel('Per-Device Average')
      .fromTopic('sensor-readings', { sourceEngine: 'mqtt', label: 'Sensor readings' })
      .keyBy('deviceId')
      .window(windows.tumbling('60s'), {
        aggregations: { avgTemp: aggs.avg('temperature') },
      })
      .filter('avgTemp > 75')
      .toTopic('sensor-minute-averages', { sinkChannel: 'email', label: 'Heat Alert' })
      .build();

    const nodes = out.nodes as Record<string, unknown>[];
    expect(nodes.map((n) => n.type)).toEqual(['source', 'agent', 'sink']);

    const src = nodes[0]!;
    expect(src.label).toBe('Sensor readings');
    expect(src.config).toEqual({ engine: 'mqtt', inputTopic: 'sensor-readings' });

    const agent = nodes[1]!;
    expect(agent.label).toBe('Per-Device Average');
    const ac = agent.config as Record<string, unknown>;
    expect(ac.engine).toBe('streaming');
    expect(ac.inputTopic).toBe('sensor-readings');
    expect(ac.outputTopic).toBe('sensor-minute-averages');
    expect(ac.operators).toEqual([
      { type: 'keyBy', field: 'deviceId' },
      { type: 'window', spec: 'tumbling(60s)', aggregations: { avgTemp: 'avg(temperature)' } },
      { type: 'filter', condition: 'avgTemp > 75' },
    ]);

    const sink = nodes[2]!;
    expect(sink.label).toBe('Heat Alert');
    expect(sink.config).toEqual({ channel: 'email', inputTopic: 'sensor-minute-averages' });
  });
});

// ---------------------------------------------------------------------------
// StreamsResource — compile + deploy
// ---------------------------------------------------------------------------

describe('client.streams', () => {
  it('accessor exists', () => {
    const client = newClient();
    expect(client.streams).toBeInstanceOf(StreamsResource);
  });

  it('compile returns dict without HTTP call', () => {
    const client = newClient();
    const b = new StreamBuilder('p').fromTopic('in').filter('x > 0');
    // No msw handler set — if compile() reached the network, msw 'error' mode
    // would throw. Reaching this assertion proves no fetch was attempted.
    const out = client.streams.compile(b);
    expect(out.name).toBe('p');
  });

  it('deploy POSTs built definition to /api/pulse/pipelines', async () => {
    let receivedBody: Record<string, unknown> | undefined;
    server.use(
      http.post(`${BASE_URL}/api/pulse/pipelines`, async ({ request }) => {
        receivedBody = (await request.json()) as Record<string, unknown>;
        return HttpResponse.json(
          { id: 'p-42', name: 'fraud-detector', status: 'running' },
          { status: 201 },
        );
      }),
    );

    const client = newClient('fake.jwt.token');
    const b = new StreamBuilder('fraud-detector')
      .fromTopic('payments')
      .filter('amount > 1000')
      .keyBy('customer_id')
      .window(windows.tumbling('60s'), { aggregations: { cnt: aggs.count() } })
      .filter('cnt > 5')
      .toTopic('fraud-alerts');

    const result = await client.streams.deploy(b);
    expect(result.id).toBe('p-42');
    expect(receivedBody?.name).toBe('fraud-detector');
    const nodes = receivedBody!.nodes as Record<string, unknown>[];
    const ops = (nodes[1]!.config as Record<string, unknown>).operators as Record<
      string,
      unknown
    >[];
    expect(ops[2]!.type).toBe('window');
  });

  it('deploy name override propagates to wire body', async () => {
    let receivedBody: Record<string, unknown> | undefined;
    server.use(
      http.post(`${BASE_URL}/api/pulse/pipelines`, async ({ request }) => {
        receivedBody = (await request.json()) as Record<string, unknown>;
        return HttpResponse.json({ id: 'p', name: 'renamed' }, { status: 201 });
      }),
    );
    const client = newClient('fake.jwt.token');
    const b = new StreamBuilder('original').fromTopic('in').filter('x > 0');
    await client.streams.deploy(b, 'renamed');
    expect(receivedBody?.name).toBe('renamed');
  });
});
