/**
 * PVSC + evals over the wire.
 *
 * The five Pulse SDKs had no PVSC surface at all — `grep -r pvsc` across
 * pulse-js / pulse-py / pulse-rs / pulse-go / pulse-java returned nothing.
 * Anything an operator could do to a topic contract, an arbitration policy or
 * an eval suite was reachable only from the browser.
 *
 * Offline throughout: MSW intercepts fetch and returns canned responses. The
 * point is to pin the wire format, not to exercise a server.
 */

import { http, HttpResponse } from 'msw';
import { setupServer } from 'msw/node';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { PulseClient } from '../src/index.js';

const BASE_URL = 'http://pulse.test:9090';
const server = setupServer();
beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

const client = () => new PulseClient({ baseUrl: BASE_URL, token: 'jwt' });

describe('client.pvsc — topic contracts', () => {
  it('lists registered schemas, unwrapping the envelope', async () => {
    server.use(http.get(`${BASE_URL}/api/pulse/pvsc/schemas`, () =>
      HttpResponse.json({ schemas: [{ topic: 'quotes' }], count: 1 })));

    await expect(client().pvsc.schemas()).resolves.toEqual([{ topic: 'quotes' }]);
  });

  it('sends the grounding policy, which is the point of the whole surface', async () => {
    // A grounding policy could be written from Java and from nowhere else
    // until recently. If the SDK dropped it, the SDK would be the next place
    // it was unreachable from.
    let received: unknown;
    server.use(http.put(`${BASE_URL}/api/pulse/pvsc/schemas`, async ({ request }) => {
      received = await request.json();
      return HttpResponse.json({ status: 'saved', topic: 'quotes' });
    }));

    await client().pvsc.saveSchema({
      topic: 'quotes',
      requiredFields: {},
      optionalFields: { price: { type: 'number', grounding: 'required' } },
      allowExtraFields: true,
    });

    expect(received).toMatchObject({
      topic: 'quotes',
      optionalFields: { price: { type: 'number', grounding: 'required' } },
    });
  });

  it('deletes a schema by topic in the body, as the route expects', async () => {
    let received: unknown;
    server.use(http.delete(`${BASE_URL}/api/pulse/pvsc/schemas`, async ({ request }) => {
      received = await request.json();
      return HttpResponse.json({ status: 'deleted' });
    }));

    await client().pvsc.deleteSchema('quotes');
    expect(received).toEqual({ topic: 'quotes' });
  });
});

describe('client.pvsc — arbitration', () => {
  it('reads the stances off the config', async () => {
    server.use(http.get(`${BASE_URL}/api/pulse/pvsc/config`, () =>
      HttpResponse.json({
        arbitration: { stances: [{ domain: 'legal', precedence: 1, veto: true }],
          stanceCount: 1, enabled: true },
      })));

    const cfg = await client().pvsc.config();
    expect((cfg.arbitration as Record<string, unknown>).enabled).toBe(true);
  });

  it('setStances writes through the config route', async () => {
    let received: unknown;
    server.use(http.put(`${BASE_URL}/api/pulse/pvsc/config`, async ({ request }) => {
      received = await request.json();
      return HttpResponse.json({ status: 'updated' });
    }));

    await client().pvsc.setStances([{ domain: 'legal', precedence: 1, veto: true }]);
    expect(received).toEqual({
      arbitrationStances: [{ domain: 'legal', precedence: 1, veto: true }],
    });
  });

  it('an empty list is a real value — it disables arbitration', async () => {
    let received: unknown;
    server.use(http.put(`${BASE_URL}/api/pulse/pvsc/config`, async ({ request }) => {
      received = await request.json();
      return HttpResponse.json({ status: 'updated' });
    }));

    await client().pvsc.setStances([]);
    expect(received).toEqual({ arbitrationStances: [] });
  });
});

describe('client.pvsc — metrics and DLQ', () => {
  it('surfaces the quorum information yield', async () => {
    server.use(http.get(`${BASE_URL}/api/pulse/pvsc/metrics`, () =>
      HttpResponse.json({
        pvscModeCount: 300,
        quorumInformationYield: 0.3333,
        quorumRedundantGuardianCalls: 400,
        quorumInterpretation: 'A guardian dissented on 100 of 300 rounds;',
      })));

    const m = await client().pvsc.metrics();
    expect(m.quorumInformationYield).toBeCloseTo(0.3333, 4);
    expect(m.quorumRedundantGuardianCalls).toBe(400);
  });

  it('lists blocked events and can replay one', async () => {
    let reinjected: unknown;
    server.use(
      http.get(`${BASE_URL}/api/pulse/pvsc/dlq`, () =>
        HttpResponse.json({ entries: [{ eventId: 'e1', rejectedBy: 'pvsc-agent-gate' }],
          counts: {}, total: 1 })),
      http.post(`${BASE_URL}/api/pulse/pvsc/dlq/reinject`, async ({ request }) => {
        reinjected = await request.json();
        return HttpResponse.json({ reinjected: true });
      }),
    );

    const entries = await client().pvsc.dlq();
    expect(entries[0].rejectedBy).toBe('pvsc-agent-gate');

    await client().pvsc.reinject('e1');
    expect(reinjected).toEqual({ eventId: 'e1' });
  });
});

describe('client.evals', () => {
  it('lists suites and their cases', async () => {
    server.use(
      http.get(`${BASE_URL}/api/pulse/evals`, () =>
        HttpResponse.json({ suites: ['pricing'], count: 1 })),
      http.get(`${BASE_URL}/api/pulse/evals/cases`, ({ request }) => {
        expect(new URL(request.url).searchParams.get('suite')).toBe('pricing');
        return HttpResponse.json({ cases: [{ caseId: 'c1', gradable: true }], count: 1 });
      }),
    );

    await expect(client().evals.suites()).resolves.toEqual(['pricing']);
    const cases = await client().evals.cases('pricing');
    expect(cases[0].caseId).toBe('c1');
  });

  it('a REGRESSION is a verdict to branch on, not a thrown error', async () => {
    // The server answers 200 with blocksRelease=true because the run
    // succeeded. A caller in CI branches on blocksRelease; if this call threw,
    // "the suite regressed" and "the call broke" would be the same event.
    server.use(http.post(`${BASE_URL}/api/pulse/evals/run`, () =>
      HttpResponse.json({
        suiteId: 'pricing', total: 10, passing: 7, failing: 3, baseline: 9,
        gate: 'REGRESSION', blocksRelease: true,
        summary: "suite 'pricing': 7/10 passing — REGRESSION", cases: [],
      })));

    const report = await client().evals.run('pricing');
    expect(report.gate).toBe('REGRESSION');
    expect(report.blocksRelease).toBe(true);
  });

  it('records a floor and saves a case', async () => {
    let baseline: unknown;
    let saved: unknown;
    server.use(
      http.post(`${BASE_URL}/api/pulse/evals/baseline`, async ({ request }) => {
        baseline = await request.json();
        return HttpResponse.json({ suiteId: 'pricing', baseline: 7 });
      }),
      http.post(`${BASE_URL}/api/pulse/evals/cases`, async ({ request }) => {
        saved = await request.json();
        return HttpResponse.json({ caseId: 'c9', gradable: true });
      }),
    );

    await client().evals.recordBaseline('pricing');
    expect(baseline).toEqual({ suiteId: 'pricing' });

    await client().evals.saveCase({
      suiteId: 'pricing', name: 'grounded price', agentKey: 'quoter',
      inputPayload: '{"montant":42.00}', expectations: { 'data.price': 42.0 },
    });
    expect(saved).toMatchObject({ suiteId: 'pricing', agentKey: 'quoter' });
  });
});
