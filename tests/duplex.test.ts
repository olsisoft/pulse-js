/**
 * B-114 — duplex round-trip tests. Deliberately in their own file with NO MSW:
 * MSW's `setupServer` patches the global WebSocket and errors any unhandled
 * connection, which would hijack these sockets. Here the client talks to a REAL
 * in-process `ws` server — version-independent (the client uses the global
 * WebSocket on Node 22+ and the optional `ws` package on Node < 22; both connect
 * to this server). Mirrors pulse-py's test_duplex.py stub server.
 */

import { afterEach, describe, expect, it } from 'vitest';
import { WebSocketServer, type WebSocket as WsServerSocket } from 'ws';

import { PulseAPIError, PulseClient } from '../src/index.js';

let wss: WebSocketServer | undefined;

async function startServer(handler: (socket: WsServerSocket) => void): Promise<string> {
  wss = new WebSocketServer({ host: '127.0.0.1', port: 0 });
  await new Promise<void>((resolve) => wss!.once('listening', resolve));
  const addr = wss.address();
  const port = typeof addr === 'object' && addr !== null ? addr.port : 0;
  wss.on('connection', handler);
  return `ws://127.0.0.1:${port}/api/pulse/agents/fraud/duplex`;
}

function decisionHandler(socket: WsServerSocket): void {
  socket.send(JSON.stringify({ type: 'connected', mode: 'duplex', agentId: 'fraud' }));
  socket.on('message', (data: Buffer) => {
    const msg = JSON.parse(data.toString()) as Record<string, unknown>;
    if (msg.type === 'send') {
      const cid = msg.correlationId as string;
      const payload = msg.payload as { amount?: number };
      socket.send(JSON.stringify({ type: 'ack', correlationId: cid, eventId: `e-${cid}` }));
      const decision = (payload.amount ?? 0) > 1000 ? 'DENY' : 'APPROVE';
      socket.send(
        JSON.stringify({
          type: 'output',
          correlationId: cid,
          event: { id: `e-${cid}`, type: 'signal', payload: { decision } },
        }),
      );
    }
  });
}

function errorHandler(socket: WsServerSocket): void {
  socket.send(JSON.stringify({ type: 'error', error: 'unknown agent or duplex disabled' }));
}

function newClient(token?: string): PulseClient {
  return new PulseClient({ baseUrl: 'http://pulse.test:9090', token });
}

describe('client.duplex round-trip', () => {
  afterEach(() => {
    wss?.close();
    wss = undefined;
  });

  it('correlates a send with its output', async () => {
    const wsUrl = await startServer(decisionHandler);
    const ch = await newClient('jwt').duplex('fraud', { wsUrl });
    const cid = await ch.send({ amount: 5000 }, 'tx-1');
    expect(cid).toBe('tx-1');
    const out = await ch.recv();
    expect(out.correlationId).toBe('tx-1');
    expect((out.payload as { decision: string }).decision).toBe('DENY');
    await ch.close();
  });

  it('generates a correlation id when omitted and pipelines sends', async () => {
    const wsUrl = await startServer(decisionHandler);
    const ch = await newClient('jwt').duplex('fraud', { wsUrl });
    const c1 = await ch.send({ amount: 50 }); // APPROVE
    const c2 = await ch.send({ amount: 9000 }); // DENY
    expect(c1).not.toBe(c2);
    const first = await ch.recv();
    const second = await ch.recv();
    const byId: Record<string, { decision: string }> = {
      [first.correlationId as string]: first.payload as { decision: string },
      [second.correlationId as string]: second.payload as { decision: string },
    };
    expect(byId[c1]!.decision).toBe('APPROVE');
    expect(byId[c2]!.decision).toBe('DENY');
    await ch.close();
  });

  it('an error frame on open raises PulseAPIError', async () => {
    const wsUrl = await startServer(errorHandler);
    await expect(newClient('jwt').duplex('ghost', { wsUrl })).rejects.toBeInstanceOf(PulseAPIError);
  });

  it('honours a wsUrl override', async () => {
    const wsUrl = await startServer(decisionHandler);
    const ch = await newClient().duplex('fraud', { wsUrl });
    await ch.send({ amount: 5000 }, 'tx-override');
    const out = await ch.recv();
    expect(out.correlationId).toBe('tx-override');
    await ch.close();
  });
});
