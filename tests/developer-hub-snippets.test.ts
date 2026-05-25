/**
 * B-119 Track B.3 (decision D3) — real syntax + exec check of the Developer
 * Hub's TypeScript snippets against the installed pulse-js SDK.
 *
 * Mirrors the Python `test_developer_hub_snippets.py`. Two layers:
 *   1. Syntax — every ```ts block transpiles via esbuild (catches type-strip
 *      syntax errors).
 *   2. Execution — every synchronous snippet that builds a `StreamBuilder` is
 *      run with the REAL StreamBuilder / windows / aggs and a proxy-mocked
 *      client. The real builder validates its own arguments (branch shape,
 *      non-blank fields, method existence), so a renamed method or a bad shape
 *      throws here instead of shipping a recipe that breaks when copied.
 *
 * Snippets that consume async client results (`for await`, duplex) are
 * syntax-checked only — the DSL-shape risk doesn't live there.
 */
import { readFileSync, readdirSync, existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { transformSync } from 'esbuild';
import { describe, it, expect } from 'vitest';

import { StreamBuilder, windows, aggs } from '../src/index.js';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const DOCS_DIR = path.resolve(
  HERE,
  '../../streamflow-pulse/src/main/resources/developer-hub',
);

const TS_BLOCK = /```ts\n([\s\S]*?)\n```/g;

interface Snippet {
  file: string;
  index: number;
  code: string;
}

function tsSnippets(): Snippet[] {
  if (!existsSync(DOCS_DIR)) return [];
  const out: Snippet[] = [];
  for (const file of readdirSync(DOCS_DIR).filter((f) => f.endsWith('.md')).sort()) {
    const md = readFileSync(path.join(DOCS_DIR, file), 'utf8');
    let m: RegExpExecArray | null;
    let i = 0;
    while ((m = TS_BLOCK.exec(md)) !== null) {
      out.push({ file, index: ++i, code: m[1] });
    }
  }
  return out;
}

/** A proxy that answers any property access / call with itself — stands in for
 *  `client` and any other free name a snippet references. */
function infiniteMock(): unknown {
  const handler: ProxyHandler<() => void> = {
    // Return undefined for `then` (and any symbol probe) so `await mock`
    // doesn't treat the proxy as a never-resolving thenable; everything else
    // resolves to the proxy so chains like client.streams.deploy(...) work.
    get: (_t, prop) => (prop === 'then' || typeof prop === 'symbol' ? undefined : proxy),
    apply: () => proxy,
  };
  const proxy: unknown = new Proxy(function () {}, handler);
  return proxy;
}

function stripImports(code: string): string {
  return code
    .split('\n')
    .filter((l) => !/^\s*import\s/.test(l))
    .join('\n');
}

async function execDsl(code: string): Promise<void> {
  const body = stripImports(code);
  const wrapped = `return (async () => {\n${body}\n})();`;
  const js = transformSync(wrapped, { loader: 'ts' }).code;

  const names = ['StreamBuilder', 'windows', 'aggs'];
  const values: unknown[] = [StreamBuilder, windows, aggs];
  for (let attempt = 0; attempt < 64; attempt++) {
    try {
      // eslint-disable-next-line @typescript-eslint/no-implied-eval
      const fn = new Function(...names, js);
      await fn(...values);
      return;
    } catch (e) {
      if (e instanceof ReferenceError) {
        const match = /(\w+) is not defined/.exec(e.message);
        if (match && !names.includes(match[1])) {
          names.push(match[1]);
          values.push(infiniteMock());
          continue;
        }
      }
      throw e; // real error (bad DSL shape / renamed method) → fail the test
    }
  }
  throw new Error('snippet needed more than 64 placeholder names');
}

const SNIPPETS = tsSnippets();

describe('developer-hub TypeScript snippets', () => {
  it('the docs ship at least one ts snippet', () => {
    expect(SNIPPETS.length).toBeGreaterThan(0);
  });

  for (const s of SNIPPETS) {
    it(`${s.file} #${s.index} transpiles`, () => {
      expect(() => transformSync(s.code, { loader: 'ts' })).not.toThrow();
    });
  }

  const dsl = SNIPPETS.filter(
    (s) => s.code.includes('new StreamBuilder(') && !s.code.includes('for await') && !s.code.includes('.duplex('),
  );
  for (const s of dsl) {
    it(`${s.file} #${s.index} executes against the real SDK`, async () => {
      await expect(execDsl(s.code)).resolves.toBeUndefined();
    });
  }
});
