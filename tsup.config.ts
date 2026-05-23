import { defineConfig } from 'tsup';

// B-098 Phase 2 — dual ESM + CJS so the SDK works in:
//   - Node 20+ ESM apps:      import { PulseClient } from '@olsisoft/pulse-client'
//   - Node CommonJS apps:     const { PulseClient } = require('@olsisoft/pulse-client')
//   - Bundled browser apps:   tree-shakeable via the ESM build
//
// dts: true emits .d.ts so TypeScript consumers get full type completion without
// installing @types/anything.

export default defineConfig({
  entry: ['src/index.ts'],
  format: ['esm', 'cjs'],
  dts: true,
  sourcemap: true,
  clean: true,
  splitting: false,
  target: 'es2022',
  outDir: 'dist',
});
