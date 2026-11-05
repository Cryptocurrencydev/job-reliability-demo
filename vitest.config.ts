import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    fileParallelism: false, // the suites share one database and SIGKILL real processes
    testTimeout: 60_000,
    hookTimeout: 60_000,
    reporters: ['default', ['json', { outputFile: 'results/vitest.json' }]],
  },
});
