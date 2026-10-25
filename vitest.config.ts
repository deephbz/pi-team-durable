import { defineConfig } from "vitest/config";
export default defineConfig({ test: { include: ["test/authority/*.test.ts"], maxWorkers: 1, fileParallelism: false, testTimeout: 2000 } });
