// Pure-engine scaling check. Run from the repository root:
// node bench/incremental-graph.ts
// History growth includes accepted commands and their replay receipts.
import { performance } from "node:perf_hooks";
import { IncrementalGraphTaskController } from "../src/authority/incremental-graph-control.ts";

const engine = new IncrementalGraphTaskController(() => new Date("2026-01-01T00:00:00Z"));
engine.applyGraph({ operationId: "graph", tasks: Array.from({ length: 500 }, (_, i) => ({
  key: `task-${i.toString().padStart(3, "0")}`, title: `Task ${i}`, goal: "Synthetic benchmark work", assignee: `worker-${i % 30}`,
})) });
let sequence = 0;
for (let i = 0; i < 450; i++) {
  const taskId = `task-${i.toString().padStart(3, "0")}`;
  for (const transition of ["claim", "goal_achieved"] as const) {
    const task = engine.readTask(taskId);
    engine.transition({ operationId: `op-${sequence++}`, taskId, expectedVersion: task.version, transition, worker: task.assignee, evidence: "done" });
  }
}
const taskId = "task-450";
let task = engine.readTask(taskId);
task = engine.transition({ operationId: `op-${sequence++}`, taskId, expectedVersion: task.version, transition: "claim", worker: task.assignee }).task;
let contextCommands = 0;
const quantile = (values: number[], q: number) => values[Math.floor(values.length * q)].toFixed(4);
console.log(JSON.stringify({ node: process.version, tasks: 500, terminalTasks: 450, samplesPerRow: 2000, units: "ms", includes: "transition and receipt creation; excludes external readTask" }));
for (const history of [0, 1000, 10000, 50000]) {
  while (contextCommands < history) {
    task = engine.transition({ operationId: `op-${sequence++}`, taskId, expectedVersion: task.version, currentContext: `context-${contextCommands++}` }).task;
  }
  const eventsBefore = sequence;
  const samples: number[] = [];
  for (let i = 0; i < 2200; i++) {
    const input = {
      operationId: `op-${sequence++}`, taskId, expectedVersion: task.version,
      transition: i % 2 === 0 ? "block" as const : "resume" as const,
      worker: task.assignee, evidence: "Synthetic blocker",
    };
    const start = performance.now();
    task = engine.transition(input).task;
    const elapsed = performance.now() - start;
    if (i >= 200) samples.push(elapsed);
  }
  samples.sort((a, b) => a - b);
  console.log(JSON.stringify({ eventsBefore, p50: quantile(samples, 0.5), p95: quantile(samples, 0.95), p99: quantile(samples, 0.99) }));
}
