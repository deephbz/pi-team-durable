import assert from "node:assert/strict";
import { test, expect, vi } from "vitest";
import {
  GraphTaskController, GraphControlRefusal,
  type GraphApplyInput, type GraphTaskDefinitionInput, type GraphTaskTransitionInput,
  type GraphTaskTransition,
} from "../../src/authority/graph-control.ts";
import { IncrementalGraphTaskController } from "../../src/authority/incremental-graph-control.ts";

const clock = () => new Date("2026-01-01T00:00:00.000Z");
const spyDerivation = (controller: IncrementalGraphTaskController) => vi.spyOn(
  controller as unknown as { deriveNode(node: { definition: { key: string } }): unknown }, "deriveNode",
);
type Command = { kind: "graph"; input: GraphApplyInput } | { kind: "transition"; input: GraphTaskTransitionInput };
const definition = (key: string, needs: string[] = [], assignee = key): GraphTaskDefinitionInput =>
  ({ key, title: key, goal: `Finish ${key}`, assignee, needs });

function rng(seed: number) {
  return (max: number) => {
    seed |= 0;
    seed = seed + 0x6D2B79F5 | 0;
    let t = Math.imul(seed ^ seed >>> 15, 1 | seed);
    t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t;
    return ((t ^ t >>> 14) >>> 0) % max;
  };
}

function execute(controller: GraphTaskController | IncrementalGraphTaskController, command: Command) {
  try {
    return command.kind === "graph" ? controller.applyGraph(command.input) : controller.transition(command.input);
  } catch (error) {
    if (!(error instanceof GraphControlRefusal)) throw error;
    return { refusal: error.code, message: error.message };
  }
}

function pair(compareSnapshot = true) {
  const oracle = new GraphTaskController(undefined, clock);
  const incremental = new IncrementalGraphTaskController(clock);
  const run = (command: Command) => {
    const expected = execute(oracle, command);
    assert.deepStrictEqual(execute(incremental, command), expected, JSON.stringify(command));
    assert.deepStrictEqual(incremental.readTasks(), oracle.readTasks());
    assert.deepStrictEqual(incremental.readAttempts(), oracle.readAttempts());
    assert.deepStrictEqual(incremental.selectReadyFrontier(), oracle.selectReadyFrontier());
    if (compareSnapshot) assert.deepStrictEqual(incremental.snapshot(), oracle.snapshot());
    assert.equal(incremental.currentGraphVersion(), oracle.currentGraphVersion());
    return expected;
  };
  return { oracle, incremental, run };
}

// Reproduce a failing sequence with PTB_SEQUENCE=<seed>. The default always
// covers 2,000 independent sequences, including refusals and accepted replays.
// PTB_SEQUENCE_SHARD=k/n runs every n-th sequence from the k-th; the shards together cover all of them.
const [sequenceShard, sequenceShards] = (process.env.PTB_SEQUENCE_SHARD ?? "1/1").split("/").map(Number);
test("seeded command sequences equal the reference after every step", () => {
  const seeds = process.env.PTB_SEQUENCE ? [Number(process.env.PTB_SEQUENCE)]
    : Array.from({ length: 2000 }, (_, i) => i + 1).filter((seed) => (seed - 1) % sequenceShards === sequenceShard - 1);
  const coverage = new Set<string>();
  for (const seed of seeds) {
    const random = rng(seed);
    const { oracle, incremental, run } = pair(false);
    const commands: Command[] = [];
    const pick = <T>(xs: T[]) => xs[random(xs.length)];
    const makeGraph = (): GraphTaskDefinitionInput[] => {
      const keys = ["a", "B", "c", "d", "e", "f", "g"].slice(0, 1 + random(7));
      return keys.map((key, index) => {
        const needs = keys.slice(0, index).filter(() => random(3) === 0);
        return {
          ...definition(key, needs, `worker-${random(3)}`),
          ...(random(3) === 0 ? { onGoalFailed: { target: pick([key, ...needs]), maxTraversals: 1 + random(3) } } : {}),
        };
      });
    };
    let definitions = makeGraph();
    for (let step = 0; step < 64; step++) {
      let command: Command;
      if (step === 0 || random(10) === 0) {
        if (step > 0) {
          definitions = structuredClone(definitions);
          const selected = pick(definitions);
          switch (random(8)) {
            case 0: selected.title += " revised"; break;
            case 1: selected.goal += " revised"; break;
            case 2: selected.assignee = `worker-${random(3)}`; break;
            case 3: definitions = makeGraph(); break;
            case 4: selected.needs = []; break;
            case 5: selected.onGoalFailed = { target: selected.key, maxTraversals: 1 + random(3) }; break;
            case 6: delete selected.onGoalFailed; break;
            case 7: definitions.reverse(); break;
          }
        }
        const tasks = structuredClone(definitions);
        if (step > 0 && random(5) === 0) {
          switch (random(8)) {
            case 0: tasks[0].needs = [tasks[0].key]; break;
            case 1: tasks[0].needs = ["missing"]; break;
            case 2: tasks.push(tasks[0]); break;
            case 3: tasks[0].goal = " "; break;
            case 4: tasks[0].onGoalFailed = { target: tasks[0].key, maxTraversals: 9 }; break;
            case 5: tasks[0].onGoalFailed = { target: "missing", maxTraversals: 1 }; break;
            case 6: tasks[0].needs = ["missing", "missing"]; break;
            case 7: tasks.length = 0; break;
          }
        }
        command = { kind: "graph", input: {
          operationId: `op-${step}`, tasks,
          ...(step > 0 ? { expectedGraphVersion: random(12) === 0 ? "g_stale" : oracle.currentGraphVersion() } : {}),
        } };
      } else if (commands.length && random(8) === 0) {
        command = structuredClone(pick(commands));
        if (random(3) === 0) {
          if (command.kind === "graph") command.input.tasks[0] && (command.input.tasks[0].goal += " conflict");
          else command.input.currentContext = "conflicting replay";
        }
      } else {
        const tasks = oracle.readTasks();
        const eligible = tasks.filter(task => ["ready", "in_progress", "blocked"].includes(task.state.kind));
        const task = pick(eligible.length && random(4) !== 0 ? eligible : tasks);
        const options: GraphTaskTransition[] = task.state.kind === "ready" ? ["claim", "claim", "claim", "cancel"]
          : task.state.kind === "in_progress" ? ["block", "goal_achieved", "goal_achieved", "goal_failed", "goal_failed", "cancel"]
            : task.state.kind === "blocked" ? ["resume", "resume", "cancel"] : ["claim", "cancel"];
        const transition = random(6) === 0 ? undefined : pick(options);
        command = { kind: "transition", input: {
          operationId: random(80) === 0 ? " " : `op-${step}`,
          taskId: random(35) === 0 ? "missing" : task.id,
          expectedVersion: random(12) === 0 ? "v_0000000000000000" : task.version,
          ...(transition ? { transition } : {}),
          ...(random(12) === 0 ? {} : { worker: random(12) === 0 ? "wrong-worker" : task.assignee }),
          ...(random(8) === 0 ? {} : { evidence: `evidence-${step}` }),
          ...(random(3) === 0 || !transition ? { currentContext: random(10) === 0 ? " " : `context-${step}` } : {}),
        } };
      }
      try {
        const result = run(command);
        coverage.add("refusal" in result ? result.refusal : result.kind === "task_transitioned" ? result.transition : result.kind);
        if (!("refusal" in result) && result.replayed) coverage.add("replayed");
        assert.equal(incremental.hasOperationReceipt(command.input.operationId), oracle.hasOperationReceipt(command.input.operationId));
        const taskId = pick(oracle.readTasks()).id;
        assert.deepStrictEqual(incremental.readAttempts(taskId), oracle.readAttempts(taskId));
      } catch (error) {
        throw new Error(`seed=${seed} step=${step}; replay with PTB_SEQUENCE=${seed}`, { cause: error });
      }
      commands.push(command);
    }
    const snapshot = oracle.snapshot();
    assert.deepStrictEqual(incremental.snapshot(), snapshot, `seed=${seed} history and receipts`);
    for (const event of snapshot.events) coverage.add(event.kind);
  }
  if (!process.env.PTB_SEQUENCE) {
    for (const key of ["graph_applied", "claim", "block", "resume", "goal_achieved", "goal_failed", "cancel", "context_updated", "replayed",
      "invalid_graph", "graph_version_conflict", "task_not_found", "task_version_conflict", "operation_conflict", "invalid_transition",
      "worker_mismatch", "worker_occupied", "evidence_required", "failure_edge_traversed", "failure_edge_exhausted", "attempt_superseded"]) {
      expect(coverage.has(key), key).toBe(true);
    }
  }
}, 900_000);

test("early cutoff skips dependents; a changed diamond join runs once", () => {
  const { oracle, incremental, run } = pair();
  run({ kind: "graph", input: { operationId: "graph", tasks: [definition("a"), definition("b", ["a"]), definition("c", ["a"]), definition("d", ["b", "c"]), definition("unrelated")] } });
  const derive = spyDerivation(incremental);
  const update = (transition?: GraphTaskTransition) => run({ kind: "transition", input: {
    operationId: `update-${oracle.snapshot().events.length}`, taskId: "a", expectedVersion: oracle.readTask("a").version,
    worker: "a", transition, currentContext: "new context", evidence: "done",
  } });
  update();
  expect(derive.mock.calls.map(([node]) => node.definition.key)).toEqual(["a"]);
  derive.mockClear();
  update("claim");
  expect(derive.mock.calls.map(([node]) => node.definition.key)).toEqual(["a"]);
  derive.mockClear();
  update("goal_achieved");
  // b and c change from waiting to ready, but their prerequisite projection
  // remains waiting. d therefore needs no recomputation.
  expect(derive.mock.calls.map(([node]) => node.definition.key)).toEqual(["a", "b", "c"]);
  derive.mockRestore();
});

test("failure repair propagates through a diamond and preserves event order", () => {
  const { oracle, incremental, run } = pair();
  run({ kind: "graph", input: { operationId: "graph", tasks: [
    definition("a"), definition("b", ["a"]), definition("c", ["a"]),
    { ...definition("d", ["b", "c"]), onGoalFailed: { target: "a", maxTraversals: 1 } },
  ] } });
  let step = 0;
  const update = (taskId: string, transition: GraphTaskTransition) => run({ kind: "transition", input: {
    operationId: `update-${step++}`, taskId, expectedVersion: oracle.readTask(taskId).version,
    transition, worker: taskId, evidence: "outcome",
  } });
  for (const id of ["a", "b", "c"]) { update(id, "claim"); update(id, "goal_achieved"); }
  update("d", "claim");
  update("d", "goal_failed");
  expect(incremental.readTask("a").state.kind).toBe("ready");
  for (const id of ["a", "b", "c"]) { update(id, "claim"); update(id, "goal_achieved"); }
  update("d", "claim");
  update("d", "goal_failed");
  expect(incremental.readTask("d").state).toMatchObject({ kind: "goal_failed", reason: "failure_edge_exhausted" });
  const derive = spyDerivation(incremental);
  run({ kind: "graph", input: { operationId: "revise", expectedGraphVersion: oracle.currentGraphVersion(), tasks: [
    { ...definition("a"), goal: "Revised" }, definition("b", ["a"]), definition("c", ["a"]), definition("d", ["b", "c"]),
  ] } });
  expect(derive.mock.calls.map(([node]) => node.definition.key)).toEqual(["a", "b", "c", "d"]);
});

test("lineage changes preserve lifetime ordinals and reset context and cancellation", () => {
  const { oracle, incremental, run } = pair();
  let step = 0;
  const graph = (tasks: GraphTaskDefinitionInput[]) => run({ kind: "graph", input: {
    operationId: `op-${step++}`, expectedGraphVersion: oracle.currentGraphVersion(), tasks,
  } });
  const update = (taskId: string, transition: GraphTaskTransition, currentContext?: string) => run({ kind: "transition", input: {
    operationId: `op-${step++}`, taskId, expectedVersion: oracle.readTask(taskId).version,
    transition, worker: oracle.readTask(taskId).assignee, evidence: "evidence", currentContext,
  } });
  graph([definition("a"), definition("b", ["a"])]);
  update("a", "claim", "first lineage context");
  update("a", "block");
  update("a", "resume");
  update("a", "goal_achieved");
  update("b", "claim");
  graph([{ ...definition("a"), goal: "Revised" }, definition("b", ["a"])]);
  expect(incremental.readTask("a")).toMatchObject({ attemptsStarted: 1, currentContext: "Work has not started." });
  expect(incremental.readTask("b").state.kind).toBe("dependency_waiting");
  expect(incremental.readAttempts().map(attempt => [attempt.id, attempt.state, attempt.current])).toEqual([
    ["a@1", "completed", false], ["b@1", "superseded", false],
  ]);
  update("a", "claim");
  update("a", "block");
  update("a", "cancel", "cancelled lineage context");
  expect(incremental.readAttempts("a")[1]).toMatchObject({ id: "a@2", state: "cancelled", evidence: "evidence" });
  graph([definition("b")]);
  graph([definition("a"), definition("b", ["a"])]);
  expect(incremental.readTask("a")).toMatchObject({ attemptsStarted: 2, state: { kind: "ready" }, currentContext: "Work has not started." });
  update("a", "claim");
  expect(incremental.readTask("a").state).toEqual({ kind: "in_progress", attemptId: "a@3" });
});

test("blocked Tasks release occupancy; all-ready IDs differ from the frontier", () => {
  const { oracle, incremental, run } = pair();
  run({ kind: "graph", input: { operationId: "graph", tasks: ["a", "b", "c"].map(id => definition(id, [], "worker")) } });
  let step = 0;
  const update = (taskId: string, transition: GraphTaskTransition) => run({ kind: "transition", input: {
    operationId: `op-${step++}`, taskId, expectedVersion: oracle.readTask(taskId).version,
    transition, worker: "worker", evidence: "blocked",
  } });
  expect(incremental.selectReadyFrontier().map(task => task.id)).toEqual(["a"]);
  update("a", "claim");
  expect(incremental.selectReadyFrontier()).toEqual([]);
  const blocked = update("a", "block");
  expect(blocked).toMatchObject({ readyTaskIds: ["b", "c"] });
  expect(incremental.selectReadyFrontier().map(task => task.id)).toEqual(["b"]);
  update("b", "claim");
  expect(update("a", "resume")).toMatchObject({ refusal: "worker_occupied" });
  update("b", "block");
  update("a", "resume");
  expect(incremental.selectReadyFrontier()).toEqual([]);
});

test("reordered restatements change only the graph version", () => {
  const { oracle, incremental, run } = pair();
  const tasks = [definition("a"), definition("b", ["a"]), definition("c")];
  run({ kind: "graph", input: { operationId: "graph", tasks } });
  const before = incremental.readTasks();
  const version = incremental.currentGraphVersion();
  const derive = spyDerivation(incremental);
  run({ kind: "graph", input: { operationId: "reorder", expectedGraphVersion: oracle.currentGraphVersion(), tasks: tasks.toReversed() } });
  expect(incremental.currentGraphVersion()).not.toEqual(version);
  expect(incremental.readTasks()).toEqual(before);
  expect(derive).not.toHaveBeenCalled();
});

test("failure traversal counts follow source lineage; demand follows target lineage", () => {
  const { oracle, incremental, run } = pair();
  let step = 0;
  const graph = (goal: string) => run({ kind: "graph", input: {
    operationId: `op-${step++}`, expectedGraphVersion: oracle.currentGraphVersion(), tasks: [
      definition("a"), { ...definition("b", ["a"]), goal, onGoalFailed: { target: "a", maxTraversals: 1 } },
    ],
  } });
  const update = (taskId: string, transition: GraphTaskTransition) => run({ kind: "transition", input: {
    operationId: `op-${step++}`, taskId, expectedVersion: oracle.readTask(taskId).version,
    transition, worker: taskId, evidence: "outcome",
  } });
  graph("First criterion");
  const firstActivation = incremental.readTask("a").activationKey;
  for (const id of ["a", "b"]) {
    update(id, "claim");
    update(id, id === "a" ? "goal_achieved" : "goal_failed");
  }
  const demandedActivation = incremental.readTask("a").activationKey;
  expect(demandedActivation).not.toEqual(firstActivation);
  graph("New criterion");
  expect(incremental.readTask("a").activationKey).toEqual(demandedActivation);
  for (const id of ["a", "b"]) {
    update(id, "claim");
    const result = update(id, id === "a" ? "goal_achieved" : "goal_failed");
    if (id === "b") expect(result).toMatchObject({ failureTraversal: { sourceTaskId: "b", targetTaskId: "a", traversal: 1 } });
  }
  expect(incremental.readTask("a").activationKey).not.toEqual(demandedActivation);
});

test("returned values cannot mutate facts or replay receipts", () => {
  const { oracle, incremental, run } = pair();
  const input = { operationId: "graph", tasks: [definition("a")] };
  const result = incremental.applyGraph(input);
  oracle.applyGraph(input);
  result.tasks[0].goal = "mutated";
  result.readyTaskIds.length = 0;
  incremental.readTask("a").needs.push("missing");
  incremental.snapshot().graphRevisions.length = 0;
  run({ kind: "graph", input });
});

test("rollback does not retain dirty entries for removed, exhausted Tasks", () => {
  const { oracle, incremental, run } = pair();
  let step = 0;
  run({ kind: "graph", input: { operationId: "graph", tasks: [
    { ...definition("a"), onGoalFailed: { target: "a", maxTraversals: 1 } }, definition("b"),
  ] } });
  for (const transition of ["claim", "goal_failed", "claim", "goal_failed"] as const) {
    run({ kind: "transition", input: { operationId: `op-${step++}`, taskId: "a", expectedVersion: oracle.readTask("a").version, transition, worker: "a", evidence: "outcome" } });
  }
  run({ kind: "graph", input: { operationId: "remove", expectedGraphVersion: oracle.currentGraphVersion(), tasks: [definition("b")] } });
  const original = (incremental as any).toView.bind(incremental);
  vi.spyOn(incremental as any, "toView").mockImplementationOnce(() => { throw new Error("injected"); }).mockImplementation(original);
  const command: Command = { kind: "transition", input: {
    operationId: "claim-b", taskId: "b", expectedVersion: oracle.readTask("b").version, transition: "claim", worker: "b",
  } };
  expect(() => execute(incremental, command)).toThrow("injected");
  expect((incremental as any).dirty.size).toBe(0);
  run(command);
});

test("post-mutation failure rolls back indexes, versions, events and sequences", () => {
  const { oracle, incremental, run } = pair();
  run({ kind: "graph", input: { operationId: "graph", tasks: [definition("a")] } });
  const original = (incremental as any).toView.bind(incremental);
  vi.spyOn(incremental as any, "toView").mockImplementationOnce(() => { throw new Error("injected"); }).mockImplementation(original);
  const command: Command = { kind: "transition", input: {
    operationId: "claim", taskId: "a", expectedVersion: oracle.readTask("a").version, transition: "claim", worker: "a",
  } };
  expect(() => execute(incremental, command)).toThrow("injected");
  run(command);
});
