/**
 * Incremental, in-memory implementation of the graph-control contract.
 * The reference controller remains the differential oracle. This module owns
 * no storage, delivery, or migration. Histories serve explicit reads only.
 */
import { createHash } from "node:crypto";
import { taskVersionRef } from "./task-version-ref.ts";
import {
  GraphControlRefusal, GRAPH_CONTROL_MAX_FAILURE_TRAVERSALS,
  type GraphVersionRef, type GraphTaskOutcome,
  type GraphTaskDefinitionInput, type GraphApplyInput, type GraphTaskTransitionInput,
  type GraphTaskState, type GraphTaskView, type GraphAttemptView,
  type GraphRevision, type GraphControlEvent, type GraphApplyResult,
  type GraphTransitionResult, type GraphControlResult, type GraphControlSnapshot,
} from "./graph-control.ts";

type StoredTaskDefinition = GraphRevision["tasks"][number];
type StoredReceipt = GraphControlSnapshot["receipts"][number];
type EventOf<K extends GraphControlEvent["kind"]> = Extract<GraphControlEvent, { kind: K }>;
type AttemptStartedEvent = EventOf<"attempt_started">;
type AttemptBlockedEvent = EventOf<"attempt_blocked">;
type AttemptResumedEvent = EventOf<"attempt_resumed">;
type AttemptCompletedEvent = EventOf<"attempt_completed">;
type AttemptSupersededEvent = EventOf<"attempt_superseded">;
type TaskCancelledEvent = EventOf<"task_cancelled">;
type FailureEdgeExhaustedEvent = EventOf<"failure_edge_exhausted">;
type GraphControlEventInput = GraphControlEvent extends infer Event
  ? Event extends GraphControlEvent ? Omit<Event, "sequence"> : never : never;
interface AttemptFold {
  started: AttemptStartedEvent;
  blocked?: AttemptBlockedEvent;
  resumed?: AttemptResumedEvent;
  completed?: AttemptCompletedEvent;
  superseded?: AttemptSupersededEvent;
  cancellation?: TaskCancelledEvent;
  exhausted?: FailureEdgeExhaustedEvent;
  endedAt?: string;
  endedSequence?: number;
}

interface DerivedTask {
  definition: StoredTaskDefinition;
  state: GraphTaskState;
  activationKey?: string;
  inputAttemptIds: Record<string, string>;
  acceptedAttemptId?: string;
  attemptsStarted: number;
}

function canonical(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonical);
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(Object.entries(value as Record<string, unknown>)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, nested]) => [key, canonical(nested)]));
}

function digest(value: unknown): string {
  return createHash("sha256").update(JSON.stringify(canonical(value))).digest("hex");
}

function clone<T>(value: T): T {
  return structuredClone(value);
}

function requireText(value: string | undefined, label: string): string {
  if (!value?.trim()) throw new GraphControlRefusal("evidence_required", `${label} must not be empty.`);
  return value;
}

function graphVersion(value: unknown): GraphVersionRef {
  return `g_${digest(value).slice(0, 16)}`;
}

function semanticDefinition(definition: Omit<StoredTaskDefinition, "lineage">): unknown {
  return {
    key: definition.key,
    title: definition.title,
    goal: definition.goal,
    assignee: definition.assignee,
    needs: definition.needs,
    onGoalFailed: definition.onGoalFailed ?? null,
  };
}

interface TaskNode {
  definition: StoredTaskDefinition;
  cancellation?: TaskCancelledEvent;
  currentContext?: string;
  demand: number;
  traversals: number;
  // At most one eligible Attempt exists for each activation.
  activations: Map<string, AttemptFold>;
  derived?: DerivedTask;
  view?: GraphTaskView;
}

/** Current-state representation. History and replay receipts have separate owners. */
export interface GraphMaterializedHead {
  sequence: number;
  revision?: Omit<GraphRevision, "tasks">;
  topology: Array<{ key: string; needs: string[] }>;
  projections: Record<string, string>;
  ready: Record<string, string[]>;
  occupied: Record<string, string[]>;
}
export interface GraphMaterializedTask {
  id: string;
  attemptsStarted: number;
  node?: Omit<TaskNode, "activations">;
  attempts: AttemptFold[];
}
export class GraphTaskRequired extends Error {
  readonly ids: string[];
  constructor(ids: string[]) { super("Task materialization required"); this.ids = ids; }
}

// A missing record suspends a detached command. The storage adapter loads the
// requested records and retries from the same committed materialization.
class TaskMap extends Map<string, TaskNode> {
  private readonly keysInGraph: Set<string>;
  private readonly load: (id: string) => TaskNode;
  constructor(ids: string[], load: (id: string) => TaskNode) {
    super(); this.keysInGraph = new Set(ids); this.load = load;
  }
  override get(id: string): TaskNode | undefined {
    if (!super.has(id) && this.keysInGraph.has(id)) super.set(id, this.load(id));
    return super.get(id);
  }
  override keys() { return this.keysInGraph.keys(); }
  override has(id: string) { return this.keysInGraph.has(id); }
  override values() { for (const id of this.keysInGraph) this.get(id); return super.values(); }
  override entries() { for (const id of this.keysInGraph) this.get(id); return super.entries(); }
  override [Symbol.iterator]() { return this.entries(); }
}

class ProjectionMap extends Map<string, string> {
  private readonly source: Record<string,string>;
  constructor(source:Record<string,string>) { super(); this.source=source; }
  override get(id:string) { const value=super.has(id)?super.get(id):this.source[id]; if(value===undefined)throw new GraphTaskRequired([id]); return value; }
  override entries() { return new Map([...Object.entries(this.source),...super.entries()]).entries(); }
  override [Symbol.iterator]() { return this.entries(); }
}
const structures = new Map<string, {topology:GraphMaterializedHead['topology'];ranks:Map<string,number>;dependents:Map<string,string[]>}>();

export class IncrementalGraphTaskController {
  private readonly graphRevisions: GraphRevision[] = [];
  private readonly events: GraphControlEvent[] = [];
  private readonly receipts = new Map<string, StoredReceipt>();
  private readonly attempts = new Map<string, AttemptFold>();
  private readonly attemptCounts = new Map<string, number>();
  private readonly activeAttempts = new Map<string, Map<string, AttemptFold>>();
  private nodes = new Map<string, TaskNode>();
  private dependents = new Map<string, string[]>();
  private ranks = new Map<string, number>();
  private readonly dirty = new Set<string>();
  private readonly ready = new Map<string, Set<string>>();
  private readonly occupied = new Map<string, Set<string>>();
  private sequence = 0;
  private projections = new Map<string, string>();
  private readonly changed = new Set<string>();
  private restoredRevisionCount = 0;
  private structuralTasks: Array<{ key: string; needs: string[] }> = [];
  private commandRecordedAt?: string;

  private readonly clock: () => Date;

  constructor(clock: () => Date = () => new Date()) { this.clock = clock; }

  /** Migration only. The caller validates the source with the reference controller. */
  static fromSnapshot(snapshot: GraphControlSnapshot, clock?: () => Date): IncrementalGraphTaskController {
    const engine = new IncrementalGraphTaskController(clock);
    const entries = [...snapshot.graphRevisions, ...snapshot.events].sort((a, b) => a.sequence - b.sequence);
    for (const entry of entries) {
      engine.sequence = entry.sequence;
      if ("tasks" in entry) {
        const revision = clone(entry);
        engine.graphRevisions.push(revision);
        engine.installGraph(revision.tasks);
      } else {
        const event = clone(entry);
        engine.events.push(event);
        engine.fold(event);
      }
      engine.flush();
    }
    for (const receipt of snapshot.receipts) engine.restoreReceipt(receipt);
    return engine;
  }

  static restore(head: GraphMaterializedHead, records: ReadonlyMap<string, GraphMaterializedTask>, clock?: () => Date): IncrementalGraphTaskController {
    const engine = new IncrementalGraphTaskController(clock);
    engine.sequence = head.sequence;
    const key=head.revision?.version??"empty";
    let structure=structures.get(key);
    if(!structure){
      const topology=JSON.parse(JSON.stringify(head.topology)) as GraphMaterializedHead['topology'];
      const dependents=new Map<string,string[]>(topology.map(t=>[t.key,[]]));
      for(const task of topology)for(const need of task.needs)dependents.get(need)!.push(task.key);
      structure={topology,ranks:new Map(topologicalOrder(topology).map((id,rank)=>[id,rank])),dependents};
      if(structures.size>=32)structures.delete(structures.keys().next().value!);
      structures.set(key,structure);
    }
    engine.structuralTasks = structure.topology;
    engine.projections = new ProjectionMap(head.projections);
    engine.ranks = structure.ranks;
    engine.dependents = structure.dependents;
    for (const [worker, ids] of Object.entries(head.ready)) engine.ready.set(worker, new Set(ids));
    for (const [worker, ids] of Object.entries(head.occupied)) engine.occupied.set(worker, new Set(ids));
    const restoreRecord = (record: GraphMaterializedTask): TaskNode | undefined => {
      engine.attemptCounts.set(record.id, record.attemptsStarted);
      const node = record.node ? { ...clone(record.node), activations: new Map<string, AttemptFold>() } : undefined;
      for (const original of record.attempts) {
        const attempt = clone(original), started = attempt.started;
        engine.attempts.set(started.attemptId, attempt);
        if (node?.definition.lineage !== started.taskLineage) continue;
        if (!attempt.superseded && !attempt.cancellation) {
          node.activations.set(started.activationKey, attempt);
          if (!attempt.completed) {
            let active = engine.activeAttempts.get(record.id);
            if (!active) engine.activeAttempts.set(record.id, active = new Map());
            active.set(started.attemptId, attempt);
          }
        }
      }
      return node;
    };
    const ids = engine.structuralTasks.map(t => t.key);
    engine.nodes = new TaskMap(ids, id => {
      const record = records.get(id);
      if (!record?.node) throw new GraphTaskRequired([...new Set([id, ...engine.dirty])]);
      return restoreRecord(record)!;
    });
    // Removed keys retain their lifetime ordinal without entering the DAG.
    for (const [id, record] of records) if (!engine.nodes.has(id)) restoreRecord(record);
    if (head.revision) {
      const revision = { ...head.revision, get tasks() { return ids.map(id => engine.definition(id)); } };
      engine.graphRevisions.push(revision);
      engine.restoredRevisionCount = 1;
    }
    return engine;
  }

  headUpdate() {
    this.flush();
    return { sequence:this.sequence,
      projections:Object.fromEntries([...this.changed].filter(id=>this.nodes.has(id)).map(id=>[id,this.projections.get(id)!])),
      ready:Object.fromEntries([...this.ready].map(([worker,ids])=>[worker,[...ids]])),
      occupied:Object.fromEntries([...this.occupied].map(([worker,ids])=>[worker,[...ids]])),
    };
  }

  materializedHead(): GraphMaterializedHead {
    this.flush();
    const revision = this.currentRevision();
    return {
      sequence: this.sequence,
      ...(revision ? { revision: { sequence: revision.sequence, version: revision.version, ...(revision.recorded_at ? { recorded_at: revision.recorded_at } : {}) } } : {}),
      topology: clone(this.structuralTasks),
      projections: Object.fromEntries(this.projections),
      ready: Object.fromEntries([...this.ready].map(([worker, ids]) => [worker, [...ids]])),
      occupied: Object.fromEntries([...this.occupied].map(([worker, ids]) => [worker, [...ids]])),
    };
  }

  materializedTask(id: string): GraphMaterializedTask {
    this.flush();
    const node = this.nodes.get(id);
    const { activations: _activations, ...data } = node ?? { activations: undefined };
    return clone({ id, attemptsStarted: this.attemptCounts.get(id) ?? 0,
      ...(node ? { node: data as Omit<TaskNode, "activations"> } : {}),
      attempts: [...this.attempts.values()].filter(a => a.started.taskId === id),
    });
  }

  changes() {
    this.flush();
    return { taskIds: [...this.changed], revisions: clone(this.graphRevisions.slice(this.restoredRevisionCount)),
      events: clone(this.events), receipts: [...this.receipts.values()].map(clone) };
  }

  restoreReceipt(receipt: StoredReceipt): void { this.receipts.set(receipt.operationId, clone(receipt)); }

  snapshot(): GraphControlSnapshot {
    return {
      schema: "pi-team-bright-graph-control/1",
      graphRevisions: clone(this.graphRevisions),
      events: clone(this.events),
      receipts: [...this.receipts.values()].map(clone),
    };
  }

  currentGraphVersion(): GraphVersionRef | undefined {
    return this.currentRevision()?.version;
  }

  hasOperationReceipt(operationId: string): boolean {
    return this.receipts.has(operationId);
  }

  applyGraph(input: GraphApplyInput): GraphApplyResult & { replayed: boolean } {
    return this.command(input.operationId, { command: "apply_graph", ...input }, () => {
      const priorRevision = this.currentRevision();
      if (!priorRevision && input.expectedGraphVersion !== undefined) {
        throw new GraphControlRefusal("graph_version_conflict", "The first graph apply must not include an expected version.");
      }
      if (priorRevision && input.expectedGraphVersion !== priorRevision.version) {
        throw new GraphControlRefusal(
          "graph_version_conflict",
          `Graph changed; expected ${String(input.expectedGraphVersion)}, current ${priorRevision.version}.`,
        );
      }

      const normalized = normalizeAndValidate(input.tasks);
      const priorById = new Map((priorRevision?.tasks ?? []).map(task => [task.key, task]));
      const revisionSequence = this.nextSequence();
      const tasks: StoredTaskDefinition[] = normalized.map(definition => {
        const prior = priorById.get(definition.key);
        const sameMeaning = prior && digest(semanticDefinition(prior)) === digest(semanticDefinition(definition));
        return {
          ...definition,
          lineage: sameMeaning
            ? prior.lineage
            : digest({ task: semanticDefinition(definition), introducedAt: revisionSequence }),
        };
      });
      const version = graphVersion({ previous: priorRevision?.version ?? null, tasks });

      this.flush();
      const attemptsBefore = this.attemptsToSupersede();
      const nextById = new Map(tasks.map(task => [task.key, task]));
      const directlyChanged = new Set<string>();
      for (const [taskId, prior] of priorById) {
        if (nextById.get(taskId)?.lineage !== prior.lineage) directlyChanged.add(taskId);
      }
      for (const [taskId, next] of nextById) {
        if (priorById.get(taskId)?.lineage !== next.lineage) directlyChanged.add(taskId);
      }
      const affected = new Set([
        ...reverseClosure([...directlyChanged], priorRevision?.tasks ?? []),
        ...reverseClosure([...directlyChanged], tasks),
      ]);

      this.graphRevisions.push({
        sequence: revisionSequence,
        version,
        tasks,
        ...(this.commandRecordedAt ? { recorded_at: this.commandRecordedAt } : {}),
      });
      this.installGraph(tasks);
      for (const taskId of [...affected].sort()) {
        const attemptId = attemptsBefore.get(taskId);
        if (attemptId) this.append({
          kind: "attempt_superseded",
          operationId: input.operationId,
          attemptId,
          reason: "graph_revised",
        });
      }

      const currentTasks = this.readTasks();
      return {
        kind: "graph_applied",
        operationId: input.operationId,
        graphVersion: version,
        tasks: currentTasks,
        readyTaskIds: readyIds(currentTasks),
      };
    });
  }

  transition(input: GraphTaskTransitionInput): GraphTransitionResult & { replayed: boolean } {
    return this.command(input.operationId, { command: "transition", ...input }, () => {
      const task = this.readTask(input.taskId);
      if (task.version !== input.expectedVersion) {
        throw new GraphControlRefusal(
          "task_version_conflict",
          `Task ${task.id} changed; expected ${input.expectedVersion}, current ${task.version}.`,
        );
      }
      if (!input.transition && !input.currentContext?.trim()) {
        throw new GraphControlRefusal("invalid_transition", "A Task command requires a transition or nonempty current context.");
      }

      let failureTraversal: GraphTransitionResult["failureTraversal"];
      switch (input.transition) {
        case "claim":
          this.claim(task, input);
          break;
        case "block":
          this.block(task, input);
          break;
        case "resume":
          this.resume(task, input);
          break;
        case "goal_achieved":
        case "goal_failed":
          failureTraversal = this.complete(task, input);
          break;
        case "cancel":
          this.cancel(task, input);
          break;
      }
      if (input.currentContext?.trim()) {
        this.append({
          kind: "task_context_updated",
          operationId: input.operationId,
          taskId: task.id,
          taskLineage: this.definition(task.id).lineage,
          currentContext: input.currentContext,
        });
      }

      this.flush();
      return {
        kind: "task_transitioned",
        operationId: input.operationId,
        transition: input.transition ?? "context_updated",
        task: this.readTask(input.taskId),
        readyTaskIds: this.readyIds(),
        ...(failureTraversal ? { failureTraversal } : {}),
      };
    });
  }

  readTask(taskId: string): GraphTaskView {
    this.flush();
    const node = this.nodes.get(taskId);
    if (!node) throw new GraphControlRefusal("task_not_found", `Task ${taskId} is not in the current graph.`);
    return clone(node.view!);
  }

  readTasks(): GraphTaskView[] {
    this.flush();
    return [...this.nodes.values()].map(node => clone(node.view!))
      .sort((left, right) => left.id.localeCompare(right.id));
  }

  attemptStartedSequence(id: string): number {
    return this.attempts.get(id)!.started.sequence;
  }

  readAttempts(taskId?: string): GraphAttemptView[] {
    const currentTaskIds = new Set(this.nodes.keys());
    const tasks = taskId ? (this.nodes.has(taskId) ? [this.readTask(taskId)] : []) : this.readTasks();
    const currentAttemptIds = new Set(tasks.flatMap(task => {
      if (task.state.kind === "in_progress" || task.state.kind === "blocked" || task.state.kind === "goal_achieved") {
        return [task.state.attemptId];
      }
      if (task.state.kind === "goal_failed" && "attemptId" in task.state) return [task.state.attemptId];
      return [];
    }));
    return [...this.attempts.values()]
      .filter(attempt => !taskId || attempt.started.taskId === taskId)
      .sort((left, right) => left.started.sequence - right.started.sequence)
      .map(attempt => {
        const blocked = attempt.blocked && (!attempt.resumed || attempt.blocked.sequence > attempt.resumed.sequence);
        const staleLineage = !currentTaskIds.has(attempt.started.taskId)
          || this.definition(attempt.started.taskId).lineage !== attempt.started.taskLineage;
        const state: GraphAttemptView["state"] = attempt.completed
          ? "completed"
          : attempt.cancellation
            ? "cancelled"
            : attempt.superseded || staleLineage
              ? "superseded"
              : blocked
                ? "blocked"
                : "in_progress";
        return {
          id: attempt.started.attemptId,
          taskId: attempt.started.taskId,
          ordinal: attempt.started.ordinal,
          graphVersion: attempt.started.graphVersion,
          taskLineage: attempt.started.taskLineage,
          activationKey: attempt.started.activationKey,
          inputAttemptIds: clone(attempt.started.inputAttemptIds),
          assignee: attempt.started.assignee,
          state,
          current: currentAttemptIds.has(attempt.started.attemptId),
          ...(attempt.completed ? { outcome: attempt.completed.outcome, evidence: attempt.completed.evidence } : {}),
          ...(blocked ? { evidence: attempt.blocked!.evidence } : {}),
        };
      });
  }

  selectReadyFrontier(): GraphTaskView[] {
    this.flush();
    const selected: GraphTaskView[] = [];
    for (const [assignee, ids] of this.ready) {
      if (this.occupied.get(assignee)?.size || !ids.size) continue;
      const id = [...ids].sort((a, b) => a.localeCompare(b))[0];
      selected.push(clone(this.nodes.get(id)!.view!));
    }
    return selected.sort((a, b) => a.id.localeCompare(b.id));
  }

  trace(): { graphRevisions: GraphRevision[]; tasks: GraphTaskView[]; attempts: GraphAttemptView[]; events: GraphControlEvent[] } {
    return {
      graphRevisions: clone(this.graphRevisions),
      tasks: this.readTasks(),
      attempts: this.readAttempts(),
      events: clone(this.events),
    };
  }

  private claim(task: GraphTaskView, input: GraphTaskTransitionInput): void {
    if (task.state.kind !== "ready") this.invalid(task, "claim");
    this.assertWorker(task, input);
    const occupied = this.occupiedTask(task.assignee);
    if (occupied) {
      throw new GraphControlRefusal("worker_occupied", `Worker ${task.assignee} already has in-progress Task ${occupied}.`);
    }
    const derived = this.nodes.get(task.id)!.derived!;
    const revision = this.currentRevision()!;
    const ordinal = derived.attemptsStarted + 1;
    this.append({
      kind: "attempt_started",
      operationId: input.operationId,
      attemptId: `${task.id}@${ordinal}`,
      taskId: task.id,
      ordinal,
      graphVersion: revision.version,
      taskLineage: derived.definition.lineage,
      activationKey: derived.activationKey!,
      inputAttemptIds: clone(derived.inputAttemptIds),
      assignee: task.assignee,
    });
  }

  private block(task: GraphTaskView, input: GraphTaskTransitionInput): void {
    if (task.state.kind !== "in_progress") this.invalid(task, "block");
    this.assertWorker(task, input);
    this.append({
      kind: "attempt_blocked",
      operationId: input.operationId,
      attemptId: task.state.attemptId,
      evidence: requireText(input.evidence, "Blocker evidence"),
    });
  }

  private resume(task: GraphTaskView, input: GraphTaskTransitionInput): void {
    if (task.state.kind !== "blocked") this.invalid(task, "resume");
    this.assertWorker(task, input);
    const occupied = this.occupiedTask(task.assignee);
    if (occupied) {
      throw new GraphControlRefusal("worker_occupied", `Worker ${task.assignee} already has in-progress Task ${occupied}.`);
    }
    this.append({
      kind: "attempt_resumed",
      operationId: input.operationId,
      attemptId: task.state.attemptId,
    });
  }

  private complete(task: GraphTaskView, input: GraphTaskTransitionInput): GraphTransitionResult["failureTraversal"] {
    if (task.state.kind !== "in_progress") this.invalid(task, input.transition ?? "complete");
    this.assertWorker(task, input);
    const evidence = requireText(input.evidence, "Goal outcome evidence");
    this.append({
      kind: "attempt_completed",
      operationId: input.operationId,
      attemptId: task.state.attemptId,
      outcome: input.transition as GraphTaskOutcome,
      evidence,
    });
    if (input.transition === "goal_achieved") return undefined;

    const definition = this.definition(task.id);
    const edge = definition.onGoalFailed;
    if (!edge) return undefined;
    const priorTraversals = this.nodes.get(task.id)!.traversals;
    if (priorTraversals >= edge.maxTraversals) {
      this.append({
        kind: "failure_edge_exhausted",
        operationId: input.operationId,
        sourceTaskId: task.id,
        sourceTaskLineage: definition.lineage,
        sourceAttemptId: task.state.attemptId,
        targetTaskId: edge.target,
        traversals: priorTraversals,
        reason: "limit_reached",
      });
      return undefined;
    }

    const target = this.definition(edge.target);
    const targetCancelled = this.nodes.get(edge.target)!.cancellation;
    if (targetCancelled) {
      this.append({
        kind: "failure_edge_exhausted",
        operationId: input.operationId,
        sourceTaskId: task.id,
        sourceTaskLineage: definition.lineage,
        sourceAttemptId: task.state.attemptId,
        targetTaskId: edge.target,
        traversals: priorTraversals,
        reason: "target_cancelled",
      });
      return undefined;
    }
    this.flush();
    const affected = reverseClosure([edge.target], this.structuralTasks);
    const attemptsBefore = this.attemptsToSupersede(affected);
    const traversal = priorTraversals + 1;
    this.append({
      kind: "failure_edge_traversed",
      operationId: input.operationId,
      sourceTaskId: task.id,
      sourceTaskLineage: definition.lineage,
      sourceAttemptId: task.state.attemptId,
      targetTaskId: edge.target,
      targetTaskLineage: target.lineage,
      traversal,
    });
    for (const affectedTaskId of affected) {
      const attemptId = attemptsBefore.get(affectedTaskId);
      if (attemptId && attemptId !== task.state.attemptId) this.append({
        kind: "attempt_superseded",
        operationId: input.operationId,
        attemptId,
        reason: "repair_requested",
      });
    }
    return { sourceTaskId: task.id, targetTaskId: edge.target, traversal };
  }

  private cancel(task: GraphTaskView, input: GraphTaskTransitionInput): void {
    if (!["dependency_waiting", "ready", "in_progress", "blocked"].includes(task.state.kind)) this.invalid(task, "cancel");
    if (input.worker !== undefined) this.assertWorker(task, input);
    const reason = requireText(input.evidence, "Cancellation reason");
    const attemptId = task.state.kind === "in_progress" || task.state.kind === "blocked" ? task.state.attemptId : undefined;
    const affected = reverseClosure([task.id], this.structuralTasks);
    for (const id of affected) this.nodes.get(id);
    const activeBefore = this.activeAttemptIds();
    this.append({
      kind: "task_cancelled",
      operationId: input.operationId,
      taskId: task.id,
      taskLineage: this.definition(task.id).lineage,
      ...(attemptId ? { attemptId } : {}),
      reason,
    });
    for (const affectedTaskId of affected) {
      const affectedAttemptId = activeBefore.get(affectedTaskId);
      if (affectedAttemptId && affectedAttemptId !== attemptId) this.append({
        kind: "attempt_superseded",
        operationId: input.operationId,
        attemptId: affectedAttemptId,
        reason: "dependency_cancelled",
      });
    }
  }

  private assertWorker(task: GraphTaskView, input: GraphTaskTransitionInput): void {
    if (input.worker !== task.assignee) {
      throw new GraphControlRefusal("worker_mismatch", `Task ${task.id} is assigned to ${task.assignee}, not ${String(input.worker)}.`);
    }
    if (task.state.kind === "in_progress" || task.state.kind === "blocked") {
      const attemptId = task.state.attemptId;
      const attempt = this.attempts.get(attemptId)?.started;
      if (attempt?.assignee !== input.worker) {
        throw new GraphControlRefusal("worker_mismatch", `Attempt ${attemptId} belongs to Worker ${String(attempt?.assignee)}.`);
      }
    }
  }

  private invalid(task: GraphTaskView, transition: string): never {
    throw new GraphControlRefusal("invalid_transition", `Task ${task.id} is ${task.state.kind}; ${transition} is not valid.`);
  }

  private command<Result extends GraphControlResult>(operationId: string, semantics: unknown, apply: () => Result): Result & { replayed: boolean } {
    if (!operationId.trim()) throw new GraphControlRefusal("operation_conflict", "Operation identity must not be empty.");
    const fingerprint = digest(semantics);
    const prior = this.receipts.get(operationId);
    if (prior) {
      if (prior.fingerprint !== fingerprint) {
        throw new GraphControlRefusal("operation_conflict", `Operation ${operationId} was already used with different semantics.`);
      }
      return { ...clone(prior.result as Result), replayed: true };
    }

    const graphLength = this.graphRevisions.length;
    const eventLength = this.events.length;
    const receiptSize = this.receipts.size;
    const sequence = this.sequence;
    try {
      this.commandRecordedAt = this.clock().toISOString();
      const result = apply();
      this.receipts.set(operationId, { operationId, fingerprint, result: clone(result) });
      return { ...clone(result), replayed: false };
    } catch (error) {
      const changed = this.sequence !== sequence;
      this.graphRevisions.length = graphLength;
      this.events.length = eventLength;
      if (this.receipts.size !== receiptSize) this.receipts.delete(operationId);
      this.sequence = sequence;
      if (changed && !this.restoredRevisionCount && !(error instanceof GraphTaskRequired)) this.rebuild();
      throw error;
    } finally {
      this.commandRecordedAt = undefined;
    }
  }

  private append(input: GraphControlEventInput): void {
    const event = {
      ...input,
      sequence: this.nextSequence(),
      ...(this.commandRecordedAt ? { recorded_at: this.commandRecordedAt } : {}),
    } as GraphControlEvent;
    this.events.push(event);
    this.fold(event);
  }

  private nextSequence(): number { return ++this.sequence; }

  private currentRevision(): GraphRevision | undefined { return this.graphRevisions.at(-1); }

  private definition(taskId: string): StoredTaskDefinition {
    const node = this.nodes.get(taskId);
    if (!node) throw new GraphControlRefusal("task_not_found", `Task ${taskId} is not in the current graph.`);
    return node.definition;
  }

  private installGraph(tasks: StoredTaskDefinition[]): void {
    this.projections = new Map(this.projections);
    this.structuralTasks = tasks.map(({ key, needs }) => ({ key, needs }));
    const previous = this.nodes;
    this.nodes = new Map(tasks.map(definition => {
      const prior = previous.get(definition.key);
      if (prior?.definition.lineage === definition.lineage) return [definition.key, prior];
      this.dirty.add(definition.key);
      this.changed.add(definition.key);
      return [definition.key, { definition, demand: 0, traversals: 0, activations: new Map() }];
    }));
    for (const [id, prior] of previous) {
      if (this.nodes.get(id)?.definition.lineage !== prior.definition.lineage) {
        this.activeAttempts.delete(id); this.changed.add(id);
        if (!this.nodes.has(id)) this.projections.delete(id);
      }
    }
    this.dependents = new Map(tasks.map(task => [task.key, []]));
    for (const task of tasks) for (const need of task.needs) this.dependents.get(need)!.push(task.key);
    this.ranks = new Map(topologicalOrder(tasks).map((id, rank) => [id, rank]));
    this.ready.clear();
    this.occupied.clear();
    for (const [id, node] of this.nodes) {
      if (node.view) this.indexView(node.view, true);
      // A replaced prerequisite can change an otherwise unchanged Task.
      if (node.definition.needs.some(need => previous.get(need) !== this.nodes.get(need))) this.dirty.add(id);
    }
    for (const id of this.dirty) if (!this.nodes.has(id)) this.dirty.delete(id);
  }

  private fold(event: GraphControlEvent): void {
    if (event.kind === "attempt_started") {
      const attempt = { started: event };
      this.attempts.set(event.attemptId, attempt);
      this.attemptCounts.set(event.taskId, event.ordinal);
      const node = this.nodes.get(event.taskId);
      if (node?.definition.lineage === event.taskLineage) {
        let active = this.activeAttempts.get(event.taskId);
        if (!active) this.activeAttempts.set(event.taskId, active = new Map());
        active.set(event.attemptId, attempt);
        node.activations.set(event.activationKey, attempt);
        this.dirty.add(event.taskId);
      }
    } else if (event.kind === "failure_edge_traversed") {
      const source = this.nodes.get(event.sourceTaskId);
      if (source?.definition.lineage === event.sourceTaskLineage) source.traversals++;
      const target = this.nodes.get(event.targetTaskId);
      if (target?.definition.lineage === event.targetTaskLineage) {
        target.demand++;
        this.dirty.add(event.targetTaskId);
      }
    } else if (event.kind === "failure_edge_exhausted") {
      const attempt = this.attempts.get(event.sourceAttemptId)!;
      attempt.exhausted = event;
      if (this.nodes.get(event.sourceTaskId)?.definition.lineage === event.sourceTaskLineage) this.dirty.add(event.sourceTaskId);
    } else {
      if (event.kind === "task_cancelled" || event.kind === "task_context_updated") {
        const node = this.nodes.get(event.taskId);
        if (node?.definition.lineage === event.taskLineage) {
          if (event.kind === "task_cancelled") node.cancellation = event;
          else node.currentContext = event.currentContext;
          this.dirty.add(event.taskId);
        }
      }
      if ("attemptId" in event && event.attemptId) {
        const attempt = this.attempts.get(event.attemptId)!;
        if (event.kind === "attempt_blocked") attempt.blocked = event;
        else if (event.kind === "attempt_resumed") attempt.resumed = event;
        else if (event.kind === "attempt_completed") attempt.completed = event;
        else if (event.kind === "attempt_superseded") attempt.superseded = event;
        else if (event.kind === "task_cancelled") attempt.cancellation = event;
        if ((event.kind === "attempt_completed" || event.kind === "attempt_superseded") && attempt.endedSequence === undefined) {
          attempt.endedSequence = event.sequence;
          if (event.recorded_at) attempt.endedAt = event.recorded_at;
        }
        const { taskId, taskLineage, activationKey } = attempt.started;
        const node = this.nodes.get(taskId);
        if (attempt.completed || attempt.superseded || attempt.cancellation) {
          const active = this.activeAttempts.get(taskId);
          active?.delete(event.attemptId);
          if (!active?.size) this.activeAttempts.delete(taskId);
        }
        if (node?.definition.lineage === taskLineage) {
          if ((attempt.superseded || attempt.cancellation) && node.activations.get(activationKey) === attempt) node.activations.delete(activationKey);
          this.dirty.add(taskId);
        }
      }
    }
  }

  // Only a post-mutation exception replays accepted history. Refusals before
  // mutation leave the indexes untouched.
  private rebuild(): void {
    this.nodes.clear();
    this.attempts.clear();
    this.attemptCounts.clear();
    this.activeAttempts.clear();
    this.dirty.clear();
    this.projections.clear();
    this.installGraph(this.currentRevision()?.tasks ?? []);
    for (const event of this.events) this.fold(event);
    this.flush();
  }

  private flush(): void {
    // Work is ordered by DAG rank. Each node runs once per flush, even at joins.
    const queue = [...this.dirty].filter(id => this.nodes.has(id))
      .sort((a, b) => this.ranks.get(a)! - this.ranks.get(b)!);
    for (let cursor = 0; cursor < queue.length; cursor++) {
      const id = queue[cursor];
      this.dirty.delete(id);
      const node = this.nodes.get(id)!;
      const prior = node.derived;
      if (node.view) this.indexView(node.view, false);
      node.derived = this.deriveNode(node);
      node.view = this.toView(node.derived);
      this.changed.add(id);
      this.projections.set(id, prerequisiteProjection(node.derived));
      this.indexView(node.view, true);
      if (prerequisiteProjection(prior) === prerequisiteProjection(node.derived)) continue;
      for (const dependent of this.dependents.get(id) ?? []) {
        if (this.dirty.has(dependent)) continue;
        this.dirty.add(dependent);
        const rank = this.ranks.get(dependent)!;
        let lo = cursor + 1, hi = queue.length;
        while (lo < hi) {
          const mid = (lo + hi) >>> 1;
          if (this.ranks.get(queue[mid])! < rank) lo = mid + 1;
          else hi = mid;
        }
        queue.splice(lo, 0, dependent);
      }
    }
  }

  private indexView(view: GraphTaskView, add: boolean): void {
    const index = view.state.kind === "ready" ? this.ready
      : view.state.kind === "in_progress" ? this.occupied : undefined;
    if (!index) return;
    if (!add) {
      const ids = index.get(view.assignee);
      ids?.delete(view.id);
      if (!ids?.size) index.delete(view.assignee);
    } else {
      let ids = index.get(view.assignee);
      if (!ids) index.set(view.assignee, ids = new Set());
      ids.add(view.id);
    }
  }

  private readyIds(): string[] { return [...this.ready.values()].flatMap(ids => [...ids]).sort(); }

  private occupiedTask(assignee: string): string | undefined {
    this.flush();
    return [...(this.occupied.get(assignee) ?? [])].sort((a, b) => a.localeCompare(b))[0];
  }

  private activeAttemptIds(): Map<string, string> {
    return new Map([...this.activeAttempts].flatMap(([id, attempts]) => {
      const node = this.nodes.get(id);
      const last = [...attempts.values()].findLast(attempt => attempt.started.taskLineage === node?.definition.lineage);
      return last ? [[id, last.started.attemptId] as const] : [];
    }));
  }

  private attemptsToSupersede(ids: Iterable<string> = this.nodes.keys()): Map<string, string> {
    const nodes = [...ids].map(id => [id, this.nodes.get(id)!] as const);
    const result = this.activeAttemptIds();
    for (const [id, node] of nodes) {
      const state = node.derived?.state;
      if (!result.has(id) && state && "attemptId" in state) result.set(id, state.attemptId);
    }
    return result;
  }

  private deriveNode(node: TaskNode): DerivedTask {
    const definition = node.definition;
    const cancellation = node.cancellation;
    const failedPrerequisites: string[] = [];
    const cancelledPrerequisites: string[] = [];
    const waitingPrerequisites: string[] = [];
    const inputAttempts: Array<[string, string]> = [];
    for (const prerequisiteId of definition.needs) {
      const projection = this.projections.get(prerequisiteId)!;
      if (projection.startsWith("achieved:")) {
        inputAttempts.push([prerequisiteId, projection.slice("achieved:".length)]);
      } else if (projection === "cancelled") {
        cancelledPrerequisites.push(prerequisiteId);
      } else if (projection === "failed") {
        failedPrerequisites.push(prerequisiteId);
      } else {
        waitingPrerequisites.push(prerequisiteId);
      }
    }

    const inputAttemptIds = Object.fromEntries(inputAttempts);
    const demand = node.demand;
    let state: GraphTaskState;
    let activationKey: string | undefined;
    let acceptedAttemptId: string | undefined;
    if (cancellation) {
      state = { kind: "cancelled", reason: cancellation.reason };
    } else if (cancelledPrerequisites.length) {
      state = { kind: "goal_failed", reason: "dependency_cancelled", prerequisiteTaskIds: cancelledPrerequisites.sort() };
    } else if (failedPrerequisites.length) {
      state = { kind: "goal_failed", reason: "dependency_failed", prerequisiteTaskIds: failedPrerequisites.sort() };
    } else if (waitingPrerequisites.length) {
      state = { kind: "dependency_waiting", prerequisiteTaskIds: waitingPrerequisites.sort() };
    } else {
      activationKey = digest({ taskLineage: definition.lineage, demand, inputAttemptIds });
      const current = node.activations.get(activationKey);
      if (!current) {
        state = { kind: "ready" };
      } else if (!current.completed) {
        const blocked = current.blocked && (!current.resumed || current.blocked.sequence > current.resumed.sequence);
        state = blocked
          ? { kind: "blocked", attemptId: current.started.attemptId, evidence: current.blocked!.evidence }
          : { kind: "in_progress", attemptId: current.started.attemptId };
      } else if (current.completed.outcome === "goal_achieved") {
        acceptedAttemptId = current.started.attemptId;
        state = { kind: "goal_achieved", attemptId: acceptedAttemptId };
      } else {
        const exhausted = current.exhausted;
        state = exhausted
          ? {
            kind: "goal_failed",
            reason: "failure_edge_exhausted",
            attemptId: current.started.attemptId,
            targetTaskId: exhausted.targetTaskId,
            traversals: exhausted.traversals,
            exhaustionReason: exhausted.reason,
          }
          : { kind: "goal_failed", reason: "criterion_failed", attemptId: current.started.attemptId };
      }
    }
    return {
      definition, state,
      ...(activationKey ? { activationKey } : {}),
      inputAttemptIds,
      ...(acceptedAttemptId ? { acceptedAttemptId } : {}),
      attemptsStarted: this.attemptCounts.get(definition.key) ?? 0,
    };
  }

  private toView(task: DerivedTask): GraphTaskView {
    const value = {
      id: task.definition.key,
      title: task.definition.title,
      goal: task.definition.goal,
      assignee: task.definition.assignee,
      needs: clone(task.definition.needs),
      ...(task.definition.onGoalFailed ? { onGoalFailed: clone(task.definition.onGoalFailed) } : {}),
      state: clone(task.state),
      currentContext: this.nodes.get(task.definition.key)!.currentContext ?? "Work has not started.",
      ...(task.activationKey ? { activationKey: task.activationKey } : {}),
      ...(task.acceptedAttemptId ? { acceptedAttemptId: task.acceptedAttemptId } : {}),
      attemptsStarted: task.attemptsStarted,
    };
    return { ...value, version: taskVersionRef(JSON.stringify(canonical(value))) };
  }
}

export function prerequisiteProjection(task: DerivedTask | undefined): string {
  const state = task?.state;
  if (!state) return "absent";
  if (state.kind === "goal_achieved") return `achieved:${state.attemptId}`;
  if (state.kind === "goal_failed") return "failed";
  if (state.kind === "cancelled") return "cancelled";
  return "waiting";
}

function readyIds(tasks: GraphTaskView[]): string[] {
  return tasks.filter(task => task.state.kind === "ready").map(task => task.id).sort();
}

function normalizeAndValidate(input: GraphTaskDefinitionInput[]): Array<Omit<StoredTaskDefinition, "lineage">> {
  if (!input.length) throw new GraphControlRefusal("invalid_graph", "A graph requires at least one Task.");
  const normalized = input.map(task => ({
    key: task.key,
    title: task.title,
    goal: task.goal,
    assignee: task.assignee,
    needs: [...(task.needs ?? [])].sort(),
    ...(task.onGoalFailed ? { onGoalFailed: { ...task.onGoalFailed } } : {}),
  }));
  const byId = new Map<string, typeof normalized[number]>();
  for (const task of normalized) {
    if (!task.key.trim() || !task.title.trim() || !task.goal.trim() || !task.assignee.trim()) {
      throw new GraphControlRefusal("invalid_graph", "Every Task requires nonempty key, title, goal, and assignee values.");
    }
    if (byId.has(task.key)) throw new GraphControlRefusal("invalid_graph", `Task key ${task.key} occurs more than once.`);
    if (new Set(task.needs).size !== task.needs.length) {
      throw new GraphControlRefusal("invalid_graph", `Task ${task.key} repeats a prerequisite.`);
    }
    byId.set(task.key, task);
  }
  for (const task of normalized) {
    for (const prerequisite of task.needs) {
      if (!byId.has(prerequisite)) throw new GraphControlRefusal("invalid_graph", `Task ${task.key} needs missing Task ${prerequisite}.`);
      if (prerequisite === task.key) throw new GraphControlRefusal("invalid_graph", `Task ${task.key} cannot need itself.`);
    }
    if (task.onGoalFailed) {
      if (!Number.isInteger(task.onGoalFailed.maxTraversals)
        || task.onGoalFailed.maxTraversals < 1
        || task.onGoalFailed.maxTraversals > GRAPH_CONTROL_MAX_FAILURE_TRAVERSALS) {
        throw new GraphControlRefusal(
          "invalid_graph",
          `Task ${task.key} failure maxTraversals must be from 1 through ${GRAPH_CONTROL_MAX_FAILURE_TRAVERSALS}.`,
        );
      }
      if (!byId.has(task.onGoalFailed.target)) {
        throw new GraphControlRefusal("invalid_graph", `Task ${task.key} has missing failure target ${task.onGoalFailed.target}.`);
      }
    }
  }
  topologicalOrder(normalized as StoredTaskDefinition[]);
  for (const task of normalized) {
    const target = task.onGoalFailed?.target;
    if (target && target !== task.key && !ancestors(task.key, byId).has(target)) {
      throw new GraphControlRefusal(
        "invalid_graph",
        `Task ${task.key} failure target ${target} must be itself or a transitive success prerequisite.`,
      );
    }
  }
  return normalized;
}

function topologicalOrder(tasks: readonly Pick<StoredTaskDefinition, "key" | "needs">[]): string[] {
  const byId = new Map(tasks.map(task => [task.key, task]));
  const visiting = new Set<string>();
  const visited = new Set<string>();
  const order: string[] = [];
  const visit = (taskId: string, path: string[]): void => {
    if (visiting.has(taskId)) {
      throw new GraphControlRefusal("invalid_graph", `Success dependency cycle: ${[...path, taskId].join(" -> ")}.`);
    }
    if (visited.has(taskId)) return;
    visiting.add(taskId);
    for (const prerequisite of byId.get(taskId)?.needs ?? []) visit(prerequisite, [...path, taskId]);
    visiting.delete(taskId);
    visited.add(taskId);
    order.push(taskId);
  };
  for (const taskId of [...byId.keys()].sort()) visit(taskId, []);
  return order;
}

function ancestors<T extends { needs: string[] }>(taskId: string, tasks: ReadonlyMap<string, T>): Set<string> {
  const result = new Set<string>();
  const collect = (id: string): void => {
    for (const prerequisite of tasks.get(id)?.needs ?? []) {
      if (result.has(prerequisite)) continue;
      result.add(prerequisite);
      collect(prerequisite);
    }
  };
  collect(taskId);
  return result;
}

function reverseClosure(seed: string[], tasks: readonly Pick<StoredTaskDefinition, "key" | "needs">[]): Set<string> {
  const result = new Set(seed);
  let changed = true;
  while (changed) {
    changed = false;
    for (const task of tasks) {
      if (result.has(task.key) || !task.needs.some(prerequisite => result.has(prerequisite))) continue;
      result.add(task.key);
      changed = true;
    }
  }
  return result;
}

export { digest as graphControlFingerprint };
