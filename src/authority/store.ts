// Current authority uses base-checkpointed records. The controller log is read
// only for migration, explicit history inspection, and generation reconciliation.
import { createHash } from 'node:crypto';
import { defineDoc, defineDocFamily, type Tx, type JsonObject, type ConversationId, type TaskId, type Cursor } from '@earendil-works/pi-durable';
import { GraphTaskController, GraphControlRefusal, type GraphControlSnapshot, type GraphControlEvent, type GraphRevision, type GraphApplyInput, type GraphTaskTransitionInput } from './graph-control.ts';
import { IncrementalGraphTaskController, GraphTaskRequired, prerequisiteProjection, graphControlFingerprint, type GraphMaterializedHead, type GraphMaterializedTask } from './incremental-graph-control.ts';
import { projectGraphTaskCards } from './graph-card.ts';
import type { GraphTaskCard } from './card-types.ts';
import { TeamDoc, GraphDoc, JournalDoc, OutboxDoc, ReceiptDoc, plain, fenceLegacyDoc, hasLegacyFence, type TeamState, type JournalState, type JournalRef, type Event, type Alert, type Obligation, type Receipt, type State } from './state.ts';

import { openObservations } from './observation-store.ts';

const family = (kind: string) => defineDocFamily<JsonObject, null>({ kind, version: 1, scope: 'session', family: true, initial: () => ({}), checkpointWhen: () => true });
const dictionary = <T>():Record<string,T> => Object.create(null);
type GraphHead=Omit<GraphMaterializedHead,'topology'|'projections'>;
const emptyGraph = (): GraphHead => ({ sequence: 0, ready: {}, occupied: {} });
export interface AuthorityHead {
 initialized: boolean; legacyFenced?:boolean; epoch?: string; graph?: GraphHead; taskCount:number; log?: ConversationId;
 journal: Omit<JournalState,'events'> & {events:string}; pendingBuckets: Record<string, true>; unscheduledBuckets: Record<string, true>; obligationSequence:number; recentObligations: string[]; recentAlerts: string[];
 leadAlerts: string[]; leadAlertCount: number; obligationTail?: string; alertTail?: string; callTail?: string;
 attention: Record<string, 'blocked'|'failed'>;
 frontier: Record<string, {id:string;version:string;generation:number}>;
 taskObligations: Record<string, string[]>;
 workerBindings:Record<string,string>;
 stopRefresh?: boolean;
}
const initialHead = ():AuthorityHead => ({ initialized: false, journal: { seq: 0, events: '[]', acknowledged: 0 }, pendingBuckets: {}, unscheduledBuckets: {}, obligationSequence:0, recentObligations: [], recentAlerts: [], leadAlerts: [], leadAlertCount: 0, taskCount:0, attention: {}, frontier: {}, taskObligations: {}, workerBindings:{} });
// The typed envelope is replaced as one base. Opaque JSON avoids tracking every
// unchanged index leaf through Chord's draft machinery.
export const HeadDoc = defineDoc<JsonObject>({kind:'ptb.head',version:1,scope:'session',checkpointWhen:()=>true,initial:()=>({value:JSON.stringify(initialHead())})});
export const TaskDoc = family('ptb.task');
export const GraphIndexDoc = family('ptb.graph-index');
export const WorkerIndexDoc = family('ptb.worker-index');
export const OrdinalDoc = family('ptb.ordinal');
export const OperationDoc = family('ptb.op');
export const ObligationDoc = family('ptb.obl');
export const EligibilityDoc = family('ptb.eligibility');
export const AlertDoc = family('ptb.alert');
export const DirectCallDoc = family('ptb.call.direct');
export const CallDoc = defineDoc<JsonObject>({ kind: 'ptb.call', version: 1, scope: 'task', initial: () => ({}), checkpointWhen: () => true });
const address = (epoch: string, id: string) => JSON.stringify([epoch, id]);
export const taskAddress = address;
export interface OperatorTask {
 key: string; title: string; goal: string; assignee: string; needs: string[]; version: string; current_context: string;
 attempts: { id: string; ordinal: number; startedAt?: number; endedAt?: number; outcome?: string; evidence?: string }[];
 derived: string; status: string; derivedReason?: string; evidence?: string;
}
export interface TaskRecord { materialized: GraphMaterializedTask; card: GraphTaskCard; operator: OperatorTask }
interface OperationRecord { legacy?:boolean; operationId: string; fingerprint: string; result: string; digest: string }
interface StoredCall { fingerprint?: string; result?: string; stops?: string[]; epoch?: string; previous?: string }
interface Linked<T> { value?: T; previous?: string; order?:number }
type CurrentJournal=Omit<JournalState,'events'>&{events:JournalRef[]};
interface Eligibility {doc:JsonObject;pending:Record<string,number>;unscheduled:Record<string,number>}
export interface Authority {
 tx: Tx; team: TeamState; head: AuthorityHead; journal: CurrentJournal;
 headDoc:JsonObject; deferFlush:boolean; flush:()=>void;
 records: Map<string, TaskRecord | undefined>;
 obligations: Map<string, Obligation>;
 eligibility:Map<string,Eligibility>;
 newObligations: Set<string>;
 calls: Map<number,{doc:StoredCall;native:boolean}>;
 workerIndexes:Map<string,string[]>;
 migratedHistory?:Map<string,GraphControlSnapshot>;
}
const sha = (text: string) => createHash('sha256').update(text).digest('hex');
const terminal = (kind: string) => ['goal_achieved','goal_failed','cancelled'].includes(kind);
const ring = (ids: string[], id: string, limit: number) => { ids.push(id); if (ids.length > limit) ids.splice(0, ids.length - limit); };

function journalView(head:AuthorityHead,flush:()=>void):CurrentJournal {
 let cached:JournalRef[]|undefined;
 const target={} as CurrentJournal;
 for(const key of ['seq','acknowledged','reservation'] as const)Object.defineProperty(target,key,{enumerable:true,configurable:true,get:()=>head.journal[key],set:value=>{(head.journal as any)[key]=value;flush();}});
 Object.defineProperty(target,'events',{enumerable:true,configurable:true,get:()=>cached??=JSON.parse(head.journal.events),set:value=>{cached=value;head.journal.events=JSON.stringify(value.map((e:JournalRef)=>e.entryId===undefined?e:{seq:e.seq,entryId:e.entryId}));flush();}});
 return new Proxy(target,{deleteProperty(object,key){if(key==='reservation'){delete head.journal.reservation;flush();}else Reflect.deleteProperty(object,key);return true;}});
}

export async function recentEvents(s:Authority,refs=s.journal.events):Promise<Event[]> {
 return Promise.all(refs.map(async ref=>{
  if('kind' in ref)return ref;
  const entry=await s.tx.entry(ref.entryId);
  if(entry?.kind!=='ptb.event'||(entry.data as any)?.seq!==ref.seq)throw new Error('Journal event reference is corrupt.');
  return {...entry.data as unknown as Event,entryId:entry.id};
 }));
}

const transactions = new WeakMap<Tx,Authority>();
export function flushAuthority(s:Authority) { s.headDoc.value=JSON.stringify(s.head); }
export async function openAuthority(tx: Tx): Promise<Authority> {
 const cached=transactions.get(tx);if(cached)return cached;
 const team = await tx.doc(TeamDoc) as unknown as TeamState;
 const headDoc = await tx.doc(HeadDoc);
 const head = JSON.parse(headDoc.value as string) as AuthorityHead;
 for(const map of [head.pendingBuckets,head.unscheduledBuckets,head.attention,head.frontier,head.taskObligations,head.workerBindings])Object.setPrototypeOf(map,null);
 const s: Authority = { tx, team, head, headDoc, deferFlush:false, flush:()=>{if(!s.deferFlush)flushAuthority(s);}, journal: journalView(head,()=>s.flush()), records: new Map(), obligations: new Map(), eligibility:new Map(), newObligations: new Set(), calls:new Map(),workerIndexes:new Map() };
 transactions.set(tx,s);
 // Fence observation access before migration appends controller-log entries.
 if(!head.legacyFenced)await openObservations(tx);
 if(!head.initialized)await migrate(s);
 if(!head.legacyFenced){
  for(const token of [GraphDoc,JournalDoc,OutboxDoc,ReceiptDoc])fenceLegacyDoc(await tx.doc(token));
  head.legacyFenced=true;flushAuthority(s);
 }
 return s;
}
export async function startEpoch(s: Authority, epoch: string) {
 s.head.epoch = epoch; s.head.graph = emptyGraph(); s.head.taskCount=0; s.head.attention = dictionary(); s.head.frontier = dictionary(); s.head.workerBindings = dictionary();
 s.head.leadAlerts = []; s.head.leadAlertCount = 0; s.head.stopRefresh = false;
 s.records.clear();
 if (s.head.log === undefined) s.head.log = (await s.tx.createConversation({ ownership: { kind: 'ownerless' } })).id;
}
const graphIndexes=new Map<string,{topology:GraphMaterializedHead['topology'];ids:Set<string>}>();
async function graphIndex(s:Authority) {
 const version=s.head.graph?.revision?.version;
 if(!version)return {topology:[],ids:new Set<string>()};
 let cached=graphIndexes.get(version);
 if(!cached){
  const doc=await s.tx.doc(GraphIndexDoc,address(s.head.epoch!,version),null);
  if(typeof doc.value!=='string')throw new Error('Established graph authority is corrupt.');
  const topology=JSON.parse(doc.value) as GraphMaterializedHead['topology'];
  cached={topology,ids:new Set(topology.map(t=>t.key))};
  if(graphIndexes.size>=32)graphIndexes.delete(graphIndexes.keys().next().value!);
  graphIndexes.set(version,cached);
 }
 return cached;
}
export async function taskIds(s:Authority) { return [...(await graphIndex(s)).ids]; }
async function saveGraphHead(s:Authority,head:GraphMaterializedHead) {
 const {topology,projections:_projections,...small}=head;
 s.head.graph=small;s.head.taskCount=topology.length;
 if(head.revision){
  (await s.tx.doc(GraphIndexDoc,address(s.head.epoch!,head.revision.version),null)).value=JSON.stringify(topology);
  if(graphIndexes.size>=32)graphIndexes.delete(graphIndexes.keys().next().value!);
  graphIndexes.set(head.revision.version,{topology,ids:new Set(topology.map(t=>t.key))});
 }
}

export async function taskRecord(s: Authority, id: string): Promise<TaskRecord | undefined> {
 if (s.records.has(id)) return s.records.get(id);
 if (!(await graphIndex(s)).ids.has(id)) { s.records.set(id, undefined); return; }
 const doc = await s.tx.doc(TaskDoc, address(s.head.epoch!, id), null) as unknown as { value?: string };
 const value=doc.value?JSON.parse(doc.value) as TaskRecord:undefined;
 if (!value?.materialized?.node || !value.card) throw new Error('Established graph authority is corrupt.');
 s.records.set(id, value); return value;
}
export async function taskCards(s: Authority, ids?:string[]): Promise<GraphTaskCard[]> {
 ids??=await taskIds(s);
 const cards = await Promise.all(ids.map(async id => (await taskRecord(s, id))?.card));
 return cards.filter((card): card is GraphTaskCard => !!card).map(plain).sort((a,b) => a.id.localeCompare(b.id));
}
async function workerIndex(s:Authority,worker:string) {
 const key=address(s.head.epoch!,worker),cached=s.workerIndexes.get(key);if(cached)return cached;
 const doc=await s.tx.doc(WorkerIndexDoc,key,null),ids=typeof doc.value==='string'?JSON.parse(doc.value) as string[]:[];
 s.workerIndexes.set(key,ids);return ids;
}
export async function unfinished(s: Authority, worker?: string): Promise<string[]> {
 const ids=worker?await workerIndex(s,worker):(await taskCards(s)).filter(t=>!terminal(t.status)).map(t=>t.id);
 return [...ids].sort((a,b)=>a.localeCompare(b));
}
async function changeWorkerIndex(s:Authority,worker:string,id:string,add:boolean){
 const ids=await workerIndex(s,worker),at=ids.indexOf(id);
 if(add&&at===-1)ids.push(id);else if(!add&&at!==-1)ids.splice(at,1);else return;
 (await s.tx.doc(WorkerIndexDoc,address(s.head.epoch!,worker),null)).value=JSON.stringify(ids);
}
export async function obligation(s: Authority, key: string): Promise<Obligation | undefined> {
 if (s.obligations.has(key)) return s.obligations.get(key);
 const doc = await s.tx.doc(ObligationDoc, key, null) as unknown as Linked<Obligation>;
 if (doc.value) s.obligations.set(key, doc.value);
 return doc.value;
}
const bucketOf=(key:string)=>String(Number.parseInt(sha(key).slice(0,8),16)%64);
async function eligibility(s:Authority,bucket:string):Promise<Eligibility>{
 let value=s.eligibility.get(bucket);if(value)return value;
 const doc=await s.tx.doc(EligibilityDoc,bucket,null);
 value={doc,...(typeof doc.value==='string'?JSON.parse(doc.value):{pending:{},unscheduled:{}})};
 s.eligibility.set(bucket,value!);return value!;
}
function saveEligibility(s:Authority,bucket:string,index:Eligibility){
 index.doc.value=JSON.stringify({pending:index.pending,unscheduled:index.unscheduled});
 for(const kind of ['pending','unscheduled'] as const){
  const head=kind==='pending'?s.head.pendingBuckets:s.head.unscheduledBuckets;
  if(Object.keys(index[kind]).length)head[bucket]=true;else delete head[bucket];
 }
}
async function indexedKeys(s:Authority,kind:'pending'|'unscheduled'){
 const buckets=Object.keys(kind==='pending'?s.head.pendingBuckets:s.head.unscheduledBuckets);
 const entries=(await Promise.all(buckets.map(async bucket=>Object.entries((await eligibility(s,bucket))[kind])))).flat();
 return entries.sort((a,b)=>a[1]-b[1]).map(([key])=>key);
}
export async function markScheduled(s:Authority,key:string){
 const bucket=bucketOf(key),index=await eligibility(s,bucket);delete index.unscheduled[key];saveEligibility(s,bucket,index);
}
export async function unscheduledObligations(s:Authority){
 return (await Promise.all((await indexedKeys(s,'unscheduled')).map(key=>obligation(s,key)))).filter((o):o is Obligation=>!!o);
}
export async function addObligation(s: Authority, value: Obligation) {
 const doc = await s.tx.doc(ObligationDoc, value.key, null) as unknown as Linked<Obligation>;
 if (doc.value) return;
 if (s.head.obligationTail) doc.previous = s.head.obligationTail;
 doc.value = plain(value); doc.order=++s.head.obligationSequence; s.head.obligationTail = value.key;
 if(value.state==='pending'){
  const bucket=bucketOf(value.key),index=await eligibility(s,bucket);index.pending[value.key]=doc.order;
  if(!value.deliveryTaskId)index.unscheduled[value.key]=doc.order;
  saveEligibility(s,bucket,index);
  if(value.kind==='task'){s.head.taskObligations[value.taskId!]??=[];s.head.taskObligations[value.taskId!].push(value.key);}
 }
 ring(s.head.recentObligations, value.key, 60);
 s.obligations.set(value.key, doc.value); s.newObligations.add(value.key);
}
export async function setObligationState(s: Authority, value: Obligation, state: Obligation['state']) {
 value.state = state;
 const bucket=bucketOf(value.key),index=await eligibility(s,bucket);
 if (state === 'pending') {const doc=await s.tx.doc(ObligationDoc,value.key,null);index.pending[value.key]=Number(doc.order);}
 else {
  delete index.pending[value.key]; delete index.unscheduled[value.key];
  if(value.kind==='task'){
   const keys=s.head.taskObligations[value.taskId!];
   if(keys){const at=keys.indexOf(value.key);if(at!==-1)keys.splice(at,1);if(!keys.length)delete s.head.taskObligations[value.taskId!];}
  }
 }
 saveEligibility(s,bucket,index);
}
export async function allObligations(s: Authority): Promise<Obligation[]> {
 const values:Obligation[]=[];let key=s.head.obligationTail;
 while(key){const doc=await s.tx.doc(ObligationDoc,key,null) as unknown as Linked<Obligation>;if(doc.value){values.push(doc.value);s.obligations.set(key,doc.value);}key=doc.previous;}
 return values.reverse();
}
export async function pendingObligations(s: Authority): Promise<Obligation[]> {
 return (await Promise.all((await indexedKeys(s,'pending')).map(key => obligation(s, key)))).filter((o): o is Obligation => !!o);
}
export async function alertRecord(s: Authority, id: string): Promise<Alert | undefined> {
 return (await s.tx.doc(AlertDoc, id, null) as unknown as Linked<Alert>).value;
}
export async function addAlert(s: Authority, value: Alert) {
 const doc = await s.tx.doc(AlertDoc, value.id, null) as unknown as Linked<Alert>;
 if (doc.value) return;
 if (s.head.alertTail) doc.previous = s.head.alertTail;
 doc.value = plain(value); s.head.alertTail = value.id;
 ring(s.head.recentAlerts, value.id, 30);
 if (value.recipients.some(r => r.name === 'team-lead' && r.conversationId === s.team.current?.lead) && value.id.startsWith(`${s.head.epoch}:`)) {
  ring(s.head.leadAlerts, value.id, 10); s.head.leadAlertCount++;
 }
}
export async function callReceipt(s: Authority, id: TaskId, native?: boolean): Promise<StoredCall> {
 const cached=s.calls.get(id);if(cached)return cached.doc;
 native??=(await s.tx.task(id))?.kind==='pi.tool';
 const doc=native?await s.tx.doc(CallDoc,id) as unknown as StoredCall:await s.tx.doc(DirectCallDoc,String(id),null) as unknown as StoredCall;
 s.calls.set(id,{doc,native});return doc;
}
export async function saveCall(s: Authority, id: TaskId, receipt: Receipt) {
 const doc = await callReceipt(s, id),native=s.calls.get(id)!.native;
 if (!doc.fingerprint && !native) {
  if (s.head.callTail) doc.previous = s.head.callTail;
  s.head.callTail = String(id);
 }
 doc.fingerprint = receipt.fingerprint;
 if (receipt.result) { doc.result = JSON.stringify(receipt.result); delete doc.stops; delete doc.epoch; }
 else { doc.stops = receipt.stops; doc.epoch = receipt.epoch; delete doc.result; }
 flushAuthority(s);
}

async function indexTask(s: Authority, prior: TaskRecord | undefined, next: TaskRecord | undefined, id: string) {
 const before=prior&&!terminal(prior.card.status)?prior.card.assignee:undefined;
 const after=next&&!terminal(next.card.status)?next.card.assignee:undefined;
 if(before!==after){
  if(before!==undefined)await changeWorkerIndex(s,before,id,false);
  if(after!==undefined)await changeWorkerIndex(s,after,id,true);
 }
 delete s.head.attention[id];
 if(next?.card.status==='blocked')s.head.attention[id]='blocked';
 else if(next?.card.status==='goal_failed'&&!String(next.operator.derivedReason??'').startsWith('dependency'))s.head.attention[id]='failed';
}
function rendered(engine: IncrementalGraphTaskController, id: string): TaskRecord {
 const materialized = engine.materializedTask(id), view = engine.readTask(id), attempts = engine.readAttempts(id);
 const [card] = projectGraphTaskCards({ readTasks: () => [view], readAttempts: () => attempts, selectReadyFrontier: () => [] });
 const operatorAttempts = attempts.map(a => {
  const fold = materialized.attempts.find(f => f.started.attemptId === a.id)!;
  return { id:a.id, ordinal:a.ordinal, ...(fold.started.recorded_at ? {startedAt:Date.parse(fold.started.recorded_at)} : {}), ...(fold.endedAt ? {endedAt:Date.parse(fold.endedAt)} : {}), ...(a.outcome ? {outcome:a.outcome} : {}), ...(a.evidence ? {evidence:a.evidence} : {}) };
 });
 const evidence = view.state.kind === 'blocked' ? view.state.evidence : operatorAttempts.at(-1)?.evidence;
 const operator: OperatorTask = { key:id, title:view.title, goal:view.goal, assignee:view.assignee, needs:view.needs, version:view.version, current_context:view.currentContext, attempts:operatorAttempts, derived:view.state.kind,
  status:['ready','dependency_waiting'].includes(view.state.kind)?'pending':view.state.kind, ...('reason' in view.state ? {derivedReason:view.state.reason} : {}), ...(evidence ? {evidence} : {}) };
 return { materialized, card, operator };
}
async function writeTask(s: Authority, engine: IncrementalGraphTaskController, id: string, present: boolean) {
 const prior = await taskRecord(s, id);
 const materialized = engine.materializedTask(id);
 const counter = await s.tx.doc(OrdinalDoc, address(s.head.epoch!, id), null);
 counter.count = materialized.attemptsStarted;
 let next: TaskRecord | undefined;
 if (present) {
  next = rendered(engine, id);
  (await s.tx.doc(TaskDoc, address(s.head.epoch!, id), null)).value = JSON.stringify(next);
 } else (await s.tx.doc(TaskDoc, address(s.head.epoch!, id), null)).value = JSON.stringify({materialized});
 await indexTask(s, prior, next, id); s.records.set(id, next);
}
async function logRevision(s: Authority, revision: GraphRevision, operationId: string, prior: GraphRevision['tasks']) {
 const previous = new Map(prior.map(task => [task.key, task]));
 const upserts = revision.tasks.filter(task => previous.get(task.key)?.lineage !== task.lineage);
 const keys = new Set(revision.tasks.map(task => task.key));
 await s.tx.appendEntry(s.head.log!, { kind:'ptb.graph-revision', data: plain({ epoch:s.head.epoch, operationId, sequence:revision.sequence, version:revision.version, recorded_at:revision.recorded_at, order:revision.tasks.map(t=>t.key), upserts, removed:prior.filter(t=>!keys.has(t.key)).map(t=>t.key) }) as unknown as JsonObject });
}
async function logEvent(s: Authority, event: GraphControlEvent) {
 await s.tx.appendEntry(s.head.log!, { kind:'ptb.graph-event', data:plain({epoch:s.head.epoch,event}) as unknown as JsonObject });
}
async function saveOperation(s: Authority, receipt: GraphControlSnapshot['receipts'][number],legacy=false) {
 const result = JSON.stringify(receipt.result);
 Object.assign(await s.tx.doc(OperationDoc, address(s.head.epoch!, receipt.operationId), null), { operationId:receipt.operationId, fingerprint:receipt.fingerprint, result, digest:sha(result),...(legacy?{legacy:true}:{}) });
}
function checkedOperation(doc: Partial<OperationRecord>,fingerprint?:string): GraphControlSnapshot['receipts'][number] | undefined {
 if (!doc.operationId) return;
 if(fingerprint&&fingerprint!==doc.fingerprint)throw new GraphControlRefusal('operation_conflict',`Operation ${doc.operationId} was already used with different semantics.`);
 let result: GraphControlSnapshot['receipts'][number]['result'];
 try { result = JSON.parse(doc.result!); }
 catch { throw new GraphControlRefusal('invalid_graph', `Operation ${doc.operationId} receipt is malformed: invalid JSON`); }
 if (!result || !['graph_applied','task_transitioned'].includes(result.kind)) throw new GraphControlRefusal('invalid_graph','Graph authority snapshot has a receipt without a committed command.');
 if (sha(doc.result!) !== doc.digest) throw new GraphControlRefusal('invalid_graph',result.kind==='graph_applied'?'Graph authority snapshot has a graph receipt that differs from committed state.':'Graph authority snapshot has a Task receipt that differs from committed state.');
 return { operationId:doc.operationId, fingerprint:doc.fingerprint!, result };
}
export async function hasOperation(s: Authority, operationId: string): Promise<boolean> {
 return !!(await s.tx.doc(OperationDoc, address(s.head.epoch!, operationId), null)).operationId;
}

export async function graphCommand(s: Authority, command: {kind:'apply';input:GraphApplyInput}|{kind:'transition';input:GraphTaskTransitionInput}, now:()=>number) {
 const receiptDoc=await s.tx.doc(OperationDoc,address(s.head.epoch!,command.input.operationId),null) as unknown as Partial<OperationRecord>;
 const receipt=checkedOperation(receiptDoc,graphControlFingerprint({command:command.kind==='apply'?'apply_graph':'transition',...command.input}));
 if(receipt){
  if(receiptDoc.legacy){
   // Validate an rc.7 receipt against its original committed projection once.
   const oracle=GraphTaskController.recover(await readGraphHistory(s));
   const result=command.kind==='apply'?oracle.applyGraph(command.input):oracle.transition(command.input);
   delete receiptDoc.legacy;return result;
  }
  return {...receipt.result,replayed:true};
 }
 const head:GraphMaterializedHead = {...s.head.graph!,topology:(await graphIndex(s)).topology,projections:dictionary()};
 const generationHistory=s.head.stopRefresh?await readGraphHistory(s):undefined;
 const records = new Map<string, GraphMaterializedTask>();
 const load = async (ids: string[]) => {
  for (const id of ids) {
   if (records.has(id)) continue;
   const record = await taskRecord(s,id);
   if (record) {records.set(id,record.materialized);head.projections[id]=prerequisiteProjection(record.materialized.node!.derived);}
   else {
    const counter = await s.tx.doc(OrdinalDoc,address(s.head.epoch!,id),null);
    const historical=await s.tx.doc(TaskDoc,address(s.head.epoch!,id),null) as unknown as {value?:string};
    records.set(id,{id,attemptsStarted:Number(counter.count??0),attempts:historical.value?JSON.parse(historical.value).materialized?.attempts??[]:[]});
   }
  }
 };
 if (command.kind==='apply') await load([...new Set([...head.topology.map(t=>t.key),...command.input.tasks.map(t=>t.key)])]);
 else await load([command.input.taskId]);
 for (;;) {
  const engine = IncrementalGraphTaskController.restore(head,records,()=>new Date(now()));
  try {
   const result = command.kind==='apply' ? engine.applyGraph(command.input) : engine.transition(command.input);
   if (result.replayed) return result;
   const changes = engine.changes(), next = command.kind==='apply'?engine.materializedHead():undefined;
   const present = next?new Set(next.topology.map(t=>t.key)):undefined;
   for (const id of changes.taskIds) await writeTask(s,engine,id,present?.has(id)??true);
   const prior = changes.revisions.length ? head.topology.map(t=>records.get(t.key)!.node!.definition) : [];
   for (const revision of changes.revisions) await logRevision(s,revision,command.input.operationId,prior);
   for (const event of changes.events) await logEvent(s,event);
   for (const op of changes.receipts) await saveOperation(s,op);
   if(next)await saveGraphHead(s,next);
   else {
    const update=engine.headUpdate();
    s.head.graph!.sequence=update.sequence;
    s.head.graph!.ready=update.ready;s.head.graph!.occupied=update.occupied;
   }
   if(generationHistory){generationHistory.graphRevisions.push(...changes.revisions);generationHistory.events.push(...changes.events);generationHistory.receipts.push(...changes.receipts);}
   await reconcile(s,engine,changes.taskIds,changes.events,now(),generationHistory);
   return result;
  } catch (error) {
   if (!(error instanceof GraphTaskRequired)) throw error;
   const missing = error.ids.filter(id=>!records.has(id));
   if (!missing.length) throw new Error('Established graph authority is corrupt.');
   await load(missing);
  }
 }
}
async function reconcile(s:Authority,engine:IncrementalGraphTaskController,changed:string[],events:GraphControlEvent[],now:number,generationHistory?:GraphControlSnapshot) {
 const team=s.team.current!;
 // Pending Task obligations are bounded by the ready frontier. Alert delivery
 // records are not read during graph reconciliation.
 for(const id of changed){
  const oldKeys=[...(s.head.taskObligations[id]??[])];
  for(const key of oldKeys){const o=await obligation(s,key);if(!o)continue;const record=await taskRecord(s,id),t=record?.card;
   if(!t||t.version!==o.version||t.assignee!==o.recipient||t.status!=='ready')await setObligationState(s,o,'retired');
  }
 }
 const frontier:AuthorityHead['frontier']=dictionary();
 const ready=Object.entries(s.head.graph!.ready).filter(([assignee,ids])=>!s.head.graph!.occupied[assignee]?.length&&ids.length)
  .map(([assignee,ids])=>({assignee,id:[...ids].sort((a,b)=>a.localeCompare(b))[0]}))
  .sort((a,b)=>a.id.localeCompare(b.id));
 for(const {assignee,id} of ready){
  const w=team.workers[assignee];
  if(!w||w.state!=='active')continue;
  const previous=s.head.frontier[assignee];
  if(previous?.id===id&&previous.generation===w.generation&&!changed.includes(id)){frontier[assignee]=plain(previous);continue;}
  const t=(await taskRecord(s,id))!.card;
  frontier[assignee]={id,version:t.version,generation:w.generation};
  const key=`${team.epoch}:task:${id}:${t.version}:g${w.generation}`;
  await addObligation(s,{key,epoch:team.epoch,kind:'task',recipient:w.name,conversationId:w.conversationId,generation:w.generation,taskId:id,version:t.version,state:'pending',acceptedAt:now});
 }
 s.head.frontier=frontier;
 let attempts = [...new Set(events.flatMap(e=>'attemptId' in e&&e.attemptId?[e.attemptId]:[]))]
  .flatMap(id=>{const taskId=events.find(e=>'attemptId' in e&&e.attemptId===id);return taskId ? changed.flatMap(key=>engine.readAttempts(key).filter(a=>a.id===id)) : [];});
 // The reference emits ready Tasks by Task ID, then stop notices by Attempt
 // start sequence. Revision event order does not preserve Attempt chronology.
 if(generationHistory){attempts=GraphTaskController.recover(generationHistory).readAttempts();s.head.stopRefresh=false;}
 else attempts.sort((a,b)=>engine.attemptStartedSequence(a.id)-engine.attemptStartedSequence(b.id));
 for(const attempt of attempts){
  if(attempt.state!=='superseded'&&attempt.state!=='cancelled')continue;
  const w=team.workers[attempt.assignee];if(!w||w.state!=='active')continue;
  const key=`${team.epoch}:stop:${attempt.id}:g${w.generation}`;
  await addObligation(s,{key,epoch:team.epoch,kind:'stop',recipient:w.name,conversationId:w.conversationId,generation:w.generation,taskId:attempt.taskId,state:'pending',acceptedAt:now});
 }
}

/** History materialization for diagnostics, migration checks, and generation reconciliation. */
export async function readGraphHistory(s:Authority,epoch=s.head.epoch!):Promise<GraphControlSnapshot> {
 if(s.migratedHistory?.has(epoch))return plain(s.migratedHistory.get(epoch)!);
 const snapshot:GraphControlSnapshot={schema:'pi-team-bright-graph-control/1',graphRevisions:[],events:[],receipts:[]};
 if(s.head.log===undefined)return snapshot;
 let cursor: Cursor|undefined;const definitions=new Map<string,GraphRevision['tasks'][number]>(),operations=new Set<string>();
 do {
  const page=await s.tx.scanEntries({conversationId:s.head.log,order:'ascending'},500,cursor);
  for(const entry of page.items){const data=entry.data as any;if(data?.epoch!==epoch)continue;
   if(entry.kind==='ptb.graph-revision'){
    for(const id of data.removed)definitions.delete(id);
    for(const task of data.upserts)definitions.set(task.key,task);
    snapshot.graphRevisions.push({sequence:data.sequence,version:data.version,tasks:data.order.map((id:string)=>definitions.get(id)!),...(data.recorded_at?{recorded_at:data.recorded_at}:{})});operations.add(data.operationId);
   }else if(entry.kind==='ptb.graph-event'){snapshot.events.push(data.event);operations.add(data.event.operationId);}
  }
  cursor=page.next;
 }while(cursor);
 for(const id of operations){const receipt=checkedOperation(await s.tx.doc(OperationDoc,address(epoch,id),null) as unknown as OperationRecord);if(receipt)snapshot.receipts.push(receipt);}
 return snapshot;
}

async function migrate(s:Authority) {
 const oldGraph=await s.tx.doc(GraphDoc) as unknown as State['graph'];
 const oldJournal=await s.tx.doc(JournalDoc) as unknown as JournalState;
 const oldOutbox=await s.tx.doc(OutboxDoc) as unknown as State['outbox'];
 const oldReceipts=await s.tx.doc(ReceiptDoc) as unknown as State['receipts'];
 if([oldGraph,oldJournal,oldOutbox,oldReceipts].some(hasLegacyFence))throw new Error('Authority head is missing after authority cutover.');
 const callsToMigrate:Array<[string,Receipt]>=[];
 for(const [id,receipt] of Object.entries(oldReceipts.calls)){
  const task=await s.tx.task(Number(id) as TaskId);
  if(task?.kind==='pi.tool'&&task.state.status==='terminal')continue;
  await callReceipt(s,Number(id) as TaskId,task?.kind==='pi.tool');callsToMigrate.push([id,plain(receipt)]);
 }
 s.head.journal={...plain(oldJournal),events:JSON.stringify(plain(oldJournal.events))};
 if(s.team.current||oldGraph.snapshot||Object.keys(oldGraph.history??{}).length){
  s.head.log=(await s.tx.createConversation({ownership:{kind:'ownerless'}})).id;
  const snapshots={...plain(oldGraph.history??{}),...(oldGraph.snapshot&&oldGraph.epoch?{[oldGraph.epoch]:plain(oldGraph.snapshot)}:{})};
  s.migratedHistory=new Map(Object.entries(snapshots));
  for(const [epoch,snapshot] of Object.entries(snapshots)){
   const oracle=GraphTaskController.recover(snapshot),engine=IncrementalGraphTaskController.fromSnapshot(snapshot);
   if(JSON.stringify(engine.readTasks())!==JSON.stringify(oracle.readTasks())||JSON.stringify(engine.readAttempts())!==JSON.stringify(oracle.readAttempts()))throw new Error('Graph migration differs from the reference controller.');
   s.head.epoch=epoch;s.head.graph=emptyGraph();s.head.taskCount=0;s.head.attention=dictionary();s.records.clear();
   const ids=new Set([...snapshot.graphRevisions.flatMap(r=>r.tasks.map(t=>t.key)),...snapshot.events.flatMap(e=>e.kind==='attempt_started'?[e.taskId]:[])]);
   const current=new Set(engine.materializedHead().topology.map(t=>t.key));
   for(const id of ids)await writeTask(s,engine,id,current.has(id));
   let prior:GraphRevision['tasks']=[];
   for(const entry of [...snapshot.graphRevisions,...snapshot.events].sort((a,b)=>a.sequence-b.sequence)){
    if('tasks' in entry){const op=snapshot.receipts.find(r=>r.result.kind==='graph_applied'&&r.result.graphVersion===entry.version)?.operationId;if(!op)throw new Error('Graph migration revision has no receipt.');await logRevision(s,entry,op,prior);prior=entry.tasks;}
    else await logEvent(s,entry);
   }
   for(const receipt of snapshot.receipts)await saveOperation(s,receipt,true);
   await saveGraphHead(s,engine.materializedHead());
  }
  s.head.epoch=oldGraph.epoch;
  if(!oldGraph.snapshot)delete s.head.graph;
 }
 for(const value of Object.values(oldOutbox.alerts))await addAlert(s,plain(value));
 for(const value of Object.values(oldOutbox.obligations))await addObligation(s,plain(value));
 for(const [id,receipt] of callsToMigrate)await saveCall(s,Number(id) as TaskId,receipt);
 s.head.workerBindings=Object.fromEntries(Object.values(s.team.current?.workers??{}).map(w=>[String(w.conversationId),w.name]));
 s.head.stopRefresh=Object.values(s.team.current?.workers??{}).some(w=>w.generation>1);
 s.head.initialized=true;
}

/** Legacy-shaped, full inspection for diagnostics and existing recovery tests. */
export async function inspectAuthority(tx:Tx):Promise<State> {
 const s=await openAuthority(tx),graph:State['graph']={...(s.head.epoch?{epoch:s.head.epoch}:{})};
 if(s.head.graph)graph.snapshot=await readGraphHistory(s);
 const history:Record<string,GraphControlSnapshot>={};
 for(const team of s.team.history)history[team.epoch]=await readGraphHistory(s,team.epoch);
 if(Object.keys(history).length)graph.history=history;
 const outbox:State['outbox']={alerts:{},obligations:{}};
 const alerts:Alert[]=[];let alert=s.head.alertTail;
 while(alert){const doc=await tx.doc(AlertDoc,alert,null) as unknown as Linked<Alert>;if(doc.value)alerts.push(plain(doc.value));alert=doc.previous;}
 for(const value of alerts.reverse())outbox.alerts[value.id]=value;
 const obligations:Obligation[]=[];let key=s.head.obligationTail;
 while(key){const doc=await tx.doc(ObligationDoc,key,null) as unknown as Linked<Obligation>;if(doc.value)obligations.push(plain(doc.value));key=doc.previous;}
 for(const value of obligations.reverse())outbox.obligations[value.key]=value;
 const receipts:State['receipts']={calls:{}};let call=s.head.callTail;
 while(call){const doc=await tx.doc(DirectCallDoc,call,null) as unknown as StoredCall;if(doc.fingerprint)receipts.calls[call]={fingerprint:doc.fingerprint,...(doc.result?{result:JSON.parse(doc.result)}:{stops:doc.stops,epoch:doc.epoch})};call=doc.previous;}
 let events=await recentEvents(s);
 const journal=new Proxy(s.journal,{get(target,key,receiver){return key==='events'?events:Reflect.get(target,key,receiver);},set(target,key,value,receiver){if(key==='events')events=value;return Reflect.set(target,key,value,receiver);}}) as JournalState;
 const result={team:s.team,graph:new Proxy(graph,{deleteProperty(target,key){if(key==='snapshot'){delete s.head.graph;flushAuthority(s);}return Reflect.deleteProperty(target,key);}}),journal,outbox,receipts};
 Object.defineProperty(result,'flush',{value:()=>flushAuthority(s)});return result;
}
