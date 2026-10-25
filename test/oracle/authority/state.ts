import { defineDoc, type ConversationId, type TaskId, type SubmissionId, type EntryId, type JsonObject, type Tx } from '@earendil-works/pi-durable';
import type { GraphControlSnapshot } from './graph-control.ts';

// Session authority changes only in durable transactions.
export interface Binding { conversationId: ConversationId; anchorId: TaskId }
export interface ModelRole { name: string; use: string; model: { provider: string; modelId: string }; thinking?: string }
/** The roles a lead can select, with the role ensure_worker uses when the call names none. */
export interface ModelRoleCatalog { modelRoles: ModelRole[]; defaultModelRole?: string }
export interface Worker extends Binding { createdAt?:number; name: string; scope: string; generation: number; state: 'active' | 'stopping' | 'stopped'; role: ModelRole }
export interface Team { createdAt?:number; epoch: string; name: string; purpose: string; lead: ConversationId; lifecycle: 'active' | 'stopped'; workers: Record<string, Worker>; shutdownPending?: boolean; notifierId?: TaskId }
export interface StopIntent { epoch:string; worker?:string; binding?:Binding; generation?:number; tasks:TaskId[]; conversations:ConversationId[]; confirmed:boolean; error?:string }
export interface TeamState { current?: Team; history: Team[]; stops: Record<string, StopIntent> }
export interface GraphState { epoch?: string; snapshot?: GraphControlSnapshot; history?: Record<string, GraphControlSnapshot> }
export interface Alert { interrupt?:true; id: string; from: string; kind: string; text: string; task_id?: string; task_version?: string; receivedAt: string; recipients: { name: string; conversationId: ConversationId; generation: number }[] }
export interface Obligation { interrupt?:true; interruptRun?:TaskId; interruption?:'interrupted'|'ended'|'failed'; acceptedAt?:number; submittedAt?:number; settledAt?:number; key: string; epoch: string; kind: 'task' | 'stop' | 'alert'; recipient: string; conversationId: ConversationId; generation: number; taskId?: string; version?: string; alertId?: string; state: 'pending' | 'retired' | 'settled'; deliveryTaskId?:TaskId; submissionId?:SubmissionId; outcome?:string }
export interface OutboxState { alerts: Record<string, Alert>; obligations: Record<string, Obligation> }
export interface Event { seq: number; epoch: string; actor: string; kind: string; taskId?: string; taskIds?:string[]; worker?: string; at?:string; text?:string; entryId?:EntryId }
export interface JournalState { seq: number; events: Event[]; reservation?: { epoch: string; call: string; through: number }; acknowledged: number }
export type Result = Record<string, unknown> & { kind: string };
export interface Receipt { fingerprint: string; result?: Result; stops?: string[]; epoch?: string }
export interface ReceiptState { calls: Record<string, Receipt> }
const doc = (kind: string, initial: () => JsonObject) => defineDoc<JsonObject>({kind, version: 1, scope: 'session', checkpointWhen: () => false, initial});
export const TeamDoc = doc('ptb.team', () => ({history: [], stops: {}}));
export const GraphDoc = doc('ptb.graph', () => ({}));
export const JournalDoc = defineDoc<JsonObject>({kind:'ptb.journal',version:1,scope:'session',initial:()=>({seq:0,events:[],acknowledged:0}),checkpointWhen:(_value,_ops,info)=>info.deltasSinceBase>=31});
export const OutboxDoc = doc('ptb.outbox', () => ({alerts: {}, obligations: {}}));
export const ReceiptDoc = doc('ptb.receipts', () => ({calls: {}}));
export async function open(tx: Tx) {
 return {
  team: await tx.doc(TeamDoc) as unknown as TeamState,
  graph: await tx.doc(GraphDoc) as unknown as GraphState,
  journal: await tx.doc(JournalDoc) as unknown as JournalState,
  outbox: await tx.doc(OutboxDoc) as unknown as OutboxState,
  receipts: await tx.doc(ReceiptDoc) as unknown as ReceiptState,
 };
}
export type State = Awaited<ReturnType<typeof open>>;
export const plain = <T>(value: T): T => JSON.parse(JSON.stringify(value));
export const JOURNAL_RESIDENT_LIMIT = 100;
/** History is append-only storage; the document contains a bounded recent projection. */
export async function event(tx: Tx, s: State, actor: string, kind: string, extra: Pick<Event,'taskId'|'taskIds'|'worker'|'at'|'text'> = {}) {
 const value={seq: ++s.journal.seq, epoch: s.team.current!.epoch, actor, kind, ...extra};
 const entry=await tx.appendEntry(s.team.current!.lead,{kind:'ptb.event',data:value});
 s.journal.events.push({...value,entryId:entry.id});
 if(s.journal.events.length>JOURNAL_RESIDENT_LIMIT)s.journal.events.splice(0,s.journal.events.length-JOURNAL_RESIDENT_LIMIT);
}
