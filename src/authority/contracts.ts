import { readFileSync } from 'node:fs';
import { Compile, type Validator } from 'typebox/compile';
import type { TSchema } from 'typebox';
import type { Result } from './state.ts';

export const TOOL_NAMES = ['team_create', 'team_sync', 'ensure_worker', 'task_graph_apply', 'task_read', 'task_update', 'worker_stop', 'team_shutdown', 'alert_send'] as const;
export type ToolName = typeof TOOL_NAMES[number];
export type Role = 'lead' | 'worker';
/** The label and description Pi Team Bright registers for one role's tool. */
export interface ToolPresentation { label: string; description: string }
export interface Contract { lead: Record<ToolName, TSchema>; worker: Partial<Record<ToolName, TSchema>>; results: Record<ToolName, TSchema>; presentation: Record<Role, Partial<Record<ToolName, ToolPresentation>>> }
// Generated from the pinned executable rc.7 contract, not the stale agent surface.
const source=readFileSync(new URL('./rc7-contract.json', import.meta.url), 'utf8');
const patchedContracts=new Map<number,string>();
const validators=new Map<string,Validator>();
export function checkSchema(schema:TSchema,value:unknown):boolean {
 const key=JSON.stringify(schema,(_key,value)=>value&&typeof value==='object'&&!Array.isArray(value)
  ?Object.fromEntries(Object.entries(value).sort(([a],[b])=>a<b?-1:a>b?1:0)):value);
 let validator=validators.get(key);
 if(!validator){validator=Compile(JSON.parse(key));validators.set(key,validator);}
 return validator.Check(value);
}
export function contracts(goalMaxLength = 1600): Contract {
 if (!Number.isSafeInteger(goalMaxLength) || goalMaxLength < 1) throw new Error('goalMaxLength must be a positive safe integer');
 const cached=patchedContracts.get(goalMaxLength);
 if(cached!==undefined)return JSON.parse(cached) as Contract;
 const copy:Contract=JSON.parse(source);
 function patch(value: unknown): void {
  if (!value || typeof value !== 'object') return;
  const node = value as Record<string, unknown>;
  if (node.properties && typeof node.properties === 'object') {
   const goal = (node.properties as Record<string, unknown>).goal;
   if (goal && typeof goal === 'object') (goal as Record<string, unknown>).maxLength = goalMaxLength;
  }
  for (const child of Object.values(node)) patch(child);
 }
 patch(copy);
 patchedContracts.set(goalMaxLength,JSON.stringify(copy));
 return copy;
}
export function checkResult(contract: Contract, name: ToolName, value: Result): Result {
 if (!checkSchema(contract.results[name], value)) throw new Error(`PTB ${name} produced a result outside the rc.7 contract: ${JSON.stringify(value)}`);
 return value;
}
