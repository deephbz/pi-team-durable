import { readFileSync } from 'node:fs';
import { Check } from 'typebox/value';
import type { TSchema } from 'typebox';
import type { Result } from './state.ts';

export const TOOL_NAMES = ['team_create', 'team_sync', 'ensure_worker', 'task_graph_apply', 'task_read', 'task_update', 'worker_stop', 'team_shutdown', 'alert_send'] as const;
export type ToolName = typeof TOOL_NAMES[number];
export type Role = 'lead' | 'worker';
interface Contract { lead: Record<ToolName, TSchema>; worker: Partial<Record<ToolName, TSchema>>; results: Record<ToolName, TSchema> }
// Generated from the pinned executable rc.7 contract, not the stale agent surface.
const source: Contract = JSON.parse(readFileSync(new URL('./rc7-contract.json', import.meta.url), 'utf8'));
export function contracts(goalMaxLength = 1600): Contract {
 if (!Number.isSafeInteger(goalMaxLength) || goalMaxLength < 1) throw new Error('goalMaxLength must be a positive safe integer');
 const copy: Contract = structuredClone(source);
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
 return copy;
}
export function checkResult(contract: Contract, name: ToolName, value: Result): Result {
 if (!Check(contract.results[name], value)) throw new Error(`PTB ${name} produced a result outside the rc.7 contract: ${JSON.stringify(value)}`);
 return value;
}
