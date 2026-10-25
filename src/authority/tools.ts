import type { TSchema } from 'typebox';
import type { Context } from '@earendil-works/chord/context';
import { defineTool, type ToolExecutionApi, type Tx } from '@earendil-works/pi-durable';
import { contracts, checkSchema, checkResult, TOOL_NAMES, type Role, type ToolName } from './contracts.ts';
import { GraphControlRefusal, type GraphApplyResult, type GraphTransitionResult, type GraphTaskDefinitionInput, type GraphTaskTransition, type GraphVersionRef, type GraphTaskView } from './graph-control.ts';
import type { TaskVersionRef } from './task-version-ref.ts';
import { projectGraphTaskCards } from './graph-card.ts';
import { snapshotResult } from './projection.ts';
import type { ConversationId } from '@earendil-works/pi-durable';
import { plain, event, type Binding, type ModelRole, type ModelRoleCatalog, type Result, type StopIntent, type Worker } from './state.ts';
import { openAuthority as open, startEpoch, taskCards, unfinished, graphCommand, hasOperation, addAlert, addObligation, callReceipt, saveCall, allObligations, setObligationState, type Authority as State } from './store.ts';

export type CoreApi = Pick<ToolExecutionApi, 'taskId' | 'conversationId' | 'commit' | 'now'> & Partial<Pick<ToolExecutionApi,'callId'>>;
export interface CoreOptions {
 modelRoles: ModelRole[];
 defaultModelRole?: string;
 goalMaxLength?: number;
 // The runtime creates the live anchor and conversation in this transaction.
 createWorker?: (tx: Tx, role: ModelRole) => Promise<Binding>;
 resolveModelRole?: (tx:Tx,role:ModelRole)=>Promise<ModelRole>;
 // The lead's catalog, read in the command transaction. Without it every lead gets modelRoles and defaultModelRole.
 catalog?: (tx:Tx,lead:ConversationId)=>Promise<ModelRoleCatalog>;
 // Must be idempotent for this exact binding. No model-selected arbitrary target.
 stopWorker?: (binding: Binding, context: Context) => Promise<boolean>;
 // One call for every unconfirmed stop of a command: the port marks them all before it settles any. One result per stop.
 settleStops?: (stops:{key:string;intent:StopIntent}[],context:Context,api:CoreApi)=>Promise<boolean[]>;
 // Register mechanics and their IDs in the same authority transaction.
 afterCommand?: (tx:Tx,s:State,name:ToolName,result:Result|{stops:string[];epoch:string},api:CoreApi)=>Promise<void>;
 sync?: (tx:Tx,s:State,view:Args['view'],api:CoreApi,snapshot:()=>Promise<Result>)=>Promise<Result>;
}
interface Args {
 name?: string; purpose?: string; scope?: string; model_role?: string; worker?: string;
 view?: 'snapshot' | 'updates'; task_id?: string; task_ids?: string[];
 operation_id?: string; expected_graph_version?: GraphVersionRef; expected_version?: TaskVersionRef;
 transition?: GraphTaskTransition; current_context?: string; evidence?: string;
 tasks?: {key: string; title: string; goal: string; assignee: string; needs?: string[]; on_goal_failed?: {target: string; max_traversals: number}}[];
 to?: string; kind?: string; text?: string; task_version?: string; interrupt?:boolean;
}
const LEAD = 'team-lead';
interface ParameterSchema extends TSchema { properties?:Record<string,ParameterSchema>; items?:ParameterSchema }
/** The input a role tool acts on. Pi's role adapters forward declared fields, including nested Task definitions. */
export function declaredParameters(schema:ParameterSchema,value:unknown):unknown {
 const {items,properties}=schema;
 if(Array.isArray(value)&&items)return value.map(item=>declaredParameters(items,item));
 if(value!==null&&typeof value==='object'&&!Array.isArray(value)&&properties)
  return Object.fromEntries(Object.entries(value).filter(([key])=>Object.hasOwn(properties,key)).map(([key,item])=>[key,declaredParameters(properties[key],item)]));
 return value;
}
class Declined extends Error { readonly result:Result; constructor(result: Result) { super(String(result.message ?? result.reason)); this.result=result; } }
const decline = (result: Result): never => { throw new Declined(result); };
function unavailable(name: ToolName, args: Args, reason: string, message: string): never {
 return decline({kind:'unavailable',reason,message,
  ...(['task_graph_apply','task_update'].includes(name) ? {operation_id:args.operation_id} : {}),
  ...(name === 'task_update' ? {task_id:args.task_id} : {}),
 });
}
function failureReason(name: ToolName) {
 return ['task_read','task_update','task_graph_apply'].includes(name) ? 'task_authority_unavailable' : name === 'team_sync' ? 'team_state_unavailable' : 'team_authority_unavailable';
}
function caller(s: State, api: CoreApi, role: Role, name: ToolName, args: Args): string {
 const team = s.team.current;
 if (!team || (team.lifecycle !== 'active' && !(name==='team_shutdown' && role==='lead' && team.lead===api.conversationId))) return unavailable(name,args,'no_active_team','No active Team.');
 if (role === 'lead' && team.lead === api.conversationId) return LEAD;
 const workerName=s.head.workerBindings[String(api.conversationId)];
 const candidate=workerName?team.workers[workerName]:undefined;
 const worker=candidate?.state==='active'&&candidate.conversationId===api.conversationId?candidate:undefined;
 if (role === 'worker' && worker) return worker.name;
 return unavailable(name,args,failureReason(name),'Caller is not the exact current Team binding.');
}
function controller(s: State, _api: CoreApi, name: ToolName, args: Args): void {
 if (!s.head.graph || s.head.epoch !== s.team.current?.epoch) return unavailable(name,args,failureReason(name),'Established graph authority is missing.');
 if (!Number.isSafeInteger(s.head.graph.sequence) || !s.head.graph.ready || !s.head.graph.occupied) return unavailable(name,args,failureReason(name),'Established graph authority is corrupt.');
}
function modelTask(t: GraphTaskView) {
 const [card]=projectGraphTaskCards({readTasks:()=>[t],readAttempts:()=>[],selectReadyFrontier:()=>[]});
 return {id:t.id,status:t.state.kind,assignee:t.assignee,needs:t.needs,state:card.state,attempts_started:t.attemptsStarted,version:t.version,
  ...('attemptId' in t.state ? {current_attempt_id:t.state.attemptId} : {}), ...(t.acceptedAttemptId ? {accepted_attempt_id:t.acceptedAttemptId} : {})};
}
/** The catalog resolver that options select: the host's per-lead port, else the static list. */
export function catalogOf(options:Pick<CoreOptions,'modelRoles'|'defaultModelRole'|'catalog'>):(tx:Tx,lead:ConversationId)=>Promise<ModelRoleCatalog> {
 return async(tx,lead)=>options.catalog?options.catalog(tx,lead):{modelRoles:options.modelRoles,...(options.defaultModelRole?{defaultModelRole:options.defaultModelRole}:{})};
}

/** Authority commands commit intent and replay receipts; injected ports own effects. */
export function createAuthorityTools(options: CoreOptions) {
 const contract=contracts(options.goalMaxLength);
 const rolesOf=catalogOf(options);
 const catalog=(c:ModelRoleCatalog)=>({model_roles:c.modelRoles.map(r=>({name:r.name,use:r.use})),...(c.defaultModelRole?{default_model_role:c.defaultModelRole}:{})});
 async function command(name:ToolName, role:Role, a:Args, api:CoreApi, tx:Tx, s:State): Promise<Result | {stops:string[]; epoch:string}> {
  if(name==='team_create') {
   if(role!=='lead') return unavailable(name,a,'team_authority_unavailable','Worker cannot create a Team.');
   if(s.team.current?.lifecycle==='active') return decline({kind:'refused',reason:'active_team_exists',message:'An active Team already owns this store.'});
   if(!/^[A-Za-z0-9_-]{1,64}$/.test(a.name!)) return decline({kind:'refused',reason:'name_unavailable',message:'Invalid Team name.'});
   if(s.team.current) {
    if(s.team.current.lead!==api.conversationId) return unavailable(name,a,'team_authority_unavailable','Only the established lead can create the next Team.');
    s.team.history.push(plain(s.team.current));
   }
   s.team.current={epoch:`team:${api.taskId}`,name:a.name!,purpose:a.purpose!,lead:api.conversationId,lifecycle:'active',workers:{},createdAt:api.now()};
   await startEpoch(s,s.team.current.epoch);
   delete s.journal.reservation;
   s.journal.acknowledged=0;
   await event(tx,s,LEAD,'team_created',{at:new Date(api.now()).toISOString()});
   return {kind:'team_created',team:{name:a.name,lifecycle:'active'},...catalog(await rolesOf(tx,api.conversationId))};
  }
  const actor=caller(s,api,role,name,a), team=s.team.current!;
  if(name==='ensure_worker') {
   if(team.shutdownPending) return unavailable(name,a,'carrier_unavailable','Team shutdown is pending.');
   if(a.name===LEAD || !/^[A-Za-z0-9_-]{1,64}$/.test(a.name!)) return unavailable(name,a,'team_authority_unavailable','Invalid or reserved Worker name.');
   const prior=Object.hasOwn(team.workers,a.name!)?team.workers[a.name!]:undefined;
   const existing=prior?{name:prior.name,scope:prior.scope,carrier:prior.state==='active'?'connected':'absent',model_role:prior.role.name}:undefined;
   if(prior && prior.scope!==a.scope) return decline({kind:'refused',reason:'name_scope_conflict',existing_worker:existing});
   if(prior && a.model_role && prior.role.name!==a.model_role) return decline({kind:'refused',reason:'model_conflict',existing_worker:existing});
   if(prior?.state==='stopping') return unavailable(name,a,'carrier_unavailable','The exact Worker stop is pending.');
   if(prior?.state==='active') return {kind:'worker_ensured',effect:'reused',worker:{name:prior.name,carrier:'connected',model_role:prior.role.name}};
   const roles=prior?undefined:await rolesOf(tx,team.lead);
   let selected=prior?.role ?? roles!.modelRoles.find(r=>r.name===(a.model_role??roles!.defaultModelRole));
   if(!selected) return decline({kind:'refused',reason:'invalid_model_role',valid_model_roles:catalog(roles!).model_roles,message:'Select a configured model role.'});
   if(!options.createWorker) return unavailable(name,a,'carrier_unavailable','Worker anchor adapter is not installed.');
   if(options.resolveModelRole)selected=await options.resolveModelRole(tx,plain(selected));
   const binding=await options.createWorker(tx,plain(selected));
   team.workers={...plain(team.workers),[a.name!]:{...binding,name:a.name!,scope:a.scope!,generation:(prior?.generation??0)+1,state:'active',role:plain(selected),createdAt:api.now()}};
   if(prior){delete s.head.workerBindings[String(prior.conversationId)];s.head.stopRefresh=true;}
   s.head.workerBindings[String(binding.conversationId)]=a.name!;
   await event(tx,s,actor,'worker_created',{worker:a.name,at:new Date(api.now()).toISOString()});
   return {kind:'worker_ensured',effect:prior?'reconnected':'created',worker:{name:a.name,carrier:'connected',model_role:selected.name}};
  }
  controller(s,api,name,a);
  if(name==='team_shutdown'&&team.lifecycle==='stopped')return {kind:'team_shutdown',lifecycle:'stopped',stopped_workers:Object.values(team.workers).filter(w=>w.state==='stopped').map(w=>w.name),unfinished_task_ids:await unfinished(s)};
  if(name==='task_read') {
   const cards=new Map((await taskCards(s,role==='worker'?[a.task_id!]:a.task_ids!)).map(t=>[t.id,t]));
   if(role==='worker') return cards.has(a.task_id!)?{kind:'found',task:cards.get(a.task_id!)}:{kind:'missing',task_id:a.task_id,reason:'task_not_found'};
   return {kind:'task_read_batch',outcomes:a.task_ids!.map((id,input_index)=>cards.has(id)?{kind:'found',input_index,task_id:id,task:cards.get(id)}:{kind:'missing',input_index,task_id:id,reason:'task_not_found'})};
  }
  if(name==='task_graph_apply') {
   // The controller owns replay identity. Check current assignees only for a new operation.
   const already=await hasOperation(s,a.operation_id!);
   if(!already && a.tasks!.some(t=>!Object.hasOwn(team.workers,t.assignee)||team.workers[t.assignee].state!=='active')) return decline({kind:'refused',operation_id:a.operation_id,reason:'worker_unavailable',message:'Every assignee must be a current Worker.'});
   const tasks:GraphTaskDefinitionInput[]=a.tasks!.map(t=>({key:t.key,title:t.title,goal:t.goal,assignee:t.assignee,needs:t.needs,...(t.on_goal_failed?{onGoalFailed:{target:t.on_goal_failed.target,maxTraversals:t.on_goal_failed.max_traversals}}:{})}));
   const r=await graphCommand(s,{kind:'apply',input:{operationId:a.operation_id!,expectedGraphVersion:a.expected_graph_version,tasks}},api.now) as GraphApplyResult & {replayed:boolean};
   if(!r.replayed)await event(tx,s,actor,'graph_applied',{taskIds:r.tasks.map(t=>t.id),at:new Date(api.now()).toISOString()});
   return {kind:'task_graph_applied',operation_id:r.operationId,graph_version:r.graphVersion,replayed:r.replayed,tasks_by_key:Object.fromEntries(r.tasks.map(t=>[t.id,modelTask(t)])),ready_task_ids:r.readyTaskIds};
  }
  if(name==='task_update') {
   if(role==='worker' && a.transition==='cancel') return decline({kind:'refused',task_id:a.task_id,operation_id:a.operation_id,reason:'worker_mismatch',message:'Only the lead cancels Tasks.'});
   const r=await graphCommand(s,{kind:'transition',input:{taskId:a.task_id!,operationId:a.operation_id!,expectedVersion:a.expected_version!,transition:a.transition,currentContext:a.current_context,evidence:a.evidence,worker:role==='worker'?actor:a.transition&&a.transition!=='cancel'?LEAD:undefined}},api.now) as GraphTransitionResult & {replayed:boolean};
   if(!r.replayed)await event(tx,s,actor,'task_transitioned',{taskId:r.task.id,at:new Date(api.now()).toISOString(),text:a.evidence??a.current_context??r.transition});
   return {kind:'updated',operation_id:r.operationId,replayed:r.replayed,transition:r.transition,task:{id:r.task.id,status:r.task.state.kind,assignee:r.task.assignee,version:r.task.version},ready_task_ids:r.readyTaskIds,...(r.failureTraversal?{failure_traversal:{source_task_id:r.failureTraversal.sourceTaskId,target_task_id:r.failureTraversal.targetTaskId,traversal:r.failureTraversal.traversal}}:{})};
  }
  if(name==='alert_send') {
   const to=role==='worker'?LEAD:a.to!,interrupt=role==='lead'&&a.interrupt===true;
   if(!a.text?.trim() || a.task_version&&!a.task_id || (to==='*')!==(a.kind==='announcement')) return decline({kind:'refused',reason:'invalid_fanout',message:'Invalid Alert text, Task reference or announcement target.'});
   const recipients=to===LEAD?[{name:LEAD,conversationId:team.lead,generation:0}]:Object.values(team.workers).filter(w=>w.state==='active'&&(to==='*'||w.name===to)).map(w=>({name:w.name,conversationId:w.conversationId,generation:w.generation}));
   if(!recipients.length) return decline({kind:'refused',reason:to==='*'?'no_eligible_recipients':'recipient_not_current',message:'No current recipient.'});
   const id=`${team.epoch}:alert:${api.taskId}`;
   await addAlert(s,plain({id,from:actor,kind:a.kind!,text:a.text.trim(),...(interrupt?{interrupt:true as const}:{}),task_id:a.task_id,task_version:a.task_version,receivedAt:new Date(api.now()).toISOString(),recipients}));
   for(const recipient of recipients){
    const key=`${id}:${recipient.name}:g${recipient.generation}`;
    await addObligation(s,{key,epoch:team.epoch,kind:'alert',recipient:recipient.name,conversationId:recipient.conversationId,generation:recipient.generation,alertId:id,state:'pending',acceptedAt:api.now(),...(interrupt?{interrupt:true as const,interruptAt:'delivery' as const}:{})});
   }
   await event(tx,s,actor,'alert_accepted',{at:new Date(api.now()).toISOString()});
   return {kind:'alert_sent',accepted_recipients:recipients.map(r=>r.name),failed_recipients:[],...(a.task_id?{task_id:a.task_id}:{}),...(a.task_version?{task_version:a.task_version}:{})};
  }
  if(name==='team_sync') {
   // Never consume a page on a tool call alone.
   if(!options.sync&&s.journal.reservation) return {kind:'refused',reason:'observation_in_progress',message:'An observation awaits presentation acknowledgement.',state_changed:false,observation_advanced:false};
   if(!options.sync)s.journal.reservation={epoch:team.epoch,call:String(api.taskId),through:s.journal.seq};
   const roles=await rolesOf(tx,team.lead),snapshot=()=>snapshotResult(s,roles.modelRoles,roles.defaultModelRole);
   return options.sync?options.sync(tx,s,a.view,api,snapshot):snapshot();
  }
  // Lifecycle effects happen after the committed stop intent, outside this transaction.
  let workers:Worker[];
  if(name==='worker_stop') {
   if(a.worker===LEAD) return decline({kind:'refused',reason:'leader_reserved',worker:a.worker});
   const worker=Object.hasOwn(team.workers,a.worker!)?team.workers[a.worker!]:undefined;
   if(!worker) return decline({kind:'refused',reason:'worker_not_found',worker:a.worker});
   const held=await unfinished(s,worker.name);
   if(held.length) return decline({kind:'refused',reason:'nonterminal_tasks_assigned',worker:worker.name,guarding_task_ids:held});
   if(worker.state==='stopped') return {kind:'worker_stopped',worker:worker.name};
   workers=[worker];
  } else workers=Object.values(team.workers).filter(w=>w.state!=='stopped');
  if(workers.length&&!options.stopWorker&&!options.settleStops) return unavailable(name,a,name==='worker_stop'?'carrier_unavailable':'team_authority_unavailable','Anchor stop adapter is not installed.');
  const stops=workers.map(w=>{const key=`${team.epoch}:stop:${w.name}:g${w.generation}`;w.state='stopping';s.team.stops[key]??={epoch:team.epoch,worker:w.name,binding:{conversationId:w.conversationId,anchorId:w.anchorId},generation:w.generation,tasks:[w.anchorId],conversations:[w.conversationId],confirmed:false};return key;});
  for(const obligation of await allObligations(s)) if(obligation.epoch===team.epoch&&workers.some(w=>w.name===obligation.recipient)) await setObligationState(s,obligation,'retired');
  if(name==='team_shutdown')team.shutdownPending=true;
  return {stops,epoch:team.epoch};
 }
 async function execute(name:ToolName,role:Role,input:unknown,api:CoreApi,context:Context):Promise<Result> {
  const schema=contract[role][name];
  // Earlier receipts hashed raw arguments. Their exact input must still replay without executing again.
  const rawFingerprint=JSON.stringify({name,role,input,conversation:api.conversationId});
  if(schema)input=declaredParameters(schema,input);
  if(!schema||!checkSchema(schema,input)) throw new Error(`Invalid ${role} ${name} parameters`);
  const a=input as Args;
  const fingerprint=JSON.stringify({name,role,input,conversation:api.conversationId});
  try {
   const result=await api.commit(async tx=>{
    const native=(await tx.task(api.taskId))?.kind==='pi.tool';
    const s=await open(tx), receipt=await callReceipt(s,api.taskId,native),saved=receipt.fingerprint?receipt:undefined;s.deferFlush=true;
    if(saved){if(saved.fingerprint!==fingerprint&&saved.fingerprint!==rawFingerprint)throw new Error('Durable tool identity reused with changed input');return saved.result?(JSON.parse(saved.result) as Result):plain({stops:saved.stops!,epoch:saved.epoch!});}
    const r=await command(name,role,a,api,tx,s);
    if(name!=='task_read')await options.afterCommand?.(tx,s,name,r,api);
    if('kind' in r) {
     const clean=plain(checkResult(contract,name,r));
     if(name!=='task_read')await saveCall(s,api.taskId,{fingerprint,result:clean});
     return clean;
    }
    await saveCall(s,api.taskId,{fingerprint,stops:r.stops,epoch:r.epoch});
    return plain(r);
   },context);
   if('kind' in result) return result;
   const pending=(await api.commit(async tx=>{const s=await open(tx);return result.stops.map(key=>({key,intent:plain(s.team.stops[key])}));},context)).filter(p=>!p.intent.confirmed);
   const settled=options.settleStops?await options.settleStops(pending,context,api):await Promise.all(pending.map(p=>p.intent.binding?options.stopWorker!(p.intent.binding,context):false));
   for(const [i,{key:stop,intent}] of pending.entries()){
    if(settled[i])await api.commit(async tx=>{
     const s=await open(tx), w=intent.worker?s.team.current?.workers[intent.worker]:undefined;
     if(s.team.current?.epoch!==intent.epoch||(intent.binding&&(!w||w.generation!==intent.generation||w.anchorId!==intent.binding.anchorId)))throw new Error('Stop binding changed');
     if(!s.team.stops[stop].confirmed){
      s.team.stops[stop].confirmed=true;
      if(w){w.state='stopped';await event(tx,s,LEAD,'worker_stopped',{worker:w.name,at:new Date(api.now()).toISOString()});}
     }
    },context);
   }
   return await api.commit(async tx=>{
    const s=await open(tx), team=s.team.current!;await callReceipt(s,api.taskId);
    if(team.epoch!==result.epoch)throw new Error('Team epoch changed during stop');
    const stopped=result.stops.filter(id=>s.team.stops[id].confirmed&&s.team.stops[id].worker).map(id=>s.team.stops[id].worker),failed=result.stops.filter(id=>!s.team.stops[id].confirmed).map(id=>s.team.stops[id].worker??LEAD);
    let r:Result;
    if(name==='worker_stop')r=failed.length?{kind:'refused',reason:'stop_not_confirmed',worker:a.worker}:{kind:'worker_stopped',worker:a.worker};
    else {
     controller(s,api,name,a);const remaining=await unfinished(s);
     if(!failed.length&&team.lifecycle!=='stopped'){team.lifecycle='stopped';await event(tx,s,LEAD,'team_stopped',{at:new Date(api.now()).toISOString()});}
     r=failed.length?{kind:'partial',lifecycle:'active',stopped_workers:stopped,failed_workers:failed,unfinished_task_ids:remaining,recovery:{action:'retry_team_shutdown'}}:{kind:'team_shutdown',lifecycle:'stopped',stopped_workers:stopped,unfinished_task_ids:remaining};
    }
    const clean=plain(checkResult(contract,name,r));await saveCall(s,api.taskId,{fingerprint,result:clean});return clean;
   },context);
  } catch(error) {
   let refusal: Result;
   if(error instanceof Declined) refusal=plain(error.result);
   else if(error instanceof GraphControlRefusal){
    const reason=error.code==='task_version_conflict'?'version_conflict':error.code;
    refusal={kind:'refused',operation_id:a.operation_id,...(name==='task_update'?{task_id:a.task_id}:{}),reason,message:error.message};
   } else throw error;
   const clean=checkResult(contract,name,refusal);
   // The failed transaction rolls back all mutations. Persist its model result separately.
   if(name!=='task_read') await api.commit(async tx=>{
    const s=await open(tx);
    await saveCall(s,api.taskId,{fingerprint,result:clean});
   },context);
   return clean;
  }
 }
 function tools(role:Role) {
  return TOOL_NAMES.filter(name=>contract[role][name]).map(name=>{
   const presentation=contract.presentation[role][name];
   if(!presentation)throw new Error(`The rc.7 contract has no ${role} ${name} label and description`);
   // pi-durable registers no label. It stays on the tool for hosts that show one, as Pi does.
   return {...defineTool({
   name,description:presentation.description,parameters:contract[role][name]!,replay:'safe',
   execute:async(args,api,context)=>{
    const result=await execute(name,role,args,api,context);
    const pending=options.settleStops&&(result.kind==='partial'||result.reason==='stop_not_confirmed');
    const names=result.kind==='partial'?(result.failed_workers as string[]).map(n=>n===LEAD?'Team delivery and notification tasks':n).join(', '):String(result.worker);
    const output=[{type:'text' as const,text:JSON.stringify(result)}];
    if(pending)output.push({type:'text',text:`Still stopping: ${names}. The stop is recorded and no new work starts. Settlement continues in the background. A later ${name} confirms completion.`});
    return {output};
   },
   }),label:presentation.label};
  });
 }
 return {lead:tools('lead'),worker:tools('worker'),execute,contract};
}
