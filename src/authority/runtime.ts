import type { Context } from '@earendil-works/chord/context';
import { configure,type Harness,type Extension } from '@earendil-works/pi-durable';
import { catalogOf,createAuthorityTools,type CoreApi,type CoreOptions } from './tools.ts';
import { createAnchoredConversation,WorkerAnchor } from './anchor.ts';
import type { AbortRun } from './interrupt.ts';
import { createDeliver,scheduleDeliveries } from './delivery.ts';
import { createNotifier } from './notifier.ts';
import { openObservations,startObservationEpoch,observationExtension,type ObservationPortResolver } from './observation.ts';
import { sync } from './sync.ts';
import { event,type StopIntent } from './state.ts';
import { openAuthority as open,flushAuthority,allObligations,setObligationState,type Authority as State } from './store.ts';

// Structural host port. The package never imports node policy or owns a startup reconciler.
export interface StopPlan { id:string; conversations?:string[]; tasks?:string[] }
export type StopOutcome={status:'settled';tasks:Record<string,string>}|{status:'failed';tasks:Record<string,string>;error:string};
export const stopPlan=(id:string,intent:StopIntent):StopPlan=>({id,conversations:intent.conversations.map(String),tasks:intent.tasks.map(String)});
export async function pendingStops(harness:Harness,context:Context):Promise<StopPlan[]> {
 return harness.commit(async tx=>Object.entries((await open(tx)).team.stops).filter(([,i])=>!i.confirmed).map(([id,i])=>stopPlan(id,i)),context);
}
export async function confirmStop(harness:Harness,id:string,outcome:StopOutcome,context:Context,now=Date.now):Promise<void> {
 await harness.commit(async tx=>{
  const s=await open(tx),intent=s.team.stops[id];if(!intent||intent.confirmed)return;
  if(outcome.status!=='settled'){intent.error=outcome.error;return;}
  // Confirm the exact persisted targets, including tasks already terminal before mark.
  for(const target of intent.tasks){const task=await tx.task(target);if(!task||task.state.status!=='terminal')throw new Error('PTB stop target is not terminal');}
  if(s.team.current?.epoch!==intent.epoch)throw new Error('PTB stop epoch changed');
  const worker=intent.worker?s.team.current.workers[intent.worker]:undefined;
  if(intent.binding&&(!worker||worker.generation!==intent.generation||worker.anchorId!==intent.binding.anchorId||worker.conversationId!==intent.binding.conversationId))throw new Error('PTB stop binding changed');
  intent.confirmed=true;delete intent.error;
  if(worker){worker.state='stopped';await event(tx,s,'team-lead','worker_stopped',{worker:worker.name,at:new Date(now()).toISOString()});}
  if(s.team.current.shutdownPending&&Object.values(s.team.stops).filter(i=>i.epoch===intent.epoch).every(i=>i.confirmed)){
   s.team.current.lifecycle='stopped';await event(tx,s,'team-lead','team_stopped',{at:new Date(now()).toISOString()});
  }
 },context);
}
async function addMechanicStops(s:State,shutdown:boolean):Promise<string|undefined> {
 const team=s.team.current!,obligations=await allObligations(s);
 for(const intent of Object.values(s.team.stops)){
  if(intent.epoch!==team.epoch||intent.confirmed)continue;
  for(const o of obligations)if(o.epoch===team.epoch&&o.recipient===intent.worker&&o.deliveryTaskId&&!intent.tasks.includes(o.deliveryTaskId))intent.tasks.push(o.deliveryTaskId);
 }
 if(!shutdown)return;
 const key=`${team.epoch}:shutdown-mechanics`;
 const tasks=obligations.filter(o=>o.epoch===team.epoch&&o.deliveryTaskId).map(o=>o.deliveryTaskId!);
 if(team.notifierId)tasks.push(team.notifierId);
 s.team.stops[key]??={epoch:team.epoch,tasks,conversations:[],confirmed:false};
 return key;
}
/** Called by NodePlugin.closing after its own lead Close intent is stored. */
export async function closing(harness:Harness,conversationId:string,context:Context):Promise<StopPlan[]> {
 return harness.commit(async tx=>{
  const s=await open(tx),team=s.team.current;
  if(!team||String(team.lead)!==conversationId||team.lifecycle!=='active')return [];
  team.shutdownPending=true;
  for(const w of Object.values(team.workers))if(w.state!=='stopped'){
   const key=`${team.epoch}:stop:${w.name}:g${w.generation}`;w.state='stopping';
   s.team.stops[key]??={epoch:team.epoch,worker:w.name,generation:w.generation,binding:{conversationId:w.conversationId,anchorId:w.anchorId},tasks:[w.anchorId],conversations:[w.conversationId],confirmed:false};
  }
  for(const o of await allObligations(s))if(o.epoch===team.epoch)await setObligationState(s,o,'retired');
  await addMechanicStops(s,true);
  flushAuthority(s);
  return Object.entries(s.team.stops).filter(([,i])=>i.epoch===team.epoch&&!i.confirmed).map(([id,i])=>stopPlan(id,i));
 },context);
}
export interface RuntimeOptions extends Pick<CoreOptions,'modelRoles'|'defaultModelRole'|'catalog'|'goalMaxLength'|'resolveModelRole'> {
 observationPort:ObservationPortResolver;
 abortRun:AbortRun;
 // Marks every plan before it settles any, then settles and confirms each; one outcome per plan, in order.
 stop:(api:CoreApi,plans:StopPlan[],context:Context)=>Promise<StopOutcome[]>;
}
export function createTeamRuntime(options:RuntimeOptions){
 const Notify=createNotifier(catalogOf(options));
 const Deliver=createDeliver(options.abortRun);
 let lead:Extension,worker:Extension;
 const core=createAuthorityTools({...options,sync,
  createWorker:async(tx,role)=>{
   const s=await open(tx),binding=await createAnchoredConversation(tx,s.team.current!.lead);
   await configure(tx,binding.conversationId,{model:role.model,...(role.thinking?{thinkingLevel:role.thinking as any}:{}),extensions:{remove:[lead],add:[worker]}});
   return binding;
  },
  settleStops:async(stops,context,api)=>stops.length?(await options.stop(api,stops.map(s=>stopPlan(s.key,s.intent)),context)).map(o=>o.status==='settled'):[],
  afterCommand:async(tx,s,name,result)=>{
   const team=s.team.current;if(!team||team.lifecycle!=='active')return;
   if(name==='team_create'){startObservationEpoch(await openObservations(tx),team.epoch);}
   if(!team.notifierId&&!team.shutdownPending)team.notifierId=await tx.createTask(Notify,{epoch:team.epoch},{conversationId:team.lead,ownership:{kind:'conversation'},background:true,abandonOnRestart:false});
   await scheduleDeliveries(tx,s,Deliver);
   const extra=name==='worker_stop'||name==='team_shutdown'?await addMechanicStops(s,name==='team_shutdown'):undefined;
   if(extra&&Array.isArray(result.stops)&&!result.stops.includes(extra))result.stops.push(extra);
  },
 });
 lead={name:'ptb.lead',tools:core.lead};worker={name:'ptb.worker',tools:core.worker};
 const mechanics:Extension={name:'ptb.mechanics',tasks:[WorkerAnchor,Deliver,Notify]};
 const observation=observationExtension(options.observationPort);
 return {core,lead,worker,mechanics,observation,extensions:[mechanics,lead,worker,observation],tasks:{WorkerAnchor,Deliver,Notify}};
}
