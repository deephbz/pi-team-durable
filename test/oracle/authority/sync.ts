import { LiveDoc,type Tx,type EntryId } from '@earendil-works/pi-durable';
import type { CoreApi } from './tools.ts';
import { GraphTaskController } from './graph-control.ts';
import { projectGraphTaskCards } from './graph-card.ts';
import { observations,reservationHead,reserve,type Page } from './observation.ts';
import { plain,type State,type Event,type Result } from './state.ts';
export const UPDATE_PAGE_EVENTS=50;
export const UPDATE_SCAN_ENTRIES=100;

export function updates(s:State,events:Event[]):Result {
 const team=s.team.current!,cards=projectGraphTaskCards(GraphTaskController.recover(plain(s.graph.snapshot!)));
 const ids=[...new Set(events.flatMap(e=>e.taskIds??(e.taskId?[e.taskId]:[])))];
 return {kind:'updates',
  team_changes:events.filter(e=>e.kind==='team_created'||e.kind==='team_stopped').map(e=>({kind:e.kind==='team_created'?'created':'lifecycle',text:e.kind==='team_created'?team.purpose:'Team stopped.'})),
  worker_changes:events.filter(e=>e.worker&&['worker_created','worker_stopped'].includes(e.kind)).map(e=>({worker:e.worker,scope:team.workers[e.worker!]?.scope??'Historical Worker',kind:e.kind==='worker_created'?'created':'stopped',text:e.kind==='worker_created'?'Worker created.':'Worker stopped.'})),
  task_changes:ids.flatMap(id=>{const current=cards.find(t=>t.id===id);return current?[{task_id:id,change_kinds:['status'],journal_entries:events.filter(e=>e.taskId===id&&e.at&&e.text).map(e=>({id:`${e.epoch}:${e.seq}`,at:e.at,actor:e.actor,kind:'note',text:e.text})),current}]:[];}),
  alerts:[],
 };
}
/** Page native history without making the journal document an unbounded cache. */
export async function pageEvents(tx:Tx,s:State,from:number,after?:EntryId):Promise<{events:Event[];scanThrough?:EntryId}> {
 const recent=s.journal.events.filter(e=>e.seq>from);
 if(recent.length&&s.journal.events[0].seq<=from+1){const events=recent.slice(0,UPDATE_PAGE_EVENTS);return {events,scanThrough:events.at(-1)?.entryId};}
 const native=await tx.scanEntries({conversationId:s.team.current!.lead,order:'ascending',...(after?{minEntryId:after}:{})},UPDATE_SCAN_ENTRIES);
 const events:Event[]=[];let scanThrough=after;
 for(const entry of native.items){
  if(after!==undefined&&entry.id<=after)continue;
  scanThrough=entry.id;
  if(entry.kind==='ptb.event'){
   const event=entry.data as unknown as Event;
   if(event.epoch===s.team.current!.epoch&&event.seq>from)events.push({...event,entryId:entry.id});
  }
  if(events.length>=UPDATE_PAGE_EVENTS)break;
 }
 return {events,scanThrough};
}
export async function sync(tx:Tx,s:State,view:'snapshot'|'updates'|undefined,api:CoreApi,snapshot:Result):Promise<Result> {
 const team=s.team.current!,live=await tx.doc(LiveDoc,api.conversationId),run=live.run?.inputs[0];
 if(run===undefined)return {kind:'unavailable',reason:'team_state_unavailable',message:'Observation requires a persisted lead run.'};
 const o=await observations(tx);o.paused=false;
 if(Object.values(o.pages).some(p=>p.epoch===team.epoch&&p.run!==run&&['reserved','presented'].includes(p.state)))return {kind:'refused',reason:'observation_in_progress',message:'Another run or automatic delivery owns an unacknowledged page.',state_changed:false,observation_advanced:false};
 const id=`${team.epoch}:sync:${api.taskId}`,from=reservationHead(o,team.epoch,run);
 const carrier={kind:'tool' as const,callId:api.callId??String(api.taskId)};
 // Failed-run pages reappear once in order; the old replay receipt remains immutable.
 const released=Object.values(o.pages).find(p=>p.epoch===team.epoch&&p.state==='released');
 if(released){
  const result=JSON.parse(released.body) as Result;released.state='replaced';
  reserve(o,{id,epoch:team.epoch,lead:team.lead,run,from:released.from,through:released.through,carrier,...(released.scanThrough?{scanThrough:released.scanThrough}:{})},result);
  return result;
 }
 const outstanding=Object.values(o.pages).filter(p=>p.epoch===team.epoch&&p.run===run&&['reserved','presented'].includes(p.state));
 if((view!=='snapshot'||outstanding.length>0)&&from>=s.journal.seq)return {kind:'caught_up',head:s.journal.seq,epoch_id:team.epoch};
 let result:Result,through:number,scanThrough:EntryId|undefined;
 if(!outstanding.length&&(view==='snapshot'||from===0)){
  result=snapshot;through=s.journal.seq;scanThrough=s.journal.events.at(-1)?.entryId;
 }else{
  const previous=Object.values(o.pages).filter(p=>p.epoch===team.epoch&&p.scanThrough!==undefined&&(p.state==='acknowledged'||p.run===run&&['reserved','presented'].includes(p.state))).sort((a,b)=>Number(b.scanThrough)-Number(a.scanThrough))[0];
  const page=await pageEvents(tx,s,from,previous?.scanThrough);
  result=updates(s,page.events);through=page.events.at(-1)?.seq??from;scanThrough=page.scanThrough;
 }
 reserve(o,{id,epoch:team.epoch,lead:team.lead,run,from,through,carrier,...(scanThrough?{scanThrough}:{})},result);
 return result;
}
