import { BACKGROUND_CONTEXT as context } from '@earendil-works/chord/context';
import { createModels } from '@earendil-works/pi-ai/models';
import { fauxProvider } from '@earendil-works/pi-ai/providers/faux';
import { createRegistry,Harness,MemoryStorage,type TaskId,type ConversationId,type Extension,type StorageWrite } from '@earendil-works/pi-durable';
import { createTeamRuntime,confirmStop } from '../../src/authority/runtime.ts';
import { abortExpectedRun,type AbortRun } from '../../src/authority/interrupt.ts';
import { open,plain } from '../../src/authority/state.ts';
import type { CoreApi } from '../../src/authority/tools.ts';
export { context };
export class ReopenableMemory extends MemoryStorage {
 fault?: (writes:readonly StorageWrite[])=>boolean;
 override async commit(writes:readonly StorageWrite[],c:typeof context){
  const seq=await super.commit(writes,c);
  if(this.fault?.(writes)){this.fault=undefined;throw new Error('simulated loss after durable commit');}
  return seq;
 }
 override async close(){}
}
export function nextTask(harness:Harness,id:TaskId,predicate:(state:any)=>boolean):Promise<void>{
 return new Promise(resolve=>{const off=harness.subscribeCommits(publication=>{
  if(publication.changes.some(c=>c.type==='task'&&c.value.id===id&&predicate(c.value.state))){off();resolve();}
 });});
}
export async function runtimeFixture(extra:Extension[]=[],wrapAbortRun:(port:AbortRun)=>AbortRun=port=>port,clock?:{now:()=>number;setTimer:(callback:()=>void,delayMs:number)=>()=>void}){
 const models=createModels(),faux=fauxProvider({provider:'ptb-faux',api:'ptb-faux',models:[{id:'test'}],tokensPerSecond:0});models.setProvider(faux.provider);
 let harness!:Harness,now=1000;
 const registry=createRegistry();
 const runtime=createTeamRuntime({modelRoles:[{name:'builder',use:'Build',model:{provider:'ptb-faux',modelId:'test'}}],defaultModelRole:'builder',
  abortRun:wrapAbortRun((_api,target,c)=>abortExpectedRun(harness,target,c)),
  observationPort:async()=>({commit:(fn,c)=>harness.commit(fn,c)}),
  stop:async(_api,plans,c)=>{
   for(const plan of plans)for(const id of plan.tasks??[])await harness.abortTask(Number(id) as TaskId,c);
   harness.resume();
   return Promise.all(plans.map(async plan=>{
    const tasks:Record<string,string>={};
    for(const id of plan.tasks??[])tasks[id]=(await harness.waitForTask(Number(id) as TaskId,c)).state.outcome.status;
    for(const id of plan.conversations??[])await (await harness.conversation(Number(id) as ConversationId,c))?.abort(c,{background:true});
    const result={status:'settled' as const,tasks};await confirmStop(harness,plan.id,result,c);return result;
   }));
  },
 });
 for(const extension of [...runtime.extensions,...extra])registry.install(extension);
 const storage=new ReopenableMemory();
 const reports:unknown[]=[];
 const options={models,registry,now:clock?.now??(()=>now),setTimer:clock?.setTimer??(()=>()=>{}),onWake:()=>{},onReport:(error:unknown)=>reports.push(error),settings:{retry:{maxRetries:0},compaction:{enabled:false,keepRecentTokens:0}}};
 harness=await Harness.open(storage,options,context);
 let root=await harness.root(context,{agent:{model:{provider:'ptb-faux',modelId:'test'},extensions:[runtime.mechanics,runtime.lead,...extra,runtime.observation]}});
 let n=1000000;
 const api=():CoreApi=>({taskId:++n as TaskId,conversationId:root.id,commit:root.commit.bind(root),now:options.now,callId:`direct-${n}`});
 await runtime.core.execute('team_create','lead',{name:'demo',purpose:'Test'},api(),context);
 return {runtime,registry,faux,reports,api,storage,get root(){return root;},get harness(){return harness;},setNow:(value:number)=>now=value,
  state:()=>root.commit(async tx=>plain(await open(tx)),context),
  reopen:async()=>{await harness.close(context);harness=await Harness.open(storage,options,context);root=await harness.root(context);},
  close:()=>harness.close(context),
 };
}
