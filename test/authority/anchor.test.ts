import { expect,test } from 'vitest';
import { BACKGROUND_CONTEXT as context } from '@earendil-works/chord/context';
import { createModels } from '@earendil-works/pi-ai/models';
import { createRegistry,defineTask,Harness,MemoryStorage } from '@earendil-works/pi-durable';
import { WorkerAnchor,createAnchoredConversation,ANCHOR_SLEEP_MS } from '../../src/authority/anchor.ts';

test('live anchor survives lead Esc and reopen; marking before resume stops owned work',async()=>{
 let enters=0;
 const Child=defineTask<null,{phase:'hold'},null>({name:'test.worker',version:1,initial:()=>({phase:'hold'}),phases:{hold:async(_t,r,c)=>{enters++;await r.sleepUntil(Number.MAX_SAFE_INTEGER,{phase:'hold'},c);}},abort:async(_t,r,c)=>r.commit(()=>({status:'terminal',outcome:{status:'aborted'}}),c)});
 const registry=createRegistry();registry.install({name:'ptb-test',tasks:[WorkerAnchor,Child]});
 // Keep the backing memory image across Harness lifetimes; no writes are lost.
 class ReopenableMemory extends MemoryStorage { override async close(){} }
 const storage=new ReopenableMemory();
 let asleep!:()=>void;const sleeping=new Promise<void>(r=>asleep=r);
 const options={models:createModels(),registry,now:()=>1000,setTimer:()=>()=>{},onWake:(wake:any)=>{if(!wake.active&&wake.at===1000+ANCHOR_SLEEP_MS)asleep();}};
 let harness=await Harness.open(storage,options,context);
 try{
  const root=await harness.root(context);
  const binding=await root.commit(tx=>createAnchoredConversation(tx,root.id),context);
  const child=await root.commit(tx=>tx.createTask(Child,null,{conversationId:binding.conversationId,ownership:{kind:'conversation'},abandonOnRestart:false}),context);
  await root.abort(context);await sleeping;
  expect((await harness.getTask(binding.anchorId,context))!.state.status).toBe('sleeping');
  expect((await harness.getTask(child,context))!.state.status).toBe('sleeping');
  expect(enters).toBe(1);
  await harness.close(context);
  harness=await Harness.open(storage,options,context);
  expect((await harness.getTask(binding.anchorId,context))!.state.status).toBe('sleeping');
  await harness.abortTask(binding.anchorId,context);
  expect(enters).toBe(1);
  harness.resume();
  expect((await harness.waitForTask(binding.anchorId,context)).state.outcome.status).toBe('aborted');
  expect((await harness.getTask(child,context))!.state.status).toBe('terminal');
  expect(enters).toBe(1);
 }finally{await harness.close(context);}
});
