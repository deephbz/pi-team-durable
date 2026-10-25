import { expect,test } from 'vitest';
import { performance } from 'node:perf_hooks';
import { BACKGROUND_CONTEXT as context } from '@earendil-works/chord/context';
import { createModels } from '@earendil-works/pi-ai/models';
import { createRegistry,Harness,MemoryStorage } from '@earendil-works/pi-durable';
import { event,open,plain,JOURNAL_RESIDENT_LIMIT } from '../../src/authority/state.ts';
import { openAuthority } from '../../src/authority/store.ts';

test('long-idle journal retains history with bounded cold-loaded projection',async()=>{
 class ReopenableMemory extends MemoryStorage { override async close(){} }
 const storage=new ReopenableMemory();
 const options={models:createModels(),registry:createRegistry(),now:()=>1000,setTimer:()=>()=>{}};
 let harness=await Harness.open(storage,options,context);
 try{
  const root=await harness.root(context);
  await root.commit(async tx=>{const s=await open(tx);s.team.current={epoch:'demo',name:'demo',purpose:'test',lead:root.id,lifecycle:'active',workers:{}};},context);
  const started=performance.now();
  for(let batch=0;batch<40;batch++)await root.commit(async tx=>{const s=await open(tx);for(let i=0;i<50;i++)await event(tx,s,'worker','task_transitioned',{taskId:'synthetic'});},context);
  const writeMs=performance.now()-started;
  await harness.close(context);harness=await Harness.open(storage,options,context);
  const coldStart=performance.now();const reopened=await harness.root(context);
  const journal=await reopened.commit(async tx=>plain((await openAuthority(tx)).journal),context);
  const coldMs=performance.now()-coldStart,bytes=Buffer.byteLength(JSON.stringify(journal));
  expect(journal.seq).toBe(2000);expect((journal.events as any[]).length).toBe(JOURNAL_RESIDENT_LIMIT);expect(bytes).toBeLessThan(16000);
  const first=await reopened.entries({order:'ascending'},1,undefined,context);
  expect(first.items[0].kind).toBe('ptb.event');expect((first.items[0].data as any).seq).toBe(1);
  console.log(JSON.stringify({events:2000,residentEvents:JOURNAL_RESIDENT_LIMIT,residentBytes:bytes,writeMs:Math.round(writeMs),coldLoadMs:Math.round(coldMs),backend:'MemoryStorage; retained history is outside projection bound'}));
 }finally{await harness.close(context);}
});
