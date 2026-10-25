import { expect,test } from 'vitest';
import { fauxAssistantMessage as answer,fauxToolCall as call } from '@earendil-works/pi-ai/providers/faux';
import { defineTool,GenerationTask,CompactionTask,hook } from '@earendil-works/pi-durable';
import { Type } from 'typebox';
import { runtimeFixture,context } from './runtime-fixture.ts';
import { observe,observations } from '../../src/authority/observation.ts';
import { open,event,plain } from '../../src/authority/state.ts';
const syncCall=(id:string)=>answer(call('team_sync',{view:'updates'},{id}),{stopReason:'toolUse'});

test('two sync calls reserve successive pages; persisted response plus run success ACKs the later sequence after reopen',async()=>{
 const f=await runtimeFixture();try{
  f.faux.setResponses([syncCall('sync-a'),async()=>{
   await f.root.commit(async tx=>{const s=await open(tx);await event(tx,s,'team-lead','team_created');},context);
   return syncCall('sync-b');
  },()=>{
   f.storage.fault=writes=>writes.some(w=>w.type==='entry'&&w.value.kind==='pi.assistant'&&JSON.stringify(w.value.model).includes('Final persisted response'));
   return answer('Final persisted response');
  }]);
  const submission=await f.root.submit({type:'input',content:'Read Team twice.'},context);
  await submission.wait(context).catch(()=>undefined);expect((await f.harness.closed).reason).toBe('failed');
  await f.reopen();
  expect((await (await f.harness.submission(submission.id,context))!.status(context)).status).toBe('done');
  const before=await f.root.commit(async tx=>plain(await observations(tx)),context);
  const pages=Object.values(before.pages);expect(pages).toHaveLength(2);expect(pages[1].from).toBe(pages[0].through);expect(pages[1].through).toBeGreaterThan(pages[0].through);
  expect(before.acknowledged).toBe(0);
  await observe(f.harness,context);
  const after=await f.root.commit(async tx=>plain(await observations(tx)),context);
  expect(after.acknowledged).toBe(pages[1].through);expect(Object.values(after.pages).every(p=>p.state==='acknowledged')).toBe(true);
  expect(f.faux.state.callCount).toBe(3);
 }finally{await f.close();}
});

test('model success followed by a failed tool and failed run releases both pages once',async()=>{
 const fail=defineTool({name:'fail_test',description:'Fail',parameters:Type.Object({}),replay:'safe',execute:async()=>{throw new Error('synthetic tool failure');}});
 const f=await runtimeFixture([{name:'fail-test',tools:[fail]}]);try{
  f.faux.setResponses([syncCall('one'),async()=>{await f.root.commit(async tx=>{const s=await open(tx);await event(tx,s,'team-lead','team_created');},context);return syncCall('two');},answer(call('fail_test',{}, {id:'fail'}),{stopReason:'toolUse'}),answer([],{stopReason:'error',errorMessage:'synthetic final failure'})]);
  const failed=await f.root.submit({type:'input',content:'Read then fail.'},context);expect((await failed.wait(context)).status).toBe('unanswered');
  await observe(f.harness,context);
  const before=await f.root.commit(async tx=>plain(await observations(tx)),context);
  expect(before.acknowledged).toBe(0);expect(Object.values(before.pages).map(p=>p.state)).toEqual(['released','released']);
  f.faux.setResponses([syncCall('again-one'),syncCall('again-two'),syncCall('again-three'),answer('Recovered')]);
  const recovered=await f.root.submit({type:'input',content:'Read released pages.'},context);expect((await recovered.wait(context)).status).toBe('done');
  await observe(f.harness,context);
  const after=await f.root.commit(async tx=>plain(await observations(tx)),context);
  expect(Object.values(after.pages).map(p=>p.state)).toEqual(['replaced','replaced','acknowledged','acknowledged']);
  expect(after.acknowledged).toBe(2);
 }finally{await f.close();}
});

test('compaction before presentation releases the unseen page without ACK',async()=>{
 let f!:Awaited<ReturnType<typeof runtimeFixture>>,compacted=false;
 const remove={name:'compact-between-rounds',hooks:[
  hook(CompactionTask,{beforeCompact:()=>({summary:'Previous work summarized without the Team page.'})}),
  hook(GenerationTask,{afterTools:async()=>{
   if(compacted)return;compacted=true;
   await f.root.commit(tx=>tx.appendEntry(f.root.id,{kind:'ptb.test-continuation',model:[{role:'user',content:'Continue after context boundary.',timestamp:1000}]}),context);
   const id=await f.root.compact('Test compaction',context);
   expect((await f.harness.waitForTask(id,context)).state.outcome.status).toBe('completed');
  }}),
 ]};
 f=await runtimeFixture([remove]);try{
  f.faux.setResponses([syncCall('cut'),answer('Done without the page')]);
  expect((await (await f.root.submit({type:'input',content:'Read Team.'},context)).wait(context)).status).toBe('done');
  await observe(f.harness,context);const o=await f.root.commit(async tx=>plain(await observations(tx)),context);
  expect(o.acknowledged).toBe(0);expect(Object.values(o.pages)[0].state).toBe('released');
 }finally{await f.close();}
});
