import { expect,test } from 'vitest';
import { fauxAssistantMessage as answer } from '@earendil-works/pi-ai/providers/faux';
import { runtimeFixture,context,nextTask } from './runtime-fixture.ts';
import { pendingStops,closing,confirmStop } from '../../src/authority/runtime.ts';
import { observe,observations } from '../../src/authority/observation.ts';
import { plain } from '../../src/authority/state.ts';
import type { TaskId } from '@earendil-works/pi-durable';

async function graph(f:Awaited<ReturnType<typeof runtimeFixture>>){
 await f.runtime.core.execute('ensure_worker','lead',{name:'builder',scope:'Build'},f.api(),context);
 return f.runtime.core.execute('task_graph_apply','lead',{operation_id:'graph',tasks:[{key:'a',title:'A',goal:'Build A',assignee:'builder'}]},f.api(),context);
}
test('delivery replays after admission commit loss with one native submission',async()=>{
 const f=await runtimeFixture();try{
  await graph(f);const before=await f.state(),obligation=Object.values(before.outbox.obligations)[0],worker=before.team.current!.workers.builder;
  f.faux.setResponses([answer('Accepted')]);
  f.storage.fault=writes=>writes.some(w=>w.type==='submission'&&w.value.requestId===`ptb:${obligation.key}`);
  f.harness.resume();expect((await f.harness.closed).reason).toBe('failed');
  await f.reopen();
  expect((await f.harness.waitForTask(obligation.deliveryTaskId!,context)).state.outcome.status).toBe('completed');
  const submissions=await f.storage.scanSubmissions({conversationId:worker.conversationId},100,undefined,context);
  expect(submissions.items).toHaveLength(1);expect(submissions.items[0].status).toBe('done');
  expect(f.faux.state.callCount).toBe(1);expect((await f.state()).outbox.obligations[obligation.key].outcome).toBe('answered');
 }finally{await f.close();}
});

test('revision before scheduling retires stale delivery and admits only the current Task',async()=>{
 const f=await runtimeFixture();try{
  const first=await graph(f);
  await f.runtime.core.execute('task_graph_apply','lead',{operation_id:'revision',expected_graph_version:first.graph_version,tasks:[{key:'a',title:'A',goal:'Revised A',assignee:'builder'}]},f.api(),context);
  const state=await f.state(),obligations=Object.values(state.outbox.obligations);
  f.faux.setResponses([answer('Accepted revision')]);
  await Promise.all(obligations.map(o=>f.harness.waitForTask(o.deliveryTaskId!,context)));
  const worker=state.team.current!.workers.builder;
  expect((await f.storage.scanSubmissions({conversationId:worker.conversationId},100,undefined,context)).items).toHaveLength(1);
  const target=await f.harness.conversation(worker.conversationId,context),entries=await target!.entries({order:'ascending'},100,undefined,context);
  expect(JSON.stringify(entries.items)).toContain('Revised A');
  expect(f.faux.state.callCount).toBe(1);
 }finally{await f.close();}
});

test('notifier sleep and admitted page survive reopen without a duplicate submission; observer ACKs later',async()=>{
 const f=await runtimeFixture();try{
  const id=(await f.state()).team.current!.notifierId!;
  const asleep=nextTask(f.harness,id,s=>s.status==='sleeping');f.harness.resume();await asleep;
  await f.reopen();f.setNow(31000);
  f.faux.setResponses([answer('Observed Team update')]);
  f.storage.fault=writes=>writes.some(w=>w.type==='submission'&&w.value.requestId?.includes(':notify:'));
  f.harness.resume();expect((await f.harness.closed).reason).toBe('failed');
  await f.reopen();const settled=nextTask(f.harness,id,s=>s.status==='sleeping'&&s.checkpoint.ordinal===2);
  f.harness.resume();await settled;
  const submissions=await f.storage.scanSubmissions({conversationId:f.root.id},100,undefined,context);
  expect(submissions.items).toHaveLength(1);expect(f.faux.state.callCount).toBe(1);
  await observe(f.harness,context);const o=await f.root.commit(async tx=>plain(await observations(tx)),context);
  expect(o.acknowledged).toBe(1);expect(Object.values(o.pages)[0].state).toBe('acknowledged');
 }finally{await f.close();}
});

test('Close enumerates all mechanics before resume; failed confirmation stays actionable',async()=>{
 const f=await runtimeFixture();try{
  await graph(f);const before=await f.state();
  const plans=await closing(f.harness,String(f.root.id),context);
  expect(plans.flatMap(p=>p.tasks??[])).toContain(String(before.team.current!.notifierId));
  expect(plans.flatMap(p=>p.tasks??[])).toContain(String(Object.values(before.outbox.obligations)[0].deliveryTaskId));
  await confirmStop(f.harness,plans[0].id,{status:'failed',tasks:{},error:'Cancelled wait'},context);
  expect((await pendingStops(f.harness,context)).length).toBe(plans.length);
  expect((await f.runtime.core.execute('ensure_worker','lead',{name:'new',scope:'New'},f.api(),context)).kind).toBe('unavailable');
  await f.reopen();const pending=await pendingStops(f.harness,context);
  // This test supplies the node's port: mark every plan, then start scheduling.
  for(const plan of pending)for(const id of plan.tasks??[])await f.harness.abortTask(Number(id) as TaskId,context);
  expect(f.faux.state.callCount).toBe(0);f.harness.resume();
  for(const plan of pending){const tasks:Record<string,string>={};for(const id of plan.tasks??[])tasks[id]=(await f.harness.waitForTask(Number(id) as TaskId,context)).state.outcome.status;await confirmStop(f.harness,plan.id,{status:'settled',tasks},context);}
  expect(await pendingStops(f.harness,context)).toEqual([]);expect((await f.state()).team.current!.lifecycle).toBe('stopped');expect(f.faux.state.callCount).toBe(0);
 }finally{await f.close();}
});
