import { expect,test } from 'vitest';
import { mkdtempSync,rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { CrashChild } from '../support/crash-child.ts';
import { Deliver } from '../../src/authority/delivery.ts';

const fixture=new URL('./delivery-version.fixture.ts',import.meta.url);
// Child start-up under load takes seconds. These bounds only stop a hung child.
const BOUND=20000;
async function killAt(c:CrashChild,name:string){await c.wait('barrier',name,BOUND);c.child.kill('SIGKILL');expect((await c.exited).signal).toBe('SIGKILL');}
async function result(c:CrashChild){const r=await c.wait('result',undefined,BOUND);expect(await c.exited).toEqual({code:0,signal:null});return r.value;}
test('SIGKILL after delivery migration preserves P inputs, checkpoints, queue IDs, and notice bytes',async()=>{
 const dir=mkdtempSync(join(tmpdir(),'ptb-delivery-version-')),children:CrashChild[]=[];
 const child=(mode:string)=>{const value=new CrashChild(dir,'delivery-version',mode,fixture);children.push(value);return value;};
 try{
  await killAt(child('seed'),'v1-seeded');
  await killAt(child('migrate'),'v2-migrated');
  const recovered=await result(child('recovered'));
  expect(recovered.before.version).toBe(2);expect(recovered.version).toBe(2);
  expect(recovered.before.input).toEqual(recovered.ids.input);
  expect(recovered.before.state.checkpoint).toEqual(recovered.ids.checkpoint);
  expect(recovered.selected).toBe(recovered.ids.captured);
  expect(recovered.submissions).toBe(1);expect(recovered.queued).toBe('done');
  expect(recovered.content).toBe(recovered.legacyContent);expect(recovered.data).toEqual(recovered.legacyData);
  const once=Deliver.definition.migrate!(recovered.ids.input,recovered.ids.checkpoint,1);
  expect(Deliver.definition.migrate!(once.input as any,once.checkpoint,1)).toEqual(once);
 }finally{for(const c of children)c.cleanup();await Promise.all(children.map(c=>c.exited));rmSync(dir,{recursive:true,force:true});}
},90000);


test('the 0.1.0-rc.2 reader visibly parks a new version-2 interrupt delivery without admission or retirement',async()=>{
 const dir=mkdtempSync(join(tmpdir(),'ptb-delivery-reader-')),children:CrashChild[]=[];
 try{
  const seed=new CrashChild(dir,'delivery-version','new-seed',fixture);children.push(seed);await killAt(seed,'v2-seeded');
  const reader=new CrashChild(dir,'delivery-version','old-reader',fixture);children.push(reader);const old=await result(reader);
  expect(old.before.version).toBe(2);expect(old.before.input.interruptAt).toBe('delivery');expect(old.after).toEqual(old.before);
  expect(old.diagnostic).toEqual({kind:'blocked',reason:'task_too_old'});
  expect(old.state).toBe('pending');expect(old.submissions).toBe(0);expect(old.oldAbortCalls).toBe(0);
 }finally{for(const c of children)c.cleanup();await Promise.all(children.map(c=>c.exited));rmSync(dir,{recursive:true,force:true});}
},90000);
