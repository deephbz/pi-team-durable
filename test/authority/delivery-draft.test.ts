import { expect,test } from 'vitest';
import { Harness,MemoryStorage,createRegistry,type ConversationId } from '@earendil-works/pi-durable';
import { createModels } from '@earendil-works/pi-ai/models';
import { BACKGROUND_CONTEXT as context } from '@earendil-works/chord/context';
import { openAuthority,addAlert } from '../../src/authority/store.ts';
import { deliveryDraft,deliveryInput,SENDER_LIMIT } from '../../src/authority/delivery.ts';
import { pageDraft } from '../../src/authority/notifier.ts';
import type { Obligation } from '../../src/authority/state.ts';
import type { Page } from '../../src/authority/observation.ts';

const lead=1 as ConversationId;
const obligation=(o:Partial<Obligation>):Obligation=>({key:'k1',epoch:'e1',kind:'stop',recipient:'worker-a',conversationId:2 as ConversationId,generation:1,state:'pending',...o});
async function input(from:string,fields:Partial<Obligation>){
 const h=await Harness.open(new MemoryStorage(),{models:createModels(),registry:createRegistry(),now:()=>0,setTimer:()=>()=>{}},context);
 try{return await h.commit(async tx=>{
  const s=await openAuthority(tx);
  // The oversized historical sender cannot be created through the current Worker schema.
  await addAlert(s,{id:'a1',from,kind:'attention',text:'Check the gate\nDetails follow',receivedAt:'2026-10-25T00:00:00Z',recipients:[]});
  return deliveryInput(s,obligation(fields));
 },context);}finally{await h.close(context);}
}

test('an alert delivery names the alert author in its queue entry; the entry data is structured, not prose',async()=>{
 const draftInput=await input('reviewer',{kind:'alert',alertId:'a1'});
 expect(draftInput).toMatchObject({kind:'alert',sender:'reviewer',whenBusy:'steer'});
 expect(deliveryDraft(draftInput).entry).toEqual({kind:'team.notice',data:{source:'team',deliveryId:'k1',epoch:'e1',delivery:'alert',sender:'reviewer'}});
});

test('a stop delivery names team-lead, the authority it acts for; a long author is bounded',async()=>{
 expect(await input('x',{kind:'stop',taskId:'t1'})).toMatchObject({kind:'stop',sender:'team-lead'});
 expect((await input('r'.repeat(200),{kind:'alert',alertId:'a1'})).sender).toHaveLength(SENDER_LIMIT);
});

test('an input persisted before kind and sender existed keeps the legacy entry data',()=>{
 const draft=deliveryDraft({key:'k0',epoch:'e0',conversationId:lead,content:'{}',whenBusy:'steer'});
 expect(draft.entry.data).toEqual({source:'team',deliveryId:'k0',epoch:'e0'});
 expect(draft.requestId).toBe('ptb:k0');
});

test('a notifier page is a Team update that durable-team writes',()=>{
 const page={id:'e1:notify:1:1',epoch:'e1',lead,carrier:{kind:'input',text:'{"kind":"team_update"}'}} as unknown as Page;
 expect(pageDraft(page).entry).toEqual({kind:'team.notice',data:{source:'team',pageId:'e1:notify:1:1',epoch:'e1',delivery:'update',sender:'durable-team'}});
});

test('an interrupt Alert names itself in the entry data; the interruption outcome stays in the model-facing content',async()=>{
 const draftInput=await input('reviewer',{kind:'alert',alertId:'a1',interrupt:true});
 const draft=deliveryDraft(draftInput,'interrupted');
 expect(draft.entry.data).toEqual({source:'team',deliveryId:'k1',epoch:'e1',delivery:'alert',sender:'reviewer',interrupt:true});
 expect(JSON.parse(draft.content).interruption.outcome).toBe('interrupted');
 expect(deliveryDraft(await input('reviewer',{kind:'alert',alertId:'a1'})).entry.data).not.toHaveProperty('interrupt');
});
