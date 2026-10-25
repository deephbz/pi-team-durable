import { expect,test } from 'vitest';
import type { ConversationId } from '@earendil-works/pi-durable';
import { deliveryDraft,deliveryInput,SENDER_LIMIT } from '../../src/authority/delivery.ts';
import { pageDraft } from '../../src/authority/notifier.ts';
import type { Obligation,State } from '../../src/authority/state.ts';
import type { Page } from '../../src/authority/observation.ts';

const lead=1 as ConversationId;
const obligation=(o:Partial<Obligation>):Obligation=>({key:'k1',epoch:'e1',kind:'stop',recipient:'worker-a',conversationId:2 as ConversationId,generation:1,state:'pending',...o});
const state=(from:string)=>({outbox:{alerts:{a1:{id:'a1',from,kind:'attention',text:'Check the gate\nDetails follow',receivedAt:'2026-10-25T00:00:00Z',recipients:[]}},obligations:{}}}) as unknown as State;

test('an alert delivery names the alert author in its queue entry; the entry data is structured, not prose',()=>{
 const input=deliveryInput(state('reviewer'),obligation({kind:'alert',alertId:'a1'}));
 expect(input).toMatchObject({kind:'alert',sender:'reviewer',whenBusy:'steer'});
 expect(deliveryDraft(input).entry).toEqual({kind:'team.notice',data:{source:'team',deliveryId:'k1',epoch:'e1',delivery:'alert',sender:'reviewer'}});
});

test('a stop delivery names team-lead, the authority it acts for; a long author is bounded',()=>{
 expect(deliveryInput(state('x'),obligation({kind:'stop',taskId:'t1'}))).toMatchObject({kind:'stop',sender:'team-lead'});
 expect(deliveryInput(state('r'.repeat(200)),obligation({kind:'alert',alertId:'a1'})).sender).toHaveLength(SENDER_LIMIT);
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

test('an interrupt Alert names itself in the entry data; the interruption outcome stays in the model-facing content',()=>{
 const input=deliveryInput(state('reviewer'),obligation({kind:'alert',alertId:'a1',interrupt:true}));
 const draft=deliveryDraft(input,'interrupted');
 expect(draft.entry.data).toEqual({source:'team',deliveryId:'k1',epoch:'e1',delivery:'alert',sender:'reviewer',interrupt:true});
 expect(JSON.parse(draft.content).interruption.outcome).toBe('interrupted');
 expect(deliveryDraft(deliveryInput(state('reviewer'),obligation({kind:'alert',alertId:'a1'}))).entry.data).not.toHaveProperty('interrupt');
});
