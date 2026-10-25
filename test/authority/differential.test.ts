import assert from 'node:assert/strict';
import { test } from 'vitest';
import { NativeIdBijection } from './native-id-bijection.ts';
import { runDifferential, runTargeted } from './differential.ts';

test('seeded differential public authority contract', () => runDifferential(3), 120000);
test('targeted lineage, repair, capacity, and generation boundaries', runTargeted, 120000);

const actorMap = () => {
  const ids = new NativeIdBijection();
  ids.bindActors(new Map([['lead', 1], ['w0', 10], ['w1', 20]]), new Map([['lead', 1], ['w0', 30], ['w1', 40]]));
  return ids;
};
const delivery = (conversationId: number, content = '{"kind":"task_changed","task_id":"t0"}') => ({ key: 'team:1000001:task:t0:v_123:g1', epoch: 'team:1000001', conversationId, content, whenBusy: 'steer' });

test('semantic actor anchors reject a Worker destination swap and an inverse collision', () => {
  const ids = actorMap();
  assert.throws(() => ids.compareDeliveries([delivery(40)], [delivery(10)]), /Native ID changed its binding/);
  assert.throws(() => ids.bind(50, 30, '$collision'), /Native ID collision/);
});

test('encoded native IDs use the same map without normalizing any other bytes', () => {
  const ids = actorMap();
  const expected = '{"kind":"alert","alert":{"id":"team:1000001:alert:1000002","recipients":[{"name":"w0","conversationId":10}],"text":"Conversation 10"}}';
  const actual = expected.replace('"conversationId":10', '"conversationId":30');
  ids.compareDeliveries([delivery(30, actual)], [delivery(10, expected)]);
  assert(ids.stringLocations.has('$deliveries[0].content.alert.recipients[0].conversationId'));
  for (const changed of [actual + ' ', actual.replace('Conversation 10', 'Conversation 30'), actual.replace('alert:1000002', 'alert:1000003')]) {
    assert.throws(() => ids.compareDeliveries([delivery(30, changed)], [delivery(10, expected)]), /exact non-ID bytes/);
  }
});

test('caller IDs, graph IDs, version tokens, and field order remain exact', () => {
  const ids = actorMap(); ids.pin(1000001);
  assert.throws(() => ids.compare({ taskId: 30 }, { taskId: 1000001 }, '$tool'), /Caller-supplied ID/);
  assert.throws(() => ids.compare({ id: '30' }, { id: '10' }, '$card'));
  assert.throws(() => ids.compare({ version: 'v_30' }, { version: 'v_10' }, '$card'));
  assert.throws(() => ids.compare({ title: 'A', id: 't0' }, { id: 't0', title: 'A' }, '$card'), /Fields\/order/);
});

test('captured caller run IDs remain exact', () => {
  const ids = actorMap(); ids.pin(900100);
  assert.throws(() => ids.compareDeliveries([{ ...delivery(30), expectedRunTaskId: 900101 }], [{ ...delivery(10), expectedRunTaskId: 900100 }]), /Caller-supplied ID/);
});

test('draft comparisons preserve every notice field and interruption outcome', () => {
  const ids = actorMap();
  const draft = { type: 'input', content: '{"kind":"alert","interruption":{"outcome":"interrupted","message":"Stopped"}}', whenBusy: 'steer', requestId: 'ptb:alert-1',
    entry: { kind: 'team.notice', data: { source: 'team', deliveryId: 'alert-1', epoch: 'epoch-1', delivery: 'alert', sender: 'w0', interrupt: true } } };
  ids.compareDraft(draft, draft);
  const encoded = { ...draft, content: '{"kind":"alert","alert":{"recipients":[{"conversationId":10}],"text":"Conversation 10"}}' };
  const renamed = { ...encoded, content: encoded.content.replace('"conversationId":10', '"conversationId":30') };
  ids.compareDraft(renamed, encoded);
  assert.throws(() => ids.compareDraft({ ...renamed, content: renamed.content.replace('Conversation 10', 'Conversation 30') }, encoded), /exact draft/);
  for (const field of ['source', 'deliveryId', 'epoch', 'delivery', 'sender', 'interrupt']) {
    const changed: any = structuredClone(draft); delete changed.entry.data[field];
    assert.throws(() => ids.compareDraft(changed, draft), /Fields\/order/);
  }
  for (const field of ['source', 'deliveryId', 'epoch', 'delivery', 'sender']) {
    const changed: any = structuredClone(draft); changed.entry.data[field] += '!';
    assert.throws(() => ids.compareDraft(changed, draft));
  }
  assert.throws(() => ids.compareDraft({ ...draft, content: draft.content.replace('interrupted', 'ended') }, draft), /exact draft/);
  const page = { ...draft, entry: { kind: 'team.notice', data: { source: 'team', pageId: 'page-1', epoch: 'epoch-1', delivery: 'update', sender: 'durable-team' } } };
  const changed: any = structuredClone(page); changed.entry.data.pageId = 'page-2';
  assert.throws(() => ids.compareDraft(changed, page));
});
