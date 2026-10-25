// Compare public outputs under one native-storage-ID renaming. Graph IDs and caller IDs stay exact.
// Actor bindings anchor routing before deliveries are compared. Never infer a Worker from a destination ID.
import assert from 'node:assert/strict';

type Part = string | number;
type Location = { path: Part[]; start: number; end: number; value: unknown };
const nativeFields = new Set(['conversationId', 'entryId', 'submissionId', 'taskId', 'anchorId', 'deliveryTaskId', 'notifierId', 'byTaskId', 'leadConversationId', 'expectedRunTaskId']);
const nativeValue = (value: unknown): value is number | string => typeof value === 'number' || typeof value === 'string' && /^[1-9]\d*$/.test(value);
const at = (path: Part[]) => path.map(p => typeof p === 'number' ? `[${p}]` : `.${p}`).join('');

// JSON.parse validates syntax. This walk retains token offsets so ID substitution cannot hide
// whitespace, key-order, escaping, or any other non-ID byte change inside a delivery's content.
function jsonLocations(source: string): Location[] {
  JSON.parse(source);
  const tokens = [...source.matchAll(/"(?:\\.|[^"\\])*"|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?|true|false|null|[{}\[\],:]/g)];
  const locations: Location[] = []; let cursor = 0;
  function walk(path: Part[]) {
    const token = tokens[cursor++], raw = token[0];
    if (raw === '{') {
      while (tokens[cursor][0] !== '}') {
        const key = JSON.parse(tokens[cursor++][0]); cursor++; // colon
        walk([...path, key]); if (tokens[cursor][0] === ',') cursor++;
      }
      cursor++; return;
    }
    if (raw === '[') {
      let index = 0;
      while (tokens[cursor][0] !== ']') { walk([...path, index++]); if (tokens[cursor][0] === ',') cursor++; }
      cursor++; return;
    }
    locations.push({ path, start: token.index!, end: token.index! + raw.length, value: JSON.parse(raw) });
  }
  walk([]); assert.equal(cursor, tokens.length);
  return locations;
}

export class NativeIdBijection {
  private readonly forward = new Map<number, number>();
  private readonly reverse = new Map<number, number>();
  private readonly contentIds = new Map<string, Location[]>();
  private readonly external = new Set<number>([900000, 900001]); // Synthetic persisted run coordinates.
  readonly pairs: { oracle: number; current: number; firstSeenAt: string }[] = [];
  readonly stringLocations = new Map<string, { oracle: number | string; current: number | string }>();

  pin(id: number) { this.external.add(id); }

  bind(oracle: number | string, current: number | string, path: string) {
    assert.equal(typeof current, typeof oracle, `Native ID type at ${path}`);
    const expected = Number(oracle), actual = Number(current);
    assert(Number.isSafeInteger(expected) && expected > 0 && Number.isSafeInteger(actual) && actual > 0, `Invalid native ID at ${path}`);
    if (this.external.has(expected) || this.external.has(actual)) {
      assert.equal(current, oracle, `Caller-supplied ID must stay exact at ${path}`); return;
    }
    if (this.forward.has(expected)) assert.equal(actual, this.forward.get(expected), `Native ID changed its binding at ${path}`);
    if (this.reverse.has(actual)) assert.equal(expected, this.reverse.get(actual), `Native ID collision at ${path}`);
    if (!this.forward.has(expected)) {
      this.forward.set(expected, actual); this.reverse.set(actual, expected);
      this.pairs.push({ oracle: expected, current: actual, firstSeenAt: path });
    }
  }

  bindActors(oracle: ReadonlyMap<string, number>, current: ReadonlyMap<string, number>) {
    assert.deepStrictEqual([...current.keys()], [...oracle.keys()], 'Carrier actor bindings');
    for (const [actor, id] of oracle) this.bind(id, current.get(actor)!, `$actors.${actor}.conversationId`);
  }

  compare(current: unknown, oracle: unknown, label: string) {
    const normalized = this.normalize(current, oracle, label);
    assert.deepStrictEqual(normalized, oracle, label);
    assert.equal(JSON.stringify(normalized), JSON.stringify(oracle), `${label}: exact serialized output`);
  }

  compareDeliveries(current: readonly unknown[], oracle: readonly unknown[]) {
    assert.equal(current.length, oracle.length, 'Created ptb.deliver task count');
    const normalized = current.map((input: any, index) => {
      const expected = oracle[index] as any, path = `$deliveries[${index}]`;
      const result = this.normalize(input, expected, path) as any;
      result.content = this.normalizeContent(input.content, expected.content, `${path}.content`);
      return result;
    });
    assert.equal(JSON.stringify(normalized), JSON.stringify(oracle), 'Created ptb.deliver inputs: exact non-ID bytes');
  }

  compareDraft(current: any, oracle: any, path = '$drafts[0]') {
    const normalized = this.normalize(current, oracle, path) as any;
    normalized.content = this.normalizeContent(current.content, oracle.content, `${path}.content`);
    assert.equal(JSON.stringify(normalized), JSON.stringify(oracle), `${path}: exact draft and entry-data bytes`);
  }

  private normalize(current: any, oracle: any, path: string, field?: string): unknown {
    if (field && nativeFields.has(field) && nativeValue(oracle)) {
      assert(nativeValue(current), `Native ID missing at ${path}`); this.bind(oracle, current, path); return oracle;
    }
    assert.equal(typeof current, typeof oracle, `Value type at ${path}`);
    if (oracle === null || typeof oracle !== 'object') {
      const encodedContent = (path.startsWith('$deliveries[') || path.startsWith('$drafts[')) && path.endsWith('.content');
      if (!encodedContent) assert.deepStrictEqual(current, oracle, `Value at ${path}`);
      return current;
    }
    assert(current !== null && Array.isArray(current) === Array.isArray(oracle), `Value shape at ${path}`);
    assert.deepStrictEqual(Object.keys(current), Object.keys(oracle), `Fields/order at ${path}`);
    if (Array.isArray(oracle)) return current.map((value: unknown, index: number) => this.normalize(value, oracle[index], `${path}[${index}]`));
    return Object.fromEntries(Object.entries(current).map(([key, value]) => [key, this.normalize(value, oracle[key], `${path}.${key}`, key)]));
  }

  private nativeLocations(source: string): Location[] {
    if (!this.contentIds.has(source)) this.contentIds.set(source, jsonLocations(source).filter(location => {
      const field = location.path.at(-1);
      return typeof field === 'string' && nativeFields.has(field) && nativeValue(location.value);
    }));
    return this.contentIds.get(source)!;
  }

  private normalizeContent(current: string, oracle: string, path: string): string {
    const expected = this.nativeLocations(oracle);
    if (!expected.length) return current;
    const actual = this.nativeLocations(current);
    const replacements: { start: number; end: number; text: string }[] = [];
    for (const location of expected) {
      const field = location.path.at(-1);
      if (typeof field !== 'string' || !nativeFields.has(field) || !nativeValue(location.value)) continue;
      const counterpart = actual.find(candidate => JSON.stringify(candidate.path) === JSON.stringify(location.path));
      const coordinate = `${path}${at(location.path)}`;
      assert(counterpart && nativeValue(counterpart.value), `Native ID missing inside JSON at ${coordinate}`);
      // The runtime emits canonical decimal IDs. Do not normalize alternate number or string encodings.
      assert.equal(current.slice(counterpart.start, counterpart.end), JSON.stringify(counterpart.value), `Native ID encoding at ${coordinate}`);
      assert.equal(oracle.slice(location.start, location.end), JSON.stringify(location.value), `Oracle native ID encoding at ${coordinate}`);
      this.bind(location.value, counterpart.value, coordinate);
      if (location.value !== counterpart.value) this.stringLocations.set(coordinate, { oracle: location.value, current: counterpart.value });
      replacements.push({ start: counterpart.start, end: counterpart.end, text: JSON.stringify(location.value) });
    }
    let result = current;
    for (const replacement of replacements.sort((a, b) => b.start - a.start)) result = result.slice(0, replacement.start) + replacement.text + result.slice(replacement.end);
    return result;
  }
}
