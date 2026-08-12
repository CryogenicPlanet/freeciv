/**
 * The remedy table.
 *
 * Two kinds of assertion here, and the second is the one that will actually
 * catch something one day. The first kind checks the mapping does what it says.
 * The second checks the table stays *honest*: that every failure this package
 * can itself produce has a hand-written entry, and that a tag nobody wrote an
 * entry for says so out loud instead of quietly emitting a plausible sentence.
 */
// `bun:test` bodies are promises, and reading the neighbouring package's source
// is what the pinning test below is *for*.
// oxlint-disable effecttsgo/async-function
// oxlint-disable effecttsgo/node-builtin-import
import { describe, expect, test } from 'bun:test';
import { readdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { Data, Option } from 'effect';
import { createError } from 'evlog';
import {
  causeOf,
  describeDetail,
  describeMessage,
  describeTag,
  errorFields,
  isRegisteredTag,
  REMEDIES,
  remedyFor,
  renderValue,
  tagOf,
  TelemetryEmitError,
  TelemetryExportError,
  TelemetryInitError,
  TelemetryWriteError,
  toEvlogError,
  UNREADABLE_FAILURE_TAG,
  WideEventAlreadySealedError,
} from 'src/index';

class DecodeFailed extends Data.TaggedError('WireDecodeError')<{
  readonly path: ReadonlyArray<string>;
}> {}

class NeverHeardOfIt extends Data.TaggedError('NeverHeardOfIt')<{
  readonly detail: string;
}> {}

/** Every tag this package itself can put on an event. */
const OWN_TAGS: ReadonlyArray<string> = [
  new TelemetryInitError({ service: 's', cause: null })._tag,
  new TelemetryEmitError({ eventId: 'e', name: 'n', cause: null })._tag,
  new TelemetryWriteError({ dir: 'd', events: 1, cause: null })._tag,
  new TelemetryExportError({ endpoint: 'u', events: 1, cause: null })._tag,
  new WideEventAlreadySealedError({ eventId: 'e', name: 'n' })._tag,
  UNREADABLE_FAILURE_TAG,
];

/** Where `@arena/wire` declares its failures. */
const WIRE_SRC = join(import.meta.dir, '..', '..', 'wire', 'src');

/** Every `.ts` file under `dir`, recursively. */
const sources = async (dir: string): Promise<ReadonlyArray<string>> => {
  const entries = await readdir(dir, { withFileTypes: true });
  const nested = await Promise.all(
    entries.map((entry) => {
      const path = join(dir, entry.name);
      if (entry.isDirectory()) return sources(path);
      return Promise.resolve(entry.name.endsWith('.ts') ? [path] : []);
    }),
  );
  return nested.flat();
};

/** The tags `@arena/wire` declares, read out of its source. */
const wireTags = async (): Promise<ReadonlyArray<string>> => {
  const files = await sources(WIRE_SRC);
  const texts = await Promise.all(files.map((file) => readFile(file, 'utf8')));
  return texts.flatMap((text) =>
    [...text.matchAll(/TaggedError\('([^']+)'\)/g)].flatMap(([, tag]) =>
      tag === undefined ? [] : [tag],
    ),
  );
};

describe('the table', () => {
  test('every entry has a why and a fix, and neither is a restatement', () => {
    const thin = Object.entries(REMEDIES).filter(
      ([, remedy]) =>
        remedy.why.length < 40 || remedy.fix.length < 40 || remedy.fix === remedy.why,
    );
    // Named rather than counted, so a failure says which entry is thin.
    expect(thin.map(([tag]) => tag)).toEqual([]);
  });

  test('every failure this package can produce has a hand-written entry', () => {
    // The one invariant that decays on its own: a new tagged error added to
    // `evlog-adapter.ts` with no table entry fails here rather than shipping a
    // fallback into the corpus.
    expect(OWN_TAGS.filter((tag) => !isRegisteredTag(tag))).toEqual([]);
  });

  test("@arena/wire's tags are the ones @arena/wire actually declares", async () => {
    // The entries for `@arena/wire` are *transcriptions* — this package sits
    // below it and must not import it — so nothing but this test connects them
    // to their source. Rename a tag over there and the table keeps a remedy for
    // a tag that no longer exists while the new one silently ships the fallback
    // into the corpus, with a green suite the whole way. Both directions are
    // checked, because a rename is both at once.
    const declared = await wireTags();
    const transcribed = Object.keys(REMEDIES).filter((tag) => !OWN_TAGS.includes(tag));

    // A refactor that moves the errors out of `arena/wire/src` should fail here
    // loudly rather than quietly assert nothing.
    expect(declared.length).toBeGreaterThan(0);
    expect(transcribed.toSorted()).toEqual(declared.toSorted());
  });

  test('a registered tag returns its entry verbatim', () => {
    expect(Object.is(remedyFor('WireDecodeError'), REMEDIES['WireDecodeError'])).toBe(true);
  });

  test('an unregistered tag gets a fallback that names its own remedy', () => {
    const remedy = remedyFor('NeverHeardOfIt');
    expect(isRegisteredTag('NeverHeardOfIt')).toBe(false);
    expect(remedy.why).toContain('NeverHeardOfIt');
    expect(remedy.why).toContain('No remedy is registered');
    // The fallback's `fix` is the one instruction that is always actionable:
    // where to go and what to add.
    expect(remedy.fix).toContain('arena/telemetry/src/error-map.ts');
    expect(remedy.fix).toContain('NeverHeardOfIt');
  });
});

describe('reading a failure value', () => {
  test('tagOf prefers _tag, falls back to Error.name, and gives up honestly', () => {
    expect(tagOf(new DecodeFailed({ path: ['a'] }))).toEqual(Option.some('WireDecodeError'));
    expect(tagOf(new RangeError('nope'))).toEqual(Option.some('RangeError'));
    expect(tagOf('a bare string')).toEqual(Option.none());
    expect(describeTag('a bare string')).toBe('Untagged');
  });

  test('describeMessage never returns [object Object]', () => {
    expect(describeMessage(new RangeError('nope'))).toBe('nope');
    expect(describeMessage('a bare string')).toBe('a bare string');
    expect(describeMessage(42)).toBe('42');
    expect(describeMessage(9007199254740993n)).toBe('9007199254740993');
    expect(describeMessage(null)).toBe('non-Error failure of type null');
    expect(describeMessage({ oops: true })).toBe('non-Error failure of type object');
    expect(describeMessage(undefined)).toBe('non-Error failure of type undefined');
  });

  test("a tagged error's message falls back to its tag, because it has none", () => {
    // `Data.TaggedError('X')<{...}>` leaves `message` empty. Recording `""` as
    // the message of every failure in this codebase would be worse than useless.
    expect(new DecodeFailed({ path: ['a'] }).message).toBe('');
    expect(describeMessage(new DecodeFailed({ path: ['a'] }))).toBe('WireDecodeError');
  });

  test('describeDetail says something the tag does not already say', () => {
    // The warning that announces a lost event prints the tag next to the
    // detail. A detail that repeats the tag is the failure mode this exists to
    // fix: a full disk and a BigInt produced the identical line.
    const write = new TelemetryWriteError({
      dir: '/full',
      events: 1,
      cause: new Error('ENOSPC: no space left on device'),
    });
    expect(describeMessage(write)).toBe('TelemetryWriteError');
    expect(describeDetail(write)).toBe('ENOSPC: no space left on device');

    // No cause to fall back on: name the payload rather than repeat the tag.
    expect(describeDetail(new DecodeFailed({ path: ['a'] }))).toBe('WireDecodeError; fields: path');
    // A real message is always the best answer.
    expect(describeDetail(new RangeError('nope'))).toBe('nope');
  });

  test('causeOf sees the cause errorFields cannot', () => {
    const cause = new Error('ENOSPC');
    const error = new TelemetryWriteError({ dir: '/full', events: 1, cause });

    // `Error` defines `cause` as non-enumerable, and `Data.TaggedError` inherits
    // that — which is why every payload reader misses it unless it asks by name.
    expect(Object.keys(error)).not.toContain('cause');
    expect(causeOf(error)).toEqual(Option.some(cause));
    expect(causeOf(new RangeError('nope'))).toEqual(Option.none());
    expect(causeOf('a bare string')).toEqual(Option.none());
  });

  test('renderValue produces a string for anything a log line can carry', () => {
    expect(renderValue('text')).toBe('text');
    expect(renderValue(42)).toBe('42');
    expect(renderValue(9007199254740993n)).toBe('9007199254740993');
    expect(renderValue(true)).toBe('true');
    expect(renderValue(null)).toBe('null');
    expect(renderValue(undefined)).toBe('undefined');
    expect(renderValue(new RangeError('nope'))).toBe('RangeError: nope');
    expect(renderValue({ a: 1 })).toBe('{"a":1}');
    expect(renderValue([1, 2])).toBe('[1,2]');
    expect(renderValue(Symbol('s'))).toBe('Symbol(s)');
  });

  test('errorFields extracts the payload and drops the tag', () => {
    expect(errorFields(new DecodeFailed({ path: ['manifest', 'gameId'] }))).toEqual({
      path: ['manifest', 'gameId'],
    });
    // `Error`'s own message/name/stack are non-enumerable, so nothing is duplicated.
    expect(errorFields(new RangeError('nope'))).toEqual({});
    expect(errorFields('a bare string')).toEqual({});
    expect(errorFields(null)).toEqual({});
  });
});

describe('toEvlogError', () => {
  test('a registered tag carries its remedy, its payload and its cause', () => {
    const original = new DecodeFailed({ path: ['manifest', 'gameId'] });
    const mapped = toEvlogError(original);

    expect(mapped.code).toBe('WireDecodeError');
    expect(mapped.why).toBe(REMEDIES['WireDecodeError']?.why);
    expect(mapped.fix).toBe(REMEDIES['WireDecodeError']?.fix);
    expect(mapped.cause).toBe(original);
    expect(mapped.internal).toEqual({
      tag: 'WireDecodeError',
      registeredRemedy: true,
      fields: { path: ['manifest', 'gameId'] },
    });
  });

  test('an unregistered tag says so in the event itself', () => {
    const mapped = toEvlogError(new NeverHeardOfIt({ detail: 'x' }));
    expect(mapped.internal).toMatchObject({ registeredRemedy: false });
    expect(mapped.why).toContain('No remedy is registered');
  });

  test('an EvlogError passes through untouched', () => {
    // Its author already chose a remedy; replacing it with a lookup on the tag
    // `EvlogError` would be strictly less information.
    const original = createError({
      message: 'upstream said no',
      code: 'GATEWAY_REFUSED',
      why: 'the gateway rejected the manifest',
      fix: 're-run the recorder',
    });
    expect(toEvlogError(original)).toBe(original);
  });

  test('a non-Error failure still produces a remedy', () => {
    // Something rejected with a raw value. The corpus should still say what to do.
    const mapped = toEvlogError({ nope: true });
    expect(mapped.code).toBe('Untagged');
    expect(mapped.message).toBe('non-Error failure of type object');
    expect(mapped.why).toContain('No remedy is registered');
    expect(mapped.cause).toBeUndefined();
    expect(mapped.internal).toMatchObject({ fields: { nope: true } });
  });

  test('the remedy strings are backend-only and say so by naming internals', () => {
    // This is the assertion that documents the policy: `why`/`fix` name files
    // and modules, so they belong in a corpus and never in a response body.
    expect(REMEDIES['TelemetryEmitError']?.why).toContain('JSON.stringify');
    expect(remedyFor('Whatever').fix).toContain('arena/telemetry/src/error-map.ts');
  });
});
