/** Python-number JSON parsing and canonical round-trip coverage. */
import { describe, expect, test } from 'bun:test';
import { CANON_UTF8, canonicalText } from '@arena/wire';
import { Either } from 'effect';
import {
  isCanonValue,
  parsePythonJson,
  parsePythonJsonObject,
  rewriteLiterals,
} from 'src/gateway/python-json.ts';

// ---------------------------------------------------------------------------
// The oracle
// ---------------------------------------------------------------------------

const ORACLE = [
  'import json, sys',
  'text = sys.stdin.buffer.read().decode("utf-8")',
  'try:',
  '    value = json.loads(text)',
  'except (ValueError, UnicodeError) as exc:',
  '    sys.stdout.write("ERR")',
  '    raise SystemExit(0)',
  'sys.stdout.write(json.dumps(',
  '    value, sort_keys=True, separators=(",", ":"), ensure_ascii=False))',
].join('\n');

const PYTHON_AVAILABLE = Bun.spawnSync(['python3', '-c', 'import sys']).exitCode === 0;

/** `json.dumps(json.loads(text), …)`, or `ERR` when CPython refuses the text. */
const oracle = (text: string): string => {
  const child = Bun.spawnSync(['python3', '-c', ORACLE], {
    stdin: Buffer.from(text, 'utf-8'),
    stdout: 'pipe',
    stderr: 'pipe',
  });
  if (child.exitCode !== 0) throw new Error(`oracle failed: ${child.stderr.toString()}`);
  return child.stdout.toString();
};

const ours = (text: string): string =>
  Either.match(parsePythonJson(text), {
    onLeft: () => 'ERR',
    onRight: (value) =>
      Either.match(canonicalText(value, CANON_UTF8), {
        onLeft: () => 'CANON-ERR',
        onRight: (rendered) => rendered,
      }),
  });

// ---------------------------------------------------------------------------
// The corpus
// ---------------------------------------------------------------------------

/**
 * Every input both readers are asked about.
 *
 * The first block is the one this module exists for; the rest are the places a
 * rewrite-then-`JSON.parse` reader could plausibly go wrong.
 */
const CORPUS: readonly string[] = [
  // --- int vs float, the whole point --------------------------------------
  '{"schema_version":1,"next_after_turn":0,"available":false,"snapshots":[]}',
  '{"a":1,"b":1.0}',
  '{"a":-1,"b":-1.0}',
  '{"a":0,"b":0.0,"c":-0,"d":-0.0}',
  '{"e":1e2,"f":1E2,"g":1e-5,"h":1E+16,"i":1.5e3}',
  '{"big":12345678901234567890,"bigger":123456789012345678901234567890}',
  '{"negbig":-99999999999999999999}',
  '{"nested":{"deep":[1,2.0,{"x":3,"y":4.25}]}}',
  '[1,2,3]',
  '[1.0,2.0]',
  '1',
  '1.0',
  '-0',
  '12345678901234567890',
  // --- everything else that has a spelling --------------------------------
  'null',
  'true',
  'false',
  '"top level string"',
  '{}',
  '[]',
  '{"":1}',
  '{"nul":"\\u0000","bell":"\\u0007"}',
  '{"quote":"a\\"b","backslash":"a\\\\b","solidus":"a\\/b"}',
  '{"tab":"a\\tb","newline":"a\\nb","cr":"a\\rb","ff":"a\\fb","bs":"a\\bb"}',
  '{"unicode":"caf\\u00e9","astral":"\\ud83d\\ude00","literal":"café 😀"}',
  '{"z":1,"a":2,"m":3}',
  '{"dup":1,"dup":2}',
  '  {  "pad" : 1 ,  "q" : [ 1 , 2 ]  }  ',
  '{"__proto__":1,"ok":2}',
  '{"constructor":1}',
  // --- the marker's own spelling, escaped -------------------------------
  '{"looks_marked":"\\u0000i5","also":"\\u0000sx","bare":"\\u0000"}',
  '["\\u0000i5",5,"\\u0000i5"]',
  // --- strings that contain things the tokenizer must not treat as tokens --
  '{"digits_in_string":"12345","float_in_string":"1.5","json_in_string":"{\\"a\\":1}"}',
  '{"colon_in_string":"a:b","quote_then_colon":"x\\":1"}',
  '{"key:with:colon":1}',
];

/** Texts CPython refuses, or that mean something CPython's `json` alone allows. */
const REFUSED: readonly string[] = [
  '',
  '   ',
  '{',
  '{"a":}',
  '{"a":1,}',
  '[1,]',
  '[1 2]',
  '{"a":01}',
  '{"a":+1}',
  '{"a":.5}',
  '{"a":1.}',
  "{'a':1}",
  '{"a":"unterminated}',
  '{"a":1}trailing',
  '{a:1}',
  'undefined',
  '{"a":1,"a"}',
];

// ---------------------------------------------------------------------------

describe.if(PYTHON_AVAILABLE)('differential against json.loads + json.dumps', () => {
  test.each([...CORPUS])('%p', (text) => {
    expect({ text, rendered: ours(text) }).toEqual({ text, rendered: oracle(text) });
  });
});

describe('refusals', () => {
  test.each([...REFUSED])('%p is refused, and JSON.parse refuses it too', (text) => {
    expect(Either.isLeft(parsePythonJson(text))).toBe(true);
    // The rewrite is only allowed to preserve refusal, never to create
    // acceptance: whatever `JSON.parse` said about the original it must still
    // say about the rewritten text.
    const rawAccepted = Either.isRight(Either.try(() => JSON.parse(text) as unknown));
    const rewrittenAccepted = Either.isRight(
      Either.try(() => JSON.parse(rewriteLiterals(text)) as unknown),
    );
    expect({ text, rawAccepted, rewrittenAccepted }).toEqual({
      text,
      rawAccepted: false,
      rewrittenAccepted: false,
    });
  });

  test.if(PYTHON_AVAILABLE)('CPython refuses the same malformed documents', () => {
    REFUSED.forEach((text) => {
      expect({ text, cpython: oracle(text) }).toEqual({ text, cpython: 'ERR' });
    });
  });
});

describe('the reader itself', () => {
  test('an integer literal is a bigint and a float literal is a number', () => {
    const value = Either.getOrThrowWith(
      parsePythonJsonObject('{"i":7,"f":7.0,"e":7e0,"neg":-7}'),
      (error) => new Error(error.message),
    );
    expect(typeof value['i']).toBe('bigint');
    expect(typeof value['f']).toBe('number');
    expect(typeof value['e']).toBe('number');
    expect(value['i']).toBe(7n);
    expect(value['neg']).toBe(-7n);
  });

  test('NaN and infinities are accepted with CPython values', () => {
    const value = Either.getOrThrowWith(
      parsePythonJsonObject('{"nan":NaN,"positive":Infinity,"negative":-Infinity}'),
      (error) => new Error(error.message),
    );
    expect(Number.isNaN(value['nan'])).toBe(true);
    expect(value['positive']).toBe(Number.POSITIVE_INFINITY);
    expect(value['negative']).toBe(Number.NEGATIVE_INFINITY);
  });

  test('a string that looks like the marker survives as a string', () => {
    const value = Either.getOrThrowWith(
      parsePythonJsonObject('{"a":"\\u0000i5","b":5}'),
      (error) => new Error(error.message),
    );
    expect(value['a']).toBe('\u0000i5');
    expect(value['b']).toBe(5n);
  });

  test('object keys are never rewritten, even when they look marked', () => {
    const value = Either.getOrThrowWith(
      parsePythonJsonObject('{"\\u0000i5":1,"12345":2}'),
      (error) => new Error(error.message),
    );
    expect(Object.keys(value).toSorted()).toEqual(['\u0000i5', '12345']);
  });

  test('a very large integer keeps every digit', () => {
    const text = `{"n":${'9'.repeat(80)}}`;
    const value = Either.getOrThrowWith(
      parsePythonJsonObject(text),
      (error) => new Error(error.message),
    );
    expect(value['n']).toBe(BigInt('9'.repeat(80)));
    expect(Either.getOrThrowWith(canonicalText(value, CANON_UTF8), () => new Error('canon'))).toBe(
      text,
    );
  });

  test('parsePythonJsonObject refuses a document that is not an object', () => {
    ['[]', '1', '"s"', 'null', 'true'].forEach((text) => {
      expect(Either.isLeft(parsePythonJsonObject(text))).toBe(true);
      expect(Either.isRight(parsePythonJson(text))).toBe(true);
    });
  });

  test('isCanonValue is the guard that replaces the cast', () => {
    expect(isCanonValue({ a: [1n, 2, 'x', null, true] })).toBe(true);
    expect(isCanonValue(undefined)).toBe(false);
    expect(isCanonValue({ a: undefined })).toBe(false);
    expect(isCanonValue([() => undefined])).toBe(false);
    expect(isCanonValue(Symbol('s'))).toBe(false);
  });
});
