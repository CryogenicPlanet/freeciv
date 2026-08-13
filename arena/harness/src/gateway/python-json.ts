/**
 * JSON decoding that preserves CPython numbers: integer tokens become `bigint`, float tokens remain
 * `number`. This is required before relayed loader documents are canonically re-encoded.
 */

import type { CanonRecord, CanonValue } from '@arena/wire';
import { Predicate, Data, Either } from 'effect';

/**
 * The text was not JSON, or was JSON this reader will not produce a
 * {@link CanonValue} for.
 *
 * `message` is `JSON.parse`'s own diagnostic and is **private**: it can quote
 * the offending text, and the gateway's rule is that a loader's or an
 * upstream's text never reaches a response body.
 */
export class PythonJsonError extends Data.TaggedError('PythonJson')<{
  readonly message: string;
  readonly cause: unknown;
}> {}

// ---------------------------------------------------------------------------
// The marker
// ---------------------------------------------------------------------------

/**
 * U+0000, written as the six characters `\u0000` — the form a JSON *text* must
 * use, since a raw NUL is not a legal string character.
 */
const MARKER_ESCAPE = '\\u0000';

/** The decoded marker: one NUL, which is what `JSON.parse` hands the reviver. */
const MARKER = '\u0000';

/** `\u0000i` — the rest of the string is a decimal integer literal. */
const MARK_INT = 'i';

/** `\u0000s` — the rest of the string is the value the document actually had. */
const MARK_STRING = 's';

/** `\u0000f` — the rest is one of CPython JSON's non-finite float tokens. */
const MARK_NONFINITE = 'f';

// ---------------------------------------------------------------------------
// The rewrite
// ---------------------------------------------------------------------------

/**
 * One token: an object **key** (a string followed by `:`), a value string, or
 * a number.
 *
 * The key alternative comes first because alternation is ordered and a key is
 * also a string; without it the scan would step *into* the key and rewrite a
 * digit inside it.  The string body is deliberately permissive
 * (`[^"\\]|\\.`) — validation is `JSON.parse`'s job, and over-accepting here
 * can only leave a malformed document malformed.
 */
const JSON_TOKEN_RE =
  /"(?:[^"\\]|\\[\s\S])*"[ \t\n\r]*:|"(?:[^"\\]|\\[\s\S])*"|-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?|-?Infinity|NaN/g;

/** True for a numeric literal CPython's scanner hands to `parse_float`. */
const isFloatLiteral = (token: string): boolean =>
  token.includes('.') || token.includes('e') || token.includes('E');

const rewriteToken = (token: string): string => {
  if (token.startsWith('"')) {
    // A key keeps its bytes: the reviver never sees one, and renaming it would
    // change the document.
    return token.endsWith(':')
      ? token
      : `"${MARKER_ESCAPE}${MARK_STRING}${token.slice(1)}`;
  }
  return token === 'NaN' || token === 'Infinity' || token === '-Infinity'
    ? `"${MARKER_ESCAPE}${MARK_NONFINITE}${token}"`
    : isFloatLiteral(token)
      ? token
      : `"${MARKER_ESCAPE}${MARK_INT}${token}"`;
};

/**
 * The input with every integer literal and every value string marked.
 *
 * Exported for the tests that prove the rewrite is meaning-preserving on valid
 * input and meaning-refusing on invalid input; nothing else should call it.
 */
export const rewriteLiterals = (text: string): string =>
  text.replace(JSON_TOKEN_RE, rewriteToken);

// ---------------------------------------------------------------------------
// The reviver
// ---------------------------------------------------------------------------

/**
 * Undo the marking: a marked integer becomes a `bigint`, a marked string
 * becomes itself.
 *
 * A string that is *not* marked cannot occur — {@link rewriteLiterals} marks
 * every value string — so the final arm is unreachable padding that keeps the
 * function total rather than partial.
 */
const reviveMarked = <Value>(
  _key: string,
  value: Value,
): Value | bigint | number | string => {
  if (!Predicate.isString(value) || !value.startsWith(MARKER)) return value;
  const kind = value.charAt(MARKER.length);
  const rest = value.slice(MARKER.length + 1);
  return kind === MARK_INT
    ? BigInt(rest)
    : kind === MARK_STRING
      ? rest
      : kind === MARK_NONFINITE
        ? Number(rest)
        : value;
};

// ---------------------------------------------------------------------------
// The guard
// ---------------------------------------------------------------------------

/**
 * A JSON object as the canonical writer models it: string keys, canonical
 * values, never an array.
 *
 * `@arena/wire`'s `isJsonObject` cannot be used for this — its value type has
 * no `bigint` member, so it refuses exactly the documents this reader exists
 * to produce.
 */
export const isCanonRecord = <Value>(value: Value): value is Value & CanonRecord =>
  Predicate.isRecord(value) && isCanonValue(value);

/**
 * Every runtime shape {@link CanonValue} admits.
 *
 * Written as a guard rather than an assertion because `JSON.parse` is typed
 * `any`: this is the one place the port turns "the reader can only have
 * produced these" into something the compiler has actually been shown.
 * Recursion depth is the document's, which `JSON.parse` has already bounded by
 * failing on anything deeper than its own parser can hold.
 */
export const isCanonValue = <Value>(value: Value): value is Value & CanonValue => {
  if (
    value === null ||
    Predicate.isBoolean(value) ||
    Predicate.isBigInt(value) ||
    Predicate.isNumber(value) ||
    Predicate.isString(value)
  ) {
    return true;
  }
  if (Array.isArray(value)) return value.every(isCanonValue);
  return Predicate.isRecord(value) && Object.values(value).every(isCanonValue);
};

// ---------------------------------------------------------------------------
// The reader
// ---------------------------------------------------------------------------

const NOT_CANON = 'the document contains a value with no canonical spelling';
const INVALID_CANON = Symbol('invalid canonical JSON');

const failure = (cause: unknown): PythonJsonError =>
  new PythonJsonError({
    message: cause instanceof Error ? cause.message : String(cause),
    cause,
  });

/**
 * `json.loads(text)` with CPython's int/float distinction preserved.
 *
 * An integer literal — no fraction, no exponent — comes back as a `bigint`;
 * everything else numeric comes back as a `number`.  The result is ready for
 * `canonicalBytes`, which is the whole point: `1` in gives `1` out.
 */
export const parsePythonJson = (text: string): Either.Either<CanonValue, PythonJsonError> =>
  Either.flatMap(
    Either.try({
      try: (): CanonValue | typeof INVALID_CANON => {
        const parsed: unknown = JSON.parse(rewriteLiterals(text), reviveMarked);
        return isCanonValue(parsed) ? parsed : INVALID_CANON;
      },
      catch: failure,
    }),
    (value) =>
      isCanonValue(value)
        ? Either.right(value)
        : Either.left(new PythonJsonError({ message: NOT_CANON, cause: null })),
  );

/** {@link parsePythonJson}, refusing anything that is not a JSON **object**. */
export const parsePythonJsonObject = (
  text: string,
): Either.Either<CanonRecord, PythonJsonError> =>
  Either.flatMap(parsePythonJson(text), (value) =>
    isCanonRecord(value)
      ? Either.right(value)
      : Either.left(new PythonJsonError({ message: NOT_CANON, cause: null })),
  );
