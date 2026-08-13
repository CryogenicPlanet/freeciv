/**
 * `json.loads` with the int/float distinction intact.
 *
 * ## Why this module has to exist
 *
 * `@arena/wire`'s canonical writer spells a JavaScript `bigint` as a Python
 * `int` (`1`) and a JavaScript `number` as a Python `float` (`1.0`) — that is
 * the only way the two literals can round-trip distinctly (`canon.ts:48`).
 * `JSON.parse` erases the distinction: every numeric literal becomes a
 * `number`, so a document that arrived as `{"schema_version":1}` is re-emitted
 * as `{"schema_version":1.0}`.
 *
 * Three seams in this gateway cross a JSON *text* boundary that CPython does
 * not, and every one of them was wrong before this module:
 *
 * | Seam | What Python does | What `JSON.parse` did |
 * |---|---|---|
 * | the derivation bridge's stdout (`services/derivation.ts`) | hands the loader's `dict` straight to `_bounded_json` — no round trip at all | `1` → `1.0` on `replay.json` and `board.json` |
 * | the `/v1/games` upstream body (`http/routes/games.ts`) | `json.loads` then `json.dumps`, each literal as it found it | every integral `float` (`7.0`) re-emitted as `7` |
 * | `replay.jsonl`'s tail (`services/runs.ts`) | `isinstance(turn, int)` rejects `2.0` | accepted `2.0`, publishing a row Python hides |
 *
 * One reader, used at all three, closes all three.  Nothing else in the port
 * may re-derive the rule: an "is it integral?" guess over a `number` is
 * exactly the lossy step this exists to remove.
 *
 * ## How
 *
 * Not a hand-rolled parser.  A recursive-descent reader would have to own the
 * whole JSON grammar (and would recurse once per array element, which the
 * house style's "no `for(;;)`" makes awkward to avoid); instead the text is
 * rewritten so that `JSON.parse` — which already owns the grammar, the escape
 * rules and the error messages — carries the distinction through:
 *
 * ```
 * {"a":1,"b":1.0,"c":"x"}            the input
 * {"a":"\u0000i1","b":1.0,"c":"\u0000sx"}   after rewriteLiterals
 * {a: 1n,        b: 1,   c: "x"}     after JSON.parse with reviveMarked
 * ```
 *
 * Two details make that sound rather than merely clever:
 *
 * 1. **Every value string is marked too**, not just the integers.  A document
 *    may legally contain `"\u0000i5"` — U+0000 is illegal *raw* in a JSON
 *    string but perfectly legal *escaped* — and an unmarked value string would
 *    then be indistinguishable from a marked integer.  Marking all of them
 *    makes the marker's presence, not its absence, the thing that is trusted.
 *    Object **keys** are deliberately left alone: the reviver is never handed a
 *    key as a value, and rewriting one would rename the property.
 * 2. **The rewrite cannot turn invalid JSON into valid JSON.**  It only
 *    inserts escape sequences immediately after a value string's opening quote
 *    and replaces an integer literal with a quoted one; anything `JSON.parse`
 *    refused before is refused after, and the tests pin a table of malformed
 *    inputs to prove it.
 *
 * The result is validated by {@link isCanonValue} rather than cast: `JSON.parse`
 * is typed `any`, and this module contains no assertion the compiler is asked
 * to take on trust.
 *
 * Python's three non-standard numeric tokens (`NaN`, `Infinity` and
 * `-Infinity`) are accepted too.  They remain JavaScript numbers so the public
 * projections can apply the same finite-number checks as Python. If one is
 * relayed unchanged, `@arena/wire` still refuses it under its repository-wide
 * `allow_nan=False` contract, whereas this Python gateway's default
 * `json.dumps` would emit the non-standard token. Widening that wire contract
 * is intentionally outside this parser and remains the one non-finite gap.
 *
 * @module
 */

import type { CanonRecord, CanonValue } from '@arena/wire';
import { Data, Either } from 'effect';

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
const reviveMarked = (_key: string, value: unknown): unknown => {
  if (typeof value !== 'string' || !value.startsWith(MARKER)) return value;
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
export const isCanonRecord = (value: unknown): value is CanonRecord =>
  typeof value === 'object' && value !== null && !Array.isArray(value) && isCanonValue(value);

/**
 * Every runtime shape {@link CanonValue} admits.
 *
 * Written as a guard rather than an assertion because `JSON.parse` is typed
 * `any`: this is the one place the port turns "the reader can only have
 * produced these" into something the compiler has actually been shown.
 * Recursion depth is the document's, which `JSON.parse` has already bounded by
 * failing on anything deeper than its own parser can hold.
 */
export const isCanonValue = (value: unknown): value is CanonValue => {
  if (value === null) return true;
  switch (typeof value) {
    case 'boolean':
    case 'bigint':
    case 'number':
    case 'string':
      return true;
    case 'object':
      return Array.isArray(value)
        ? value.every(isCanonValue)
        : Object.values(value).every(isCanonValue);
    default:
      return false;
  }
};

// ---------------------------------------------------------------------------
// The reader
// ---------------------------------------------------------------------------

const NOT_CANON = 'the document contains a value with no canonical spelling';

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
      try: (): unknown => JSON.parse(rewriteLiterals(text), reviveMarked) as unknown,
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
