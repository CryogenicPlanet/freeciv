/**
 * The **one** body normalization the parity matrix is allowed to perform, and
 * the argument for why it is the only one.
 *
 * Every other body in the matrix is compared **byte for byte** — a JSON
 * document, a PNG, CPython's stdlib HTML error page, all of it.  That is the
 * point: the two gateways serialize through different writers, and a rig that
 * compared parsed documents would not notice `1.0` where Python wrote `1`,
 * which is the exact four-byte defect `test/gateway/smoke-live.test.ts` was
 * written to catch.  So the default rule here is `bytes`, and it has no
 * options.
 *
 * ## Why `/health` needs an exception at all
 *
 * `identity_payload()` (`agent_eval/replay_gateway.py:1301`) reports facts
 * about **the process answering**, not about the port: its pid, the port the
 * kernel handed *it*, the self-URL built from that port, the cache root it was
 * given, and the identity digest derived from that cache root.  Two processes
 * cannot agree on those and remain two processes.  Everything else in the
 * payload — `schema_version`, `ok`, `kind`, `protocol_version`, `host`,
 * `repo_root`, `upstream_service_url`, `runs_root`, `viewer_public_url` —
 * comes from flags both gateways were handed identically by `boot.ts`, so it
 * is compared verbatim.
 *
 * ## Textual substitution, not structural comparison
 *
 * {@link normalizeHealthBody} rewrites the *value token* of each volatile
 * field in the raw response text and leaves every other byte alone.  It does
 * **not** parse and re-serialize, because a `JSON.parse` round trip is exactly
 * the machinery that hides a spelling difference: key order, `1` versus `1.0`,
 * `\/` versus `/` would all be normalized away for free.  After substitution
 * the two texts are compared with the same byte rule as everything else.
 *
 * The substitution is a flat-object regex, and it is safe *because* the payload
 * is flat: `identity_payload` is a `dict[str, str | int | bool]` with no nested
 * object or array in it (`src/gateway/http/routes/health.ts:75-93`).  A nested
 * value would need a real parser, and the assertion in
 * {@link healthPayloadIsFlat} is what keeps that assumption from rotting
 * silently.
 *
 * ## The normalization is itself under test
 *
 * Every field below must **actually differ** between the two processes.  A
 * normalization that no longer hides anything is a normalization that is
 * quietly weakening the comparison, so {@link compareHealthBodies} reports
 * `VolatileFieldAgrees` for it and the matrix fails.  The inverse — a *sixth*
 * field drifting apart — surfaces as an ordinary `Differ`, because the
 * post-substitution comparison is still byte-exact.
 *
 * @module
 */

import { Either } from 'effect';
import { bodyLatin1, type WireResponse } from './wire-client.ts';

// ---------------------------------------------------------------------------
// The volatile field list
// ---------------------------------------------------------------------------

/** One `/health` field that two processes cannot agree on, and why. */
export interface VolatileHealthField {
  /** The key as it appears in the canonical body. */
  readonly field: string;
  /** Why two correct implementations must disagree here. */
  readonly why: string;
}

/**
 * The five fields — and *only* these five — that the matrix rewrites.
 *
 * Measured, not assumed: `test/parity/diff.test.ts` asserts that the set of
 * `/health` keys whose values differ is exactly this set, in both directions.
 */
export const VOLATILE_HEALTH_FIELDS: ReadonlyArray<VolatileHealthField> = [
  {
    field: 'pid',
    why: '`os.getpid()` (`:1310`). Two processes, two pids; there is no configuration that could make them equal.',
  },
  {
    field: 'port',
    why: 'the **bound** port, read off the listening socket after `--port 0` (`:1312`). The kernel hands each process a different one, and `boot.ts` asserts they differ.',
  },
  {
    field: 'url',
    why: '`gatewaySelfUrl(host, boundPort)` — the same divergence as `port`, spelled as an origin. It is the field that would otherwise leak the port into every archive body, which is why the matrix pins `--viewer-public-url` instead of normalizing those.',
  },
  {
    field: 'cache_root',
    why: '`--cache-root`, one of the two flags `boot.ts` deliberately gives different values (`SLOT_SCOPED_FLAGS`): a shared derivation cache would let whichever gateway ran first answer for the other, and the rig would report parity on a body only one implementation ever produced.',
  },
  {
    field: 'identity',
    why: 'the 20-hex digest over the resolved configuration, which includes `cache_root` (`config.ts:873`). It differs *because* `cache_root` differs — a derived divergence, listed separately because it is a distinct key on the wire and the substitution has to name it.',
  },
];

/**
 * `host` is deliberately **not** here.
 *
 * It is `--host 127.0.0.1`, spelled once in `boot.ts` and handed to both
 * processes, so it is byte-identical and normalizing it would be dead weight
 * that silently widens the exemption.  The same argument covers `repo_root`,
 * `runs_root`, `upstream_service_url` and `viewer_public_url`.
 */
export const NON_VOLATILE_HEALTH_FIELDS: ReadonlyArray<string> = [
  'schema_version',
  'ok',
  'kind',
  'protocol_version',
  'host',
  'repo_root',
  'upstream_service_url',
  'runs_root',
];

/** The placeholder a volatile value is replaced with. */
export const VOLATILE_PLACEHOLDER = '<volatile>';

// ---------------------------------------------------------------------------
// Substitution
// ---------------------------------------------------------------------------

/**
 * `"field":<value>` where `<value>` is a JSON string, or a run of bytes up to
 * the next `,` or `}`.
 *
 * Correct only for a flat object — see {@link healthPayloadIsFlat}, which is
 * how the matrix keeps that precondition honest.  Anchored on the quoted key so
 * a *value* that happens to contain `"pid":` (a cache root under a directory
 * named that, say) cannot be mistaken for the key.
 */
const fieldPattern = (field: string): RegExp =>
  new RegExp(`("${field}":)("(?:[^"\\\\]|\\\\.)*"|[^,}]*)`);

/** A `/health` body with its volatile values replaced, and what they were. */
export interface NormalizedHealthBody {
  /** The response text, byte-identical except for the replaced value tokens. */
  readonly text: string;
  /** field → the value token that was replaced; `null` when the key was absent. */
  readonly replaced: Readonly<Record<string, string | null>>;
}

/**
 * Rewrite the volatile values out of one `/health` body.
 *
 * Total: a field the body does not carry is recorded as `null` rather than
 * treated as an error, so an implementation that *dropped* `pid` shows up as a
 * missing field in {@link compareHealthBodies} instead of as a thrown parse.
 */
export const normalizeHealthBody = (text: string): NormalizedHealthBody =>
  VOLATILE_HEALTH_FIELDS.reduce<NormalizedHealthBody>(
    (accumulated, { field }) => {
      const match = fieldPattern(field).exec(accumulated.text);
      return {
        text:
          match === null
            ? accumulated.text
            : accumulated.text.replace(fieldPattern(field), `$1${VOLATILE_PLACEHOLDER}`),
        replaced: { ...accumulated.replaced, [field]: match?.[2] ?? null },
      };
    },
    { text, replaced: {} },
  );

/**
 * `JSON.parse` as a value.
 *
 * `JSON.parse` is the one stdlib boundary in this file that reports failure by
 * throwing, and a body that does not parse is an *expected* input here — the
 * `/health?x=1` rejection is a problem body and takes the byte rule — so
 * `Either.try` converts the throw at the boundary and nothing downstream has to
 * know it was ever an exception.
 */
const jsonOrNull = (text: string): unknown =>
  Either.getOrNull(Either.try(() => JSON.parse(text) as unknown));

/**
 * True when `text` is a JSON object with no nested object or array — the
 * precondition {@link normalizeHealthBody}'s regex rests on.
 *
 * Deliberately structural rather than a schema check: the question is not
 * "is this a valid identity payload" (the byte comparison answers that) but
 * "could a value hide a `,` or a `}` from a flat-object regex".  Total: a body
 * that does not parse at all is `false`, which is the conservative answer.
 */
export const healthPayloadIsFlat = (text: string): boolean => {
  const parsed: unknown = jsonOrNull(text);
  return (
    typeof parsed === 'object' &&
    parsed !== null &&
    !Array.isArray(parsed) &&
    Object.values(parsed).every((value) => typeof value !== 'object' || value === null)
  );
};

// ---------------------------------------------------------------------------
// The verdict
// ---------------------------------------------------------------------------

/** Which rule decided a body comparison. */
export type BodyRule = 'bytes' | 'health';

/** The outcome of comparing two bodies.  Every arm is a value; nothing throws. */
export type BodyVerdict =
  /** The bodies are equal under {@link BodyVerdict.rule}. */
  | { readonly _tag: 'Equal'; readonly rule: BodyRule }
  /** They are not.  `python`/`typescript` are the compared forms, not the raw ones. */
  | {
      readonly _tag: 'Differ';
      readonly rule: BodyRule;
      readonly python: string;
      readonly typescript: string;
    }
  /** A volatile field was absent from one side — a bigger finding than a value difference. */
  | {
      readonly _tag: 'VolatileFieldMissing';
      readonly field: string;
      readonly side: 'python' | 'typescript';
    }
  /**
   * A field on the volatile list turned out to be **identical**.
   *
   * The dead-normalization alarm: the exemption is no longer buying anything,
   * so it must be deleted rather than left to widen the comparison for free.
   */
  | { readonly _tag: 'VolatileFieldAgrees'; readonly field: string; readonly value: string }
  /** A `/health` body that is not a flat JSON object — the regex's precondition. */
  | {
      readonly _tag: 'HealthPayloadNotFlat';
      readonly side: 'python' | 'typescript';
      readonly preview: string;
    };

/** The body as one comparable latin-1 string — lossless for PNG and mp4 alike. */
export const comparableBody = (response: WireResponse): string => bodyLatin1(response);

/**
 * Compare two `/health` bodies: substitute the volatile values, then compare
 * the remaining bytes exactly.
 *
 * Three failure modes are reported before the byte comparison, because each is
 * a stronger statement than "the bodies differ": a payload that is not flat
 * (the regex cannot be trusted), a volatile field one side does not send, and a
 * volatile field that no longer differs.
 */
export const compareHealthBodies = (python: string, typescript: string): BodyVerdict => {
  const flatness: ReadonlyArray<BodyVerdict> = (
    [
      ['python', python],
      ['typescript', typescript],
    ] as const
  ).flatMap(([side, text]) =>
    healthPayloadIsFlat(text)
      ? []
      : [{ _tag: 'HealthPayloadNotFlat', side, preview: text.slice(0, 200) } as const],
  );
  const firstFlatness = flatness[0];
  if (firstFlatness !== undefined) return firstFlatness;

  const pythonSide = normalizeHealthBody(python);
  const typescriptSide = normalizeHealthBody(typescript);

  const problems: ReadonlyArray<BodyVerdict> = VOLATILE_HEALTH_FIELDS.flatMap(
    ({ field }): ReadonlyArray<BodyVerdict> => {
      const left = pythonSide.replaced[field] ?? null;
      const right = typescriptSide.replaced[field] ?? null;
      if (left === null) return [{ _tag: 'VolatileFieldMissing', field, side: 'python' }];
      if (right === null) return [{ _tag: 'VolatileFieldMissing', field, side: 'typescript' }];
      return left === right ? [{ _tag: 'VolatileFieldAgrees', field, value: left }] : [];
    },
  );
  const firstProblem = problems[0];
  if (firstProblem !== undefined) return firstProblem;

  return pythonSide.text === typescriptSide.text
    ? { _tag: 'Equal', rule: 'health' }
    : {
        _tag: 'Differ',
        rule: 'health',
        python: pythonSide.text,
        typescript: typescriptSide.text,
      };
};

/**
 * The matrix's body comparison, in one function.
 *
 * `rule` is the leg's declared rule, not a guess from the payload: a
 * `/health` leg that answered `400 {"error":…}` (the `?x=1` rejection) has no
 * volatile fields in it at all, so the health rule degrades to the byte rule
 * rather than reporting five missing fields.
 */
export const compareBodies = (
  rule: BodyRule,
  python: WireResponse,
  typescript: WireResponse,
): BodyVerdict => {
  const left = comparableBody(python);
  const right = comparableBody(typescript);
  if (rule === 'health' && python.status === 200 && typescript.status === 200) {
    return compareHealthBodies(left, right);
  }
  return left === right
    ? { _tag: 'Equal', rule: 'bytes' }
    : { _tag: 'Differ', rule: 'bytes', python: left, typescript: right };
};

/** A one-line rendering of a verdict, for the evidence table. */
export const describeBodyVerdict = (verdict: BodyVerdict): string => {
  switch (verdict._tag) {
    case 'Equal':
      return `equal(${verdict.rule})`;
    case 'Differ':
      return `DIFFER(${verdict.rule}) py=${String(verdict.python.length)}B ts=${String(verdict.typescript.length)}B`;
    case 'VolatileFieldMissing':
      return `MISSING ${verdict.field} on ${verdict.side}`;
    case 'VolatileFieldAgrees':
      return `DEAD NORMALIZATION ${verdict.field}`;
    default:
      return `NOT FLAT on ${verdict.side}`;
  }
};
