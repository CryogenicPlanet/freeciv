/**
 * The matrix's only body normalization. `/health` process identity fields
 * cannot match across two processes, so their raw value tokens are replaced
 * without parsing or reserializing; every other byte remains exact.
 *
 * The payload must remain flat, every listed volatile field must exist and
 * differ, and any additional difference fails. This makes the exemption
 * self-invalidating rather than a permanent comparison hole.
 */

import { isFlatJsonObject, parseJsonValueFromText } from './json-boundary.ts';
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

/** The five process-specific fields, and only these five. */
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

/** Shared flag-derived fields remain exact rather than normalized. */
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

/** Match one value token in a flat object, anchored on its quoted key. */
const fieldPattern = (field: string): RegExp =>
  new RegExp(`("${field}":)("(?:[^"\\\\]|\\\\.)*"|[^,}]*)`);

/** A `/health` body with its volatile values replaced, and what they were. */
export interface NormalizedHealthBody {
  /** The response text, byte-identical except for the replaced value tokens. */
  readonly text: string;
  /** field → the value token that was replaced; `null` when the key was absent. */
  readonly replaced: Readonly<Record<string, string | null>>;
}

/** Replace volatile values; missing fields are recorded as `null`. */
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

/** Validate the flat-object precondition required by textual substitution. */
export const healthPayloadIsFlat = (text: string): boolean => {
  const parsed = parseJsonValueFromText(text);
  return parsed !== null && isFlatJsonObject(parsed);
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

/** Substitute volatile tokens, validate the exemption, then compare bytes. */
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

/** Apply health normalization only to successful declared health legs. */
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
