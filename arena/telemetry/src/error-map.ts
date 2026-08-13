/**
 * Every refusal names its remedy.
 *
 * A `Data.TaggedError` carries a tag and whatever fields its author chose.  That
 * is enough to *branch* on and nowhere near enough to *act* on: six months later
 * a corpus row reading `{"_tag":"MaxDepthExceeded"}` tells the reader that
 * something nested too deep and nothing about what to do next.  This module is
 * the table that closes that gap — tag in, `{ why, fix }` out — and the mapping
 * happens at the moment the failure is observed, not when someone eventually
 * reads the file.
 *
 * ## Telemetry-only. Never a response body.
 *
 * The output is an `EvlogError`, which is an HTTP-shaped type: it has a
 * `status` (500 unless set) and a `toJSON()` that would serialize cleanly into
 * a response.  **Nothing in this package ever puts one on a wire.**  These
 * values exist to be written into the NDJSON corpus, where the audience is a
 * developer reading their own logs.  `why` and `fix` are written for that
 * reader; they name internal modules and file paths, which is exactly what you
 * do not want in a response body.  The `status` field is an artifact of the
 * shape and means nothing here.
 *
 * `internal` is likewise backend-only by construction: `evlog` excludes it from
 * `toJSON()` and from HTTP responses, but *does* copy it into the wide event.
 * That asymmetry is the reason it is used here to record the tag and whether
 * the remedy was looked up or fabricated.
 *
 * ## Adding a tag
 *
 * Add it to {@link REMEDIES}.  Failing to is not fatal — {@link remedyFor}
 * returns a fallback — but the fallback is deliberately unflattering, because
 * an honest "nobody wrote a remedy for this" is worth more than a plausible
 * invented one, and it names its own remedy: add the tag.
 *
 * @module
 */

import { Option } from 'effect';
import { createError, EvlogError } from 'evlog';

/** What went wrong underneath, and what the reader should do about it. */
export interface Remedy {
  /** Why this failure happens — the mechanism, not a restatement of the message. */
  readonly why: string;
  /** What to do next. An instruction, addressed to whoever is reading the event. */
  readonly fix: string;
}

/**
 * The tag table.
 *
 * Deliberately a plain record rather than a closed union: this package sits
 * *below* the modules whose errors it describes and must not import them, so
 * the keys are their tags spelled out. `@arena/wire`'s entries are transcribed
 * from `arena/wire/src/canon.ts` and `arena/wire/src/codec.ts`; the
 * `Telemetry*` entries are this package's own, from `./evlog-adapter.ts`.
 */
export const REMEDIES: Readonly<Record<string, Remedy>> = {
  // --- @arena/wire: decoding and encoding -----------------------------------
  WireDecodeError: {
    why: 'A value crossing the wire boundary did not match its schema — the producer and this consumer disagree about the shape.',
    fix: 'Read the schema path on the event, compare it against the Python side that produced the value, and fix whichever one drifted. Do not widen the schema to make the error go away.',
  },
  WireEncodeError: {
    why: 'A value this process built could not be encoded into its wire form, so the schema and the constructed value disagree.',
    fix: 'Fix the construction site named in the event. An encode failure is always a bug on this side of the boundary.',
  },
  // --- @arena/wire: canonical JSON ------------------------------------------
  NonFiniteFloat: {
    why: 'Canonical JSON has no spelling for NaN or Infinity, so a hash over these bytes could not be reproduced by CPython.',
    fix: 'Find the arithmetic that produced the non-finite value — division by a zero denominator is the usual source — and decide what the field should be when the input is empty.',
  },
  LoneSurrogate: {
    why: 'A string held an unpaired UTF-16 surrogate, which has no UTF-8 encoding, so the canonical bytes are undefined.',
    fix: 'Trace the string to its origin: a lone surrogate almost always means a byte buffer was sliced mid-character or a fixed-width truncation cut through an astral character.',
  },
  MaxDepthExceeded: {
    why: 'Canonicalization is depth-bounded so that a cyclic or pathological structure fails fast instead of exhausting the stack.',
    fix: 'Check the value for a cycle first. If it is genuinely that deep, the shape is the problem — flatten it rather than raising the bound.',
  },
  UnsupportedValue: {
    why: 'The value has no canonical JSON spelling — a function, a symbol, a class instance, or undefined reached the encoder.',
    fix: 'Project the value into plain JSON at the boundary where it is created. The encoder deliberately refuses to guess.',
  },
  // --- this package ---------------------------------------------------------
  TelemetryInitError: {
    why: "evlog's process-global logger state could not be initialized, so no event in this process would carry a service identity.",
    fix: 'Check that initLogger is called exactly once, from a single layer acquire. A second call overwrites every field for every copy of evlog in the process.',
  },
  TelemetryEmitError: {
    why: "evlog's emit() serializes the event with an unguarded JSON.stringify, which throws on a circular reference, a BigInt, or a toJSON() that throws.",
    fix: 'Find the annotate() call that attached the offending value and project it to plain JSON there — a BigInt to a string, a graph to ids.',
  },
  TelemetryWriteError: {
    why: 'The sealed event could not be appended to the NDJSON corpus, so that unit of work left no record.',
    fix: 'Check the run directory exists and is writable, and that the volume has space. This is the only failure path that reports a lost event at all — the drain-based alternative swallows it.',
  },
  TelemetryExportError: {
    why: 'The OTLP mirror rejected or timed out after its retries. The NDJSON corpus is unaffected — it is written first, and it is the record of truth.',
    fix: 'Check the collector endpoint and its auth headers. If the collector is not meant to be running, leave otlpEndpoint unset rather than pointing it at a dead address.',
  },
  WideEventAlreadySealed: {
    why: 'Something tried to seal a wide event that had already been sealed and emitted, which means two owners believed they were finishing the same unit of work.',
    fix: 'Find the second sealer. withWideEvent seals exactly once from its onExit finalizer; a second seal means an event was passed somewhere it should not have been.',
  },
  UnreadableFailure: {
    why: 'The failure value could not be described: reading its tag, its message or its fields threw, which means one of its property getters is hostile or its prototype is.',
    fix: 'Find the code that constructed the failure value and make it a plain tagged error. A value that cannot be read cannot be recorded, and this note is standing in for it.',
  },
};

/** The tag {@link REMEDIES} registers for a failure value that could not be read at all. */
export const UNREADABLE_FAILURE_TAG = 'UnreadableFailure';

/**
 * The remedy for `tag`, or an honest fallback that names its own remedy.
 *
 * The fallback is not a stand-in for a real entry: it says plainly that nobody
 * wrote one, which is a true and actionable statement, where an invented
 * plausible remedy would be neither.
 */
export const remedyFor = (tag: string): Remedy =>
  REMEDIES[tag] ?? {
    why: `No remedy is registered for "${tag}", so this event carries the failure without an explanation of it.`,
    fix: `Add "${tag}" to REMEDIES in arena/telemetry/src/error-map.ts, so the next reader of this event is told what to do instead of finding this note.`,
  };

/** True when {@link REMEDIES} has a hand-written entry for `tag`. */
export const isRegisteredTag = (tag: string): boolean => Object.hasOwn(REMEDIES, tag);

/**
 * `'_tag' in value` narrows `value` to `object & Record<'_tag', unknown>`, so
 * the property read below needs no cast — which is the point: a package whose
 * job is describing other people's failure values has no business asserting
 * anything about their shape.
 */
const isTagged = (value: unknown): value is { readonly _tag: string } =>
  typeof value === 'object' &&
  value !== null &&
  '_tag' in value &&
  typeof value._tag === 'string';

/**
 * The tag of a failure value.
 *
 * `Data.TaggedError` and every other tagged value in this codebase spell it
 * `_tag`; a plain `Error` falls back to its `name`, which is the closest thing
 * it has to one.  Anything else has no tag, and {@link describeTag} says so
 * rather than inventing one.
 */
export const tagOf = (error: unknown): Option.Option<string> => {
  if (isTagged(error)) return Option.some(error._tag);
  if (error instanceof Error && error.name !== '') return Option.some(error.name);
  return Option.none();
};

/** The tag of a failure value, or `'Untagged'` when it has none. */
export const describeTag = (error: unknown): string =>
  Option.getOrElse(tagOf(error), () => 'Untagged');

/**
 * The message of a failure value.
 *
 * A non-`Error` reaching here means something rejected or died with a raw
 * value; `String(value)` is a poor description of an object literal, so the
 * shape is named explicitly instead of leaving `[object Object]` in the corpus.
 */
export const describeMessage = (error: unknown): string => {
  // A `Data.TaggedError` is an `Error` whose `message` is `''` unless the
  // author happened to name a field `message` — its information lives in its
  // fields, not in a sentence. Falling back to the tag keeps `message` from
  // being the empty string on most of this codebase's failures; the fields
  // themselves are carried separately, by `errorFields` below.
  if (error instanceof Error) return error.message === '' ? describeTag(error) : error.message;
  if (typeof error === 'string') return error;
  if (typeof error === 'number' || typeof error === 'boolean' || typeof error === 'bigint') {
    return String(error);
  }
  if (typeof error === 'symbol') return error.toString();
  return `non-Error failure of type ${error === null ? 'null' : typeof error}`;
};

/**
 * The own enumerable fields of a failure value, minus its tag.
 *
 * This is the payload of a `Data.TaggedError` — `{ reason: 'no legal move' }`,
 * `{ path: [...], expected: 'Int' }` — and it is the entire reason tagged errors
 * are worth having.  `Error`'s own `message`, `name` and `stack` are
 * non-enumerable, so they do not appear here and are not duplicated.
 *
 * It goes into `internal`, which `evlog` writes into the wide event but
 * excludes from `toJSON()` and from HTTP responses.  That is the correct
 * asymmetry: a decoder's expected-vs-actual belongs in the corpus and belongs
 * nowhere near a response body.
 */
export const errorFields = (error: unknown): Readonly<Record<string, unknown>> => {
  if (typeof error !== 'object' || error === null) return {};
  return Object.fromEntries(
    Object.keys(error)
      .filter((key) => key !== '_tag')
      .map((key): readonly [string, unknown] => [key, Reflect.get(error, key)]),
  );
};

/**
 * The failure a failure wraps, if it wraps one.
 *
 * Not part of {@link errorFields}, and it cannot be: `Error` defines `cause` as
 * a **non-enumerable** own property, and `Data.TaggedError` inherits that
 * treatment for a `cause` field, so `Object.keys` does not list it.  Which
 * means the single most informative field on every error in this package —
 * `TelemetryWriteError.cause`, `TelemetryEmitError.cause` — is invisible to
 * every payload reader unless it is asked for by name.  This asks.
 *
 * `'cause' in error` rather than `Object.hasOwn`: a wrapper class that puts its
 * cause on the prototype is still carrying a cause.
 */
export const causeOf = (error: unknown): Option.Option<unknown> =>
  typeof error === 'object' && error !== null && 'cause' in error
    ? Option.some(Reflect.get(error, 'cause'))
    : Option.none();

/**
 * A one-line description of a failure that is not merely its own tag.
 *
 * {@link describeMessage} falls back to the tag, which is the right answer for
 * an event's `message` field and a useless one for a log line that already
 * prints the tag next to it.  So: the message when there is a real one,
 * otherwise the wrapped `cause` — the field almost every error in this codebase
 * carries the actual reason in — otherwise the names of the fields that do
 * carry it.
 *
 * Reads properties of `error`, so it is total only for readable values; call it
 * inside the fence.  @see {@link toEvlogError}.
 */
export const describeDetail = (error: unknown): string =>
  describeMessage(error) !== describeTag(error)
    ? describeMessage(error)
    : Option.match(causeOf(error), {
        onSome: (cause) => describeMessage(cause),
        onNone: () => {
          const keys = Object.keys(errorFields(error));
          return keys.length === 0
            ? describeMessage(error)
            : `${describeMessage(error)}; fields: ${keys.join(', ')}`;
        },
      });

/**
 * A short string for an arbitrary value, for log annotations.
 *
 * Log annotations are rendered by whichever `Logger` is installed, and a value
 * that throws while being rendered would turn a dropped-event *warning* into a
 * defect in the finalizer that is announcing it.  Rendering to a string first,
 * inside the fence, is what stops that.
 *
 * `JSON.stringify` here can throw (a cycle, a `BigInt`); that is the fence's
 * problem and not this function's.
 */
export const renderValue = (value: unknown): string => {
  if (typeof value === 'string') return value;
  if (value === null) return 'null';
  if (value === undefined) return 'undefined';
  if (value instanceof Error) return `${describeTag(value)}: ${describeMessage(value)}`;
  if (typeof value === 'number' || typeof value === 'boolean' || typeof value === 'bigint') {
    return String(value);
  }
  if (typeof value === 'symbol') return value.toString();
  if (typeof value === 'function') return `function ${value.name}`;
  return JSON.stringify(value) ?? 'undefined';
};

/**
 * Map any failure value to the `{ message, why, fix }` triple `evlog` records.
 *
 * Pure, and total for any value whose own properties can be read: `createError`
 * never throws, but `describeMessage` and `errorFields` do read `error`, and a
 * value with a throwing getter takes them down with it.  Every call site in
 * this package therefore goes through `toEvlogErrorSafely` in
 * `./evlog-adapter.ts`, which is the fence.
 *
 * An `EvlogError` passes through untouched — it already carries a remedy its
 * author chose, and overwriting that with a table lookup keyed on
 * `'EvlogError'` would be strictly worse information.
 */
export const toEvlogError = (error: unknown): EvlogError => {
  if (EvlogError.isEvlogError(error)) return error;
  const tag = describeTag(error);
  const remedy = remedyFor(tag);
  const fields = errorFields(error);
  return createError({
    message: describeMessage(error),
    code: tag,
    why: remedy.why,
    fix: remedy.fix,
    internal: {
      tag,
      // Says plainly whether the `why`/`fix` above were written by a person or
      // generated by the fallback. A reader should not have to guess.
      registeredRemedy: isRegisteredTag(tag),
      ...(Object.keys(fields).length > 0 ? { fields } : {}),
    },
    ...(error instanceof Error ? { cause: error } : {}),
  });
};
