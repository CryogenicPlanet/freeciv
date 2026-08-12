/**
 * The fence. Everything in this package that can throw lives here, and nothing
 * outside this file uses `Effect.try` or `Effect.tryPromise`.
 *
 * `evlog` is a good library with a conventional JavaScript surface, which means
 * three of its calls throw and two of its promises reject. Rather than dust
 * `Effect.try` across the package and lose track of which call is fenced by
 * what, every crossing is made here and every function below returns a typed
 * failure. The rest of `@arena/telemetry` is pure `Effect`.
 *
 * The specific hazards, all verified against evlog 2.26.0:
 *
 * - **The event has to be serializable, and nothing on the way in checks.**
 *   `emit()`'s console path calls `JSON.stringify(event)` unguarded — but this
 *   module runs `silent: true`, and evlog gates that stringify behind
 *   `if (!state.silent)` (`audit-DZIXf7Z8.mjs:186`), so in *this* configuration
 *   `emit()` does not serialize at all. Left alone, a `BigInt` or a circular
 *   reference would therefore first be noticed by `writeBatchToFs` and reported
 *   as a `TelemetryWriteError` — sending whoever reads that warning to check
 *   disk space for what is an `annotate()` bug. {@link emitWideEvent} closes
 *   that by serializing the finished event itself, inside the same fence: one
 *   extra `JSON.stringify` per event, in exchange for the failure being named
 *   where it happened and being identical on the live and test layers.
 * - **`writeBatchToFs` rejects** on any real filesystem error. This is the
 *   *reason* this module writes files itself instead of registering
 *   `initLogger({ drain })`: a drain built by `createFsDrain` **never rejects**
 *   — it catches, `console.error`s, and on `EROFS`/`EACCES`/`EPERM` permanently
 *   self-disables after one warning. A corpus recorder that cannot tell you it
 *   stopped recording is not a recorder.
 * - **`sendBatchToOTLP` rejects** on timeout, non-2xx, or network failure,
 *   after its own retries.
 * - **`initLogger` never throws** but is fenced anyway, because it is the one
 *   call whose contract might change under us and the cost is one wrapper.
 *
 * ## Why there is no drain
 *
 * `emit()` is synchronous; the drain it kicks off is fire-and-forget and, with
 * no `waitUntil`, has *no referent at all* — you cannot await it after the
 * fact, and `evlog` registers no `beforeExit` or `SIGINT` handler in its core.
 * A `process.exit()` immediately after `emit()` writes nothing. So this module
 * runs `initLogger({ silent: true })` with **no** drain, takes the `WideEvent`
 * that `emit()` returns, and writes it itself inside `Effect.tryPromise`. That
 * buys typed write errors, exact ordering, and no exit race, and it costs
 * nothing: `emit()` still stamps, samples and finalizes the event exactly as it
 * would have.
 *
 * ## The sealed-once guard
 *
 * {@link sealWideEvent} is a compare-and-set on the event's `sealed` `Ref`, and
 * a second seal is a **typed error**, not a silent no-op. The choice matters:
 * a second sealer is code that believes it is finishing a unit of work and
 * believes its annotations were recorded. Handing it back a `SealedWideEvent`
 * would confirm a recording that never happened; failing tells it the truth.
 * `withWideEvent` is then free to swallow that failure at its own boundary —
 * it does, into a log line — because telemetry must never change a program's
 * outcome. The guard being structural rather than conventional is what makes
 * "exactly once" a property of the type rather than a property of the reviewer.
 *
 * ## The hazards that are not evlog's
 *
 * Three functions at the bottom of this file fence something else: the values
 * *callers* hand us. `annotate({ …value… })` copies its argument, and an own
 * enumerable getter that throws makes that copy throw — which would turn a
 * documented no-op into a defect, and only when telemetry is switched on.
 * {@link readTelemetryFields}, {@link toEvlogErrorSafely} and
 * {@link describeFailure} are the fenced readers, and they live here for the
 * same reason everything else does: so that "which calls can throw" has one
 * answer and one file.
 *
 * @module
 */

import { Clock, Data, Effect, Function, Option, Ref } from 'effect';
import type { AuditableLogger, EvlogError, WideEvent as EvlogWideEvent } from 'evlog';
import { createError, createLogger, initLogger } from 'evlog';
import { writeBatchToFs } from 'evlog/fs';
import { sendBatchToOTLP } from 'evlog/otlp';
import type { TelemetryConfigShape } from './config.ts';
import {
  causeOf,
  describeDetail,
  describeTag,
  errorFields,
  remedyFor,
  renderValue,
  toEvlogError,
  UNREADABLE_FAILURE_TAG,
} from './error-map.ts';
import type { SealedWideEvent, TelemetryFields, WideEvent } from './wide-event.ts';

/** `evlog`'s emitted event — the shape written to the corpus. Re-exported for callers. */
export type { EvlogWideEvent };

/** `initLogger` refused to establish the process-global logger state. */
export class TelemetryInitError extends Data.TaggedError('TelemetryInitError')<{
  /** The service identity that failed to initialize. */
  readonly service: string;
  /** Whatever `initLogger` threw. */
  readonly cause: unknown;
}> {}

/** `emit()` threw — almost always a value in the event that `JSON.stringify` refuses. */
export class TelemetryEmitError extends Data.TaggedError('TelemetryEmitError')<{
  /** `eventId` of the event that could not be emitted. */
  readonly eventId: string;
  /** The unit of work that event described. */
  readonly name: string;
  /** Whatever `emit()` threw, typically a `TypeError`. */
  readonly cause: unknown;
}> {}

/** The NDJSON append failed, so this event is not in the corpus. */
export class TelemetryWriteError extends Data.TaggedError('TelemetryWriteError')<{
  /** The run directory that was being appended to. */
  readonly dir: string;
  /** How many events were lost with this write. */
  readonly events: number;
  /** The rejection from `writeBatchToFs`. */
  readonly cause: unknown;
}> {}

/** The OTLP mirror failed. The corpus is written first and is unaffected. */
export class TelemetryExportError extends Data.TaggedError('TelemetryExportError')<{
  /** The collector endpoint that rejected or timed out. */
  readonly endpoint: string;
  /** How many events did not reach the collector. */
  readonly events: number;
  /** The rejection from `sendBatchToOTLP`, after its own retries. */
  readonly cause: unknown;
}> {}

/** A second attempt to seal an event that was already sealed and emitted. */
export class WideEventAlreadySealedError extends Data.TaggedError('WideEventAlreadySealed')<{
  /** `eventId` of the already-sealed event. */
  readonly eventId: string;
  /** The unit of work it described. */
  readonly name: string;
}> {}

/** Every failure this package can produce. */
export type TelemetryError =
  | TelemetryInitError
  | TelemetryEmitError
  | TelemetryWriteError
  | TelemetryExportError
  | WideEventAlreadySealedError;

/**
 * Establish `evlog`'s process-global logger state.
 *
 * Call this **once**, from a single layer's acquire. `initLogger` writes to a
 * `Symbol.for('evlog.registry.v2')` slot on `globalThis` that every copy of
 * evlog 2.x in the process shares, and it overwrites *every* field on each
 * call. Because this package configures no drain, repeated calls with an
 * identical configuration are harmless — but a call from anywhere else with a
 * different one silently changes this package's behaviour.
 *
 * That global is exactly why nothing this function establishes is trusted to
 * identify an event. `state.env` here is a courtesy: two `ObservabilityLive`
 * layers in one process share it, and the second to be built would otherwise
 * relabel the first one's events. {@link stampIdentity} writes `service`,
 * `environment`, `version`, `commitHash` and `region` from the layer's own
 * configuration after `emit()`, so a layer is self-describing whatever any
 * other layer — or `detectEnvironment()`'s `GITHUB_SHA` probe — did to the
 * global.
 *
 * The settings are all deliberate:
 *
 * - `silent: true` — this module writes the corpus itself; console output would
 *   be a second, differently-shaped copy of every event.
 * - `redact: false` — evlog redacts by default outside development. A corpus is
 *   backend-only and never leaves the run directory, and silent rewriting of
 *   recorded values would break exactly the byte-level comparisons a corpus
 *   exists for. If a secret should not be in the corpus, it should not be
 *   annotated onto an event.
 * - `_suppressDrainWarning` — `silent` with no drain otherwise prints a one-time
 *   warning telling us to configure the drain we deliberately did not configure.
 */
export const initializeEvlog = (
  config: TelemetryConfigShape,
): Effect.Effect<void, TelemetryInitError> =>
  Effect.try({
    try: () =>
      initLogger({
        enabled: true,
        silent: true,
        pretty: false,
        stringify: true,
        redact: false,
        _suppressDrainWarning: true,
        env: {
          service: config.service,
          environment: config.environment,
          ...Option.match(config.version, {
            onNone: () => ({}),
            onSome: (version) => ({ version }),
          }),
        },
      }),
    catch: (cause) => new TelemetryInitError({ service: config.service, cause }),
  });

/**
 * Seal `event`, exactly once, and take its immutable snapshot.
 *
 * The compare-and-set and the snapshot are separated by a `Clock` read, which
 * is safe: the seal is already set, so nothing can annotate the event in
 * between, and a concurrent sealer has already failed.
 */
export const sealWideEvent = (
  event: WideEvent,
): Effect.Effect<SealedWideEvent, WideEventAlreadySealedError> =>
  Ref.modify(event.sealed, (sealed) => [sealed, true] as const).pipe(
    Effect.flatMap((wasSealed) =>
      wasSealed
        ? Effect.fail(new WideEventAlreadySealedError({ eventId: event.id, name: event.name }))
        : Effect.all({
            now: Clock.currentTimeMillis,
            fields: Ref.get(event.fields),
            error: Ref.get(event.error),
          }).pipe(
            Effect.map(
              (snapshot): SealedWideEvent => ({
                id: event.id,
                name: event.name,
                startedAt: event.startedAt,
                durationMs: snapshot.now - event.startedAt,
                fields: snapshot.fields,
                error: snapshot.error,
              }),
            ),
          ),
    ),
  );

/**
 * The context handed to `evlog`.
 *
 * `event`, `eventId` and `durationMs` are written *after* the caller's fields
 * so that instrumentation cannot accidentally shadow the three keys every query
 * over the corpus depends on. `durationMs` is set explicitly because
 * `createLogger` — unlike the request loggers — stamps no duration of its own.
 */
const toEvlogContext = (sealed: SealedWideEvent): Record<string, unknown> => ({
  ...sealed.fields,
  event: sealed.name,
  eventId: sealed.id,
  durationMs: sealed.durationMs,
});

/**
 * `logger.error()` is what sets `level: 'error'` and copies `code`, `why`,
 * `fix`, `stack` and `internal` onto `event.error`. Doing it by hand with
 * `set({ error: ... })` would produce a differently-shaped event.
 */
const applyError = (logger: AuditableLogger, error: Option.Option<EvlogError>): void =>
  Option.match(error, {
    onNone: Function.constVoid,
    onSome: (value) => logger.error(value),
  });

/**
 * `formatDuration`, transcribed from `evlog/dist/utils.mjs:12`:
 *
 * ```js
 * function formatDuration(ms) {
 *   if (ms < 1e3) return `${Math.round(ms)}ms`;
 *   return `${(ms / 1e3).toFixed(2)}s`;
 * }
 * ```
 *
 * Transcribed rather than imported because `evlog` does not export `./utils`
 * from its `exports` map. Four lines is a cheaper dependency than reaching into
 * an unexported path, and {@link stampIdentity} explains why we need it at all.
 */
const formatDuration = (ms: number): string =>
  ms < 1000 ? `${Math.round(ms)}ms` : `${(ms / 1000).toFixed(2)}s`;

/**
 * The five fields `evlog` fills in from its process-global `state.env`, and the
 * five this package overwrites from its own configuration.
 *
 * They are stripped before being rewritten rather than merely overwritten,
 * because three of them are *optional*: `version`, `commitHash` and `region`
 * must be **absent** when the configuration does not carry them, and
 * `initLogger` will have put `APP_VERSION`, `GITHUB_SHA` and `AWS_REGION` there
 * given the chance. An event whose field set depends on which machine ran it is
 * not comparable byte-for-byte against one recorded somewhere else, which is
 * the entire premise of a corpus.
 */
const OPTIONAL_IDENTITY: ReadonlyArray<string> = ['version', 'commitHash', 'region'];

/** `{ key: value }` when `value` is `Some`, and nothing at all when it is `None`. */
const optionalField = (
  key: string,
  value: Option.Option<string>,
): Readonly<Record<string, string>> =>
  Option.match(value, {
    onNone: () => ({}),
    onSome: (some) => ({ [key]: some }),
  });

/**
 * Stamp the layer's identity onto the emitted event, and put *our* duration
 * back on it.
 *
 * ## Identity
 *
 * `emit()` fills `service`, `environment`, `version`, `commitHash` and `region`
 * from `state.env` whenever the event does not already carry them
 * (`audit-DZIXf7Z8.mjs:168-173`) — and `state.env` is process-global, last
 * `initLogger` wins, seeded from `detectEnvironment()`. Both of those are
 * someone else's state, so neither is allowed near the corpus's primary
 * faceting key. Every one of the five is written here, from this layer's
 * configuration, or deliberately left off.
 *
 * ## Duration
 *
 * `evlog`'s `emit()` ends with an unconditional
 *
 * ```js
 * context.durationMs = elapsedMs(startTime);
 * context.duration   = formatDuration(durationMs);
 * ```
 *
 * (`audit-DZIXf7Z8.mjs:718`), *after* the `emit(overrides)` merge — so a caller
 * cannot supply a duration by any documented route. `startTime` is when the
 * logger was constructed, and this package constructs one per event at seal
 * time, which would report every unit of work as taking well under a
 * millisecond. Rather than hold an `evlog` logger open for the lifetime of the
 * unit of work — which the library warns against, and which would put a wall
 * clock where this package deliberately uses the `Clock` service — the returned
 * event is corrected here.
 *
 * A fresh object, not a mutation: nothing else holds a reference to the one
 * `emit()` returned (there is no drain), but "the value evlog returned" and
 * "the value we write" being distinct objects is worth the one allocation.
 * `timestamp` and `level` are re-stated only to keep the result typed as an
 * `EvlogWideEvent` across the `Object.fromEntries` — their values are unchanged.
 */
const stampIdentity = (
  event: EvlogWideEvent,
  durationMs: number,
  config: TelemetryConfigShape,
): EvlogWideEvent => ({
  ...Object.fromEntries(
    Object.entries(event).filter(([key]) => !OPTIONAL_IDENTITY.includes(key)),
  ),
  timestamp: event.timestamp,
  level: event.level,
  service: config.service,
  environment: config.environment,
  durationMs,
  duration: formatDuration(durationMs),
  ...optionalField('version', config.version),
  ...optionalField('commitHash', config.commitHash),
  ...optionalField('region', config.region),
});

/**
 * Serialize the finished event, and throw the way `JSON.stringify` throws.
 *
 * The result is discarded: `writeBatchToFs` and `sendBatchToOTLP` each do their
 * own serialization, and there is no supported way to hand either of them
 * bytes. What this buys is *where* the failure is reported. With `silent: true`
 * nothing on the emit path serializes, so a `BigInt` attached by `annotate()`
 * would otherwise surface as a `TelemetryWriteError` — whose remedy sends the
 * reader to check disk space — or, on `ObservabilityTest`, not surface at all,
 * quietly making the capture layer more forgiving than production.
 */
const proveSerializable = (event: EvlogWideEvent): EvlogWideEvent => {
  JSON.stringify(event);
  return event;
};

/**
 * Build an `evlog` logger from a sealed event, emit it, and finish the event
 * off with this layer's identity and duration.
 *
 * Returns `None` when `emit()` returned `null`: head sampling dropped the
 * event, or someone else's `initLogger({ enabled: false })` disabled the
 * process. That is a *successful* outcome — the logger is sealed and the event
 * was deliberately not recorded — and must never be retried.
 *
 * A fresh logger per event, never a shared one: `evlog` loggers are
 * single-use, and reusing one would emit an event that had already been sealed.
 */
export const emitWideEvent = (
  sealed: SealedWideEvent,
  config: TelemetryConfigShape,
): Effect.Effect<Option.Option<EvlogWideEvent>, TelemetryEmitError> =>
  Effect.try({
    try: () => {
      const logger = createLogger(toEvlogContext(sealed));
      applyError(logger, sealed.error);
      return Option.map(Option.fromNullable(logger.emit()), (event) =>
        proveSerializable(stampIdentity(event, sealed.durationMs, config)),
      );
    },
    catch: (cause) => new TelemetryEmitError({ eventId: sealed.id, name: sealed.name, cause }),
  });

/**
 * Append events to the NDJSON corpus in `dir`.
 *
 * `pretty: false` is not a preference. `evlog/fs` writes into a `.jsonl` file
 * either way, and `pretty: true` puts indented multi-line JSON in it — a file
 * that is no longer NDJSON and that no line-oriented reader can parse.
 */
export const writeWideEvents = (
  events: ReadonlyArray<EvlogWideEvent>,
  dir: string,
): Effect.Effect<void, TelemetryWriteError> =>
  Effect.tryPromise({
    try: () => writeBatchToFs([...events], { dir, pretty: false }),
    catch: (cause) => new TelemetryWriteError({ dir, events: events.length, cause }),
  });

/**
 * Mirror events to an OTLP logs collector.
 *
 * `recordShape: 'compact'` flattens nested fields to dotted attributes
 * (`turn.number`), which is the form backends can facet on; the default
 * `'json'` stringifies them into one opaque attribute and is close to useless
 * for querying.
 */
export const exportWideEvents = (
  events: ReadonlyArray<EvlogWideEvent>,
  endpoint: string,
  serviceName: string,
): Effect.Effect<void, TelemetryExportError> =>
  Effect.tryPromise({
    try: () => sendBatchToOTLP([...events], { endpoint, serviceName, recordShape: 'compact' }),
    catch: (cause) => new TelemetryExportError({ endpoint, events: events.length, cause }),
  });

/**
 * Something a reader below threw on. Never escapes this file: it exists so the
 * error channel of {@link fenced} names what it is carrying while it carries it.
 */
class FenceBreach extends Data.TaggedError('FenceBreach')<{
  /** Whatever the read threw. */
  readonly cause: unknown;
}> {}

/**
 * Run `compute`, and describe what it threw with `onThrow` if it does.
 *
 * The shape every reader below shares: a best-effort description of a value
 * nobody has vetted, which must not fail and must not take its caller with it.
 */
const fenced = <A>(compute: () => A, onThrow: (cause: unknown) => A): Effect.Effect<A> =>
  Effect.try({ try: compute, catch: (cause) => new FenceBreach({ cause }) }).pipe(
    Effect.catchAll((breach) => Effect.succeed(onThrow(breach.cause))),
  );

/**
 * What a value that could not be read is recorded as.
 *
 * The thrown value's *constructor name* and nothing else: it came from reading
 * a hostile value, so reading it in turn is not obviously safer.
 */
const unreadable = (cause: unknown): string =>
  `@arena/telemetry: unreadable value (${cause instanceof Error ? cause.constructor.name : 'threw'})`;

/**
 * Copy `fields` into a plain object this package owns.
 *
 * `annotate({ … })` is documented as a no-op outside a wide event and as a
 * plain field merge inside one; neither promise survives an own enumerable
 * getter that throws, because the spread that reads it is the caller's fiber
 * doing the reading. The fast path is the same spread, fenced; the slow path
 * reads key by key, so one hostile field costs that field and not the other
 * eleven.
 *
 * Only the *top* level is read. A nested getter that throws is still in the
 * value, and it fails later, at {@link emitWideEvent}'s serialization — as a
 * `TelemetryEmitError`, which is the failure it actually is.
 */
export const readTelemetryFields = (fields: TelemetryFields): Effect.Effect<TelemetryFields> =>
  Effect.try({ try: () => ({ ...fields }), catch: (cause) => new FenceBreach({ cause }) }).pipe(
    Effect.catchAll(() =>
      fenced(
        (): ReadonlyArray<string> => Object.keys(fields),
        () => [],
      ).pipe(
        Effect.flatMap((keys) =>
          Effect.forEach(keys, (key) =>
            fenced(
              (): readonly [string, unknown] => [key, Reflect.get(fields, key)],
              (cause): readonly [string, unknown] => [key, unreadable(cause)],
            ),
          ),
        ),
        Effect.map((entries) => Object.fromEntries(entries)),
      ),
    ),
  );

/**
 * {@link toEvlogError}, for a failure value nobody has vetted.
 *
 * `toEvlogError` reads `error`'s message and its own enumerable fields, so a
 * hostile failure value would make *recording* a failure fail. The fallback
 * records that honestly, under a tag {@link REMEDIES} has an entry for, rather
 * than dropping the error channel of the event on the floor.
 */
export const toEvlogErrorSafely = (error: unknown): Effect.Effect<EvlogError> =>
  fenced(
    () => toEvlogError(error),
    (cause) =>
      createError({
        message: unreadable(cause),
        code: UNREADABLE_FAILURE_TAG,
        why: remedyFor(UNREADABLE_FAILURE_TAG).why,
        fix: remedyFor(UNREADABLE_FAILURE_TAG).fix,
        internal: { tag: UNREADABLE_FAILURE_TAG, registeredRemedy: true },
      }),
  );

/** Everything a dropped-event warning says about the failure that dropped it. */
export interface FailureReport {
  /** The failure's tag — `TelemetryWriteError`, `RangeError`, `Untagged`. */
  readonly reason: string;
  /** What actually went wrong, in words, and never merely {@link FailureReport.reason} again. */
  readonly detail: string;
  /** The registered remedy's `fix`, so the warning says what to do about itself. */
  readonly fix: string;
  /** The failure's payload, rendered to strings — `dir`, `events`, `endpoint`, `cause`. */
  readonly fields: Readonly<Record<string, string>>;
}

/**
 * Describe a failure for the one log line that announces a lost event.
 *
 * Every part is fenced separately, so a value that resists being described
 * still contributes whatever *can* be read. The payload is rendered to strings
 * here rather than handed to the logger as-is: annotations are serialized by
 * whichever `Logger` is installed, and a `BigInt` in there — which is one of
 * the likelier reasons the event was dropped in the first place — would turn
 * the warning into a defect in the finalizer raising it.
 */
export const describeFailure = (cause: unknown): Effect.Effect<FailureReport> =>
  Effect.all({
    reason: fenced(
      () => describeTag(cause),
      () => UNREADABLE_FAILURE_TAG,
    ),
    detail: fenced(
      () => describeDetail(cause),
      (thrown) => unreadable(thrown),
    ),
    fields: fenced(
      (): ReadonlyArray<readonly [string, unknown]> => [
        ...Object.entries(errorFields(cause)),
        // `cause` is a non-enumerable own property, so `errorFields` cannot see
        // it and the reason the write or the emit actually failed would
        // otherwise never reach the line announcing that it failed.
        ...Option.match(causeOf(cause), {
          onNone: (): ReadonlyArray<readonly [string, unknown]> => [],
          onSome: (wrapped) => [['cause', wrapped] as const],
        }),
      ],
      () => [],
    ).pipe(
      Effect.flatMap((entries) =>
        Effect.forEach(entries, ([key, value]) =>
          fenced(
            (): readonly [string, string] => [key, renderValue(value)],
            (thrown): readonly [string, string] => [key, unreadable(thrown)],
          ),
        ),
      ),
      Effect.map((entries) => Object.fromEntries(entries)),
    ),
  }).pipe(Effect.map((report) => ({ ...report, fix: remedyFor(report.reason).fix })));
