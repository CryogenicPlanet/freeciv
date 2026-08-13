/**
 * `withWideEvent` — open an event around an effect, seal and emit it exactly
 * once, and change nothing else about the effect.
 *
 * Two promises, and everything in this file exists to keep them:
 *
 * **1. Exactly one event, on all four exits.** An `Effect` can end four ways —
 * success, typed failure, defect, interruption — and a telemetry wrapper that
 * only handles the first two is the reason "we have logs for everything except
 * the incidents" is a familiar sentence. The seal is placed in
 * `Effect.onExit`, which fires on every one of them; and `Effect.onExit` runs
 * its finalizer **uninterruptibly**, which is the part that matters, because
 * the interruption case is precisely the one where a merely-`ensuring`-style
 * cleanup would itself be interrupted before it could write. Below that, the
 * `Ref`-based seal in `./evlog-adapter.ts` makes a second emission structurally
 * impossible rather than merely unlikely.
 *
 * **2. The wrapped effect's result is untouched.** Same value, same error, same
 * cause, same interruption status. This is why every telemetry failure inside
 * the finalizer is caught and logged rather than raised: `Effect.onExit`'s
 * cleanup is typed `Effect<X, never, R2>`, so the compiler will not let this
 * file forget. A telemetry package that can fail a request is worse than no
 * telemetry package.
 *
 * ## Where each annotation comes from, and why it is split
 *
 * The one tap and the finalizer write different things on purpose:
 *
 * - `Effect.tapError` runs **inside** the `Effect.locally` scope and goes
 *   through the ambient `annotateError` — the same public API instrumentation
 *   uses. It is what records a *typed* failure, and it runs after the body, so
 *   the failure the wrapper records is the one the effect actually ended with.
 * - The finalizer runs **outside** that scope and writes to the event
 *   directly. It is the only place that can see interruption or a defect,
 *   because no tap fires on either, and it is the sole author of `outcome`,
 *   which it derives from the `Exit` and writes on every path.
 *
 * There is deliberately no success tap. `describeExit` writes
 * `outcome: 'success'` from the `Exit` itself, after every tap has run, so a
 * tap that wrote the same value would be a `FiberRef` read and a `Ref.update`
 * per successful unit of work that no reader of the corpus could ever detect.
 *
 * The `Effect.locally` boundary is what gives concurrent units of work their
 * own events: the `FiberRef` is set for the wrapped effect's fiber and its
 * children, so two fibers running `withWideEvent` at once annotate two
 * different events with no coordination.
 *
 * @module
 */

import { Cause, Effect, Exit, Function, Option } from 'effect';
import { describeFailure, sealWideEvent, TelemetryExportError } from './evlog-adapter.ts';
import { Observability } from './observability.ts';
import {
  annotateError,
  annotateErrorOn,
  annotateOn,
  currentWideEvent,
  makeWideEvent,
  type WideEvent,
} from './wide-event.ts';

/** How a unit of work ended. Emitted as `outcome` on every event. */
export type WideEventOutcome = 'success' | 'failure' | 'defect' | 'interrupt';

/**
 * Classify an `Exit`.
 *
 * The order is the interesting part. A `Cause` is a tree and can hold several
 * things at once — a fiber that fails and is *then* interrupted during its own
 * finalizers carries both. Asking about the typed failure first, then the
 * defect, then falling through to interruption reports the most specific thing
 * that happened rather than the last thing to touch the fiber.
 */
const outcomeOf = <A, E>(exit: Exit.Exit<A, E>): WideEventOutcome => {
  if (Exit.isSuccess(exit)) return 'success';
  if (Option.isSome(Cause.failureOption(exit.cause))) return 'failure';
  if (Option.isSome(Cause.dieOption(exit.cause))) return 'defect';
  return 'interrupt';
};

/**
 * Write the exit onto the event.
 *
 * A defect is recorded here rather than by a tap because no tap sees one:
 * `Effect.tapError` fires on the typed error channel only, and a defect never
 * reaches it. Typed failures are already recorded by `tapError`, so they are
 * not written twice.
 */
// The key is `outcome` and not `status`: `status` is one of `evlog`'s
// `InternalFields`, is typed as an HTTP status number, and is what its tail
// sampling reads.
const describeExit = <A, E>(event: WideEvent, exit: Exit.Exit<A, E>): Effect.Effect<void> => {
  const outcome = annotateOn(event, { outcome: outcomeOf(exit) });
  if (Exit.isSuccess(exit)) return outcome;
  return Option.match(Cause.dieOption(exit.cause), {
    onNone: () => outcome,
    onSome: (defect) => Effect.zipRight(outcome, annotateErrorOn(event, defect)),
  });
};

/**
 * The failure's payload, under keys that cannot collide with the event's own.
 *
 * A `TelemetryEmitError` carries `eventId` and `name` of its own; annotating
 * those raw would overwrite the identity of the event the warning is about.
 */
const prefixed = (
  fields: Readonly<Record<string, string>>,
): Readonly<Record<string, string>> =>
  Object.fromEntries(
    Object.entries(fields).map(([key, value]): readonly [string, string] => [
      `failure.${key}`,
      value,
    ]),
  );

const isTelemetryExportError = (cause: unknown): cause is TelemetryExportError =>
  cause instanceof TelemetryExportError;

/**
 * The last resort: telemetry failed, so say so on the logger and let the
 * program continue exactly as it would have.
 *
 * Two failure classes, two messages. A `TelemetryExportError` means the NDJSON
 * corpus was written and only the OTLP mirror failed — calling that a dropped
 * event would send the reader to check disk space when the record of truth is
 * already on disk. Every other failure on this path is a genuine corpus loss.
 *
 * A `Data.TaggedError` has an empty `message`, so a warning built from its tag
 * and its message says the same word twice and nothing else — a full disk and
 * a `BigInt` in an `annotate()` call would produce the identical line. So the
 * failure's payload goes on the line as well (`failure.dir`, `failure.cause`,
 * `failure.endpoint`, …), along with the remedy registered for its tag,
 * because a warning that names what to do is this package's whole thesis and
 * the dropped-event path was the one place not holding to it.
 *
 * `describeFailure` is fenced: the value being described arrives from
 * `catchAllDefect` and nobody has vetted it.
 */
const reportTelemetryFailure =
  (event: WideEvent) =>
  (cause: unknown): Effect.Effect<void> =>
    describeFailure(cause).pipe(
      Effect.flatMap((failure) =>
        Effect.logWarning(
          isTelemetryExportError(cause)
            ? '@arena/telemetry OTLP mirror failed for a wide event'
            : '@arena/telemetry dropped a wide event',
        ).pipe(
          Effect.annotateLogs({
            eventId: event.id,
            event: event.name,
            reason: failure.reason,
            detail: failure.detail,
            fix: failure.fix,
            ...prefixed(failure.fields),
          }),
        ),
      ),
    );

/**
 * Seal, emit, deliver — and absorb every way that can go wrong.
 *
 * Both `catchAll` and `catchAllDefect` are needed: a typed telemetry failure is
 * expected and handled, and a *defect* in the observability layer (a backend
 * that throws where it promised not to) must not become a defect in the
 * caller's program either.
 */
const finish = <A, E>(
  event: WideEvent,
  exit: Exit.Exit<A, E>,
): Effect.Effect<void, never, Observability> =>
  describeExit(event, exit).pipe(
    Effect.zipRight(sealWideEvent(event)),
    Effect.flatMap((sealed) =>
      Effect.flatMap(Observability, (observability) => observability.record(sealed)),
    ),
    Effect.asVoid,
    Effect.catchAll(reportTelemetryFailure(event)),
    Effect.catchAllDefect(reportTelemetryFailure(event)),
  );

/**
 * Run `self` as one named unit of work, recorded as a single wide event.
 *
 * ```ts
 * playTurn(state).pipe(withWideEvent('turn.play'))
 * ```
 *
 * Inside `self` — at any depth, in any forked child — `annotate({ … })` writes
 * to this event. Outside it, `annotate` no-ops, so instrumented code stays
 * callable from anywhere.
 *
 * The result is `self`'s result, unchanged: `withWideEvent(x, 'n')` and `x`
 * produce the same `Exit`, and `test/result-preserved.test.ts` holds that to
 * it on all four exit paths against both a recording and a no-op backend.
 *
 * Available data-first (`withWideEvent(self, 'name')`) and data-last
 * (`self.pipe(withWideEvent('name'))`), following `Effect.withSpan`.
 */
export const withWideEvent: {
  (
    name: string,
  ): <A, E, R>(self: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R | Observability>;
  <A, E, R>(
    self: Effect.Effect<A, E, R>,
    name: string,
  ): Effect.Effect<A, E, R | Observability>;
} = Function.dual(
  2,
  <A, E, R>(
    self: Effect.Effect<A, E, R>,
    name: string,
  ): Effect.Effect<A, E, R | Observability> =>
    Effect.flatMap(makeWideEvent(name), (event) =>
      self.pipe(
        Effect.tapError((error) => annotateError(error)),
        Effect.locally(currentWideEvent, Option.some(event)),
        Effect.onExit((exit) => finish(event, exit)),
      ),
    ),
);
