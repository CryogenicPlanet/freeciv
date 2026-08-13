import { Cause, Effect, Exit, Function, Option } from 'effect';
import { snapshotWideEvent } from './evlog-adapter.ts';
import { summarizeFailure } from './error-map.ts';
import { Observability } from './observability.ts';
import {
  annotateErrorOn,
  annotateOn,
  currentWideEvent,
  makeWideEvent,
  type WideEvent,
} from './wide-event.ts';

export type WideEventOutcome = 'success' | 'failure' | 'defect' | 'interrupt';

const outcomeOf = <A, E>(exit: Exit.Exit<A, E>): WideEventOutcome => {
  if (Exit.isSuccess(exit)) return 'success';
  if (Option.isSome(Cause.failureOption(exit.cause))) return 'failure';
  if (Option.isSome(Cause.dieOption(exit.cause))) return 'defect';
  return 'interrupt';
};

const describeExit = <A, E>(
  event: WideEvent,
  exit: Exit.Exit<A, E>,
): Effect.Effect<void> => {
  const outcome = annotateOn(event, { outcome: outcomeOf(exit) });
  if (Exit.isSuccess(exit)) return outcome;
  const failure = Cause.failureOption(exit.cause);
  if (Option.isSome(failure)) {
    return Effect.zipRight(outcome, annotateErrorOn(event, failure.value));
  }
  const defect = Cause.dieOption(exit.cause);
  return Option.isSome(defect)
    ? Effect.zipRight(outcome, annotateErrorOn(event, defect.value))
    : outcome;
};

const reportTelemetryFailure =
  (event: WideEvent) =>
  (cause: unknown): Effect.Effect<void> => {
    const failure = summarizeFailure(cause);
    return Effect.logWarning('@arena/telemetry dropped a wide event').pipe(
      Effect.annotateLogs({
        eventId: event.id,
        event: event.name,
        reason: failure.reason,
        detail: failure.detail,
      }),
    );
  };

/** Finalize and deliver once, swallowing both typed backend errors and defects. */
const finish = <A, E>(
  event: WideEvent,
  exit: Exit.Exit<A, E>,
): Effect.Effect<void, never, Observability> =>
  describeExit(event, exit).pipe(
    Effect.zipRight(snapshotWideEvent(event)),
    Effect.flatMap((sealed) =>
      Effect.flatMap(Observability, (observability) => observability.record(sealed)),
    ),
    Effect.asVoid,
    Effect.catchAll(reportTelemetryFailure(event)),
    Effect.catchAllDefect(reportTelemetryFailure(event)),
  );

/**
 * Record one wide event around an effect without changing any of its four exits.
 * Available data-first and data-last, like `Effect.withSpan`.
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
        Effect.locally(currentWideEvent, Option.some(event)),
        Effect.onExit((exit) => finish(event, exit)),
      ),
    ),
);
