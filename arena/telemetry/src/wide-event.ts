/**
 * One event per unit of work, annotated from anywhere inside it.
 *
 * A wide event is the opposite of a log line.  Instead of scattering twelve
 * messages across a request and reassembling them later by timestamp, the unit
 * of work opens **one** event, everything that happens writes a field onto it,
 * and it is emitted once at the end.  Queries then read like questions about
 * the work ("which turns took over 2s and also hit a legality refusal?")
 * instead of joins over text.
 *
 * ## The three pieces here
 *
 * - {@link WideEvent} — the open, mutable-until-sealed accumulator.  Its
 *   mutability lives in `Ref`s, so every write is an `Effect` and there is no
 *   plain object anyone can scribble on.
 * - {@link currentWideEvent} — a module-level `FiberRef` holding the event the
 *   running fiber belongs to, or `None`.  A module-level `FiberRef` is the one
 *   module-level-state idiom this codebase accepts, and it is the only thing
 *   that makes ambient annotation possible: `annotate()` deep in a call stack
 *   must not require every intermediate function to thread an event parameter.
 *   Fibers get their own copy, so concurrent units of work cannot cross-write.
 * - {@link annotate} / {@link annotateError} — the ambient writers.  **They
 *   no-op when there is no current event**, which is deliberate: a library
 *   annotating an event is describing work, not doing work, and code that is
 *   correct inside `withWideEvent` must stay correct outside it.  A missing
 *   event is a missing observation, never a failure.
 *
 * ## Sealing
 *
 * A sealed event stops accepting writes — {@link annotateOn} checks the seal and
 * returns without touching the fields.  This mirrors `evlog`'s own behaviour
 * (its logger seals on `emit()` and warns on late `set()`), except that here a
 * late write is silent: by the time the seal is set the event is already on its
 * way to disk, and a warning from a fiber that merely outlived its parent is
 * noise, not a defect.  The *second seal* is the interesting case, and that one
 * is a typed error — see `sealWideEvent` in `./evlog-adapter.ts`.
 *
 * @module
 */

import { Clock, Effect, FiberRef, Option, Random, Ref } from 'effect';
import type { EvlogError } from 'evlog';
import { readTelemetryFields, toEvlogErrorSafely } from './evlog-adapter.ts';

/**
 * Fields accumulated on an open event.
 *
 * `unknown` values, not `JsonValue`: the whole point of a wide event is that
 * callers attach whatever describes their work.  The serialization boundary is
 * in `./evlog-adapter.ts`, which is where a circular reference or a `BigInt`
 * put here becomes a `TelemetryEmitError` naming this `annotate` call, instead
 * of a defect in whoever was annotating.
 */
export type TelemetryFields = Readonly<Record<string, unknown>>;

/** An open wide event: annotate it until something seals it. */
export interface WideEvent {
  /** Correlation id, emitted as `eventId`. Unique per event, not per fiber. */
  readonly id: string;
  /** The unit of work, emitted as `event` — e.g. `turn.play`, `gateway.request`. */
  readonly name: string;
  /** `Clock.currentTimeMillis` when the event opened; `TestClock` moves it. */
  readonly startedAt: number;
  /** Everything annotated so far. */
  readonly fields: Ref.Ref<TelemetryFields>;
  /** The failure this event ended with, already mapped to `{ message, why, fix }`. */
  readonly error: Ref.Ref<Option.Option<EvlogError>>;
  /** `true` once the event has been sealed; further annotation is ignored. */
  readonly sealed: Ref.Ref<boolean>;
}

/**
 * The immutable snapshot taken at seal time — what actually gets emitted.
 *
 * Nothing here is a `Ref`: once an event is sealed its content can no longer
 * change, so the emission path cannot observe a different event than the one
 * that was sealed.
 */
export interface SealedWideEvent {
  /** @see {@link WideEvent.id} */
  readonly id: string;
  /** @see {@link WideEvent.name} */
  readonly name: string;
  /** @see {@link WideEvent.startedAt} */
  readonly startedAt: number;
  /** Wall-clock milliseconds between opening and sealing, measured on the `Clock`. */
  readonly durationMs: number;
  /** @see {@link WideEvent.fields} */
  readonly fields: TelemetryFields;
  /** @see {@link WideEvent.error} */
  readonly error: Option.Option<EvlogError>;
}

/**
 * The event the running fiber belongs to, or `None` outside `withWideEvent`.
 *
 * Module-level by necessity and by convention: it is a `FiberRef`, so the value
 * is per-fiber and scoped by `Effect.locally`, not process-global state.  Forked
 * fibers inherit the parent's value (the default `fork` behaviour), which is
 * what makes `Effect.forEach(..., { concurrency })` inside a unit of work still
 * annotate that unit's event.
 */
export const currentWideEvent: FiberRef.FiberRef<Option.Option<WideEvent>> = FiberRef.unsafeMake<
  Option.Option<WideEvent>
>(Option.none());

/** The current event, or `None` when the fiber is not inside a `withWideEvent`. */
export const currentEvent: Effect.Effect<Option.Option<WideEvent>> = FiberRef.get(currentWideEvent);

/**
 * 64 bits of correlation id as hex, drawn from the `Random` service rather than
 * `crypto.randomUUID()`.
 *
 * Effect-injected randomness is the difference between a test that can pin an
 * id and one that can only assert its shape: under `TestContext` the sequence
 * is seeded and reproducible, and a recorded corpus from a deterministic replay
 * comes out byte-identical.  Two 32-bit draws, unsigned and zero-padded, are
 * plenty to correlate events inside one run — which is all an `eventId` is for.
 */
const randomEventId: Effect.Effect<string> = Effect.map(
  Effect.all([Random.nextInt, Random.nextInt]),
  ([high, low]) => `${(high >>> 0).toString(16).padStart(8, '0')}${(low >>> 0).toString(16).padStart(8, '0')}`,
);

/**
 * Open a new event named `name`.
 *
 * Normally called by `withWideEvent`; call it directly only when you own the
 * seal too.  An event opened and never sealed is simply never emitted — it
 * holds no resource and needs no finalizer.
 */
export const makeWideEvent = (name: string): Effect.Effect<WideEvent> =>
  Effect.all({
    id: randomEventId,
    startedAt: Clock.currentTimeMillis,
    fields: Ref.make<TelemetryFields>({}),
    error: Ref.make<Option.Option<EvlogError>>(Option.none()),
    sealed: Ref.make(false),
  }).pipe(Effect.map((parts) => ({ ...parts, name })));

/**
 * Merge `fields` into a specific event, shallowly; last write wins per key.
 *
 * Shallow on purpose.  `evlog`'s `set()` deep-merges and *concatenates* arrays,
 * which turns "correct the `moves` field" into "append to it"; this package
 * hands `evlog` one finished object at emit time instead, so the merge
 * semantics are the ones written here and nowhere else.
 *
 * `fields` is copied through `readTelemetryFields` — the fence — before it goes
 * anywhere near the `Ref`.  A caller's object can have an own enumerable getter
 * that throws, and reading it *here* would make `annotate` a defect inside a
 * unit of work and a no-op outside one, which is precisely the "correct code
 * stops being correct when telemetry is switched on" failure this module's
 * docstring rules out.  What lands in the `Ref` is always a plain object this
 * package built, which is what keeps the merge itself a pure, atomic
 * `Ref.update`.
 *
 * No-ops on a sealed event.
 */
export const annotateOn = (event: WideEvent, fields: TelemetryFields): Effect.Effect<void> =>
  Ref.get(event.sealed).pipe(
    Effect.flatMap((sealed) =>
      sealed
        ? Effect.void
        : readTelemetryFields(fields).pipe(
            Effect.flatMap((safe) =>
              Ref.update(event.fields, (current) => ({ ...current, ...safe })),
            ),
          ),
    ),
  );

/**
 * Record `error` as the failure of a specific event.
 *
 * The value is mapped through `./error-map.ts` on the way in, so the remedy is
 * attached at the point the failure is observed rather than reconstructed later
 * from a tag.  The mapping reads the failure's message and its own fields, so
 * it goes through the fence for the same reason {@link annotateOn} does: an
 * unreadable failure value is recorded as an unreadable failure value, not as a
 * defect in the code that was reporting it.  The last error wins: an event ends
 * one way.
 *
 * No-ops on a sealed event.
 */
export const annotateErrorOn = (event: WideEvent, error: unknown): Effect.Effect<void> =>
  Ref.get(event.sealed).pipe(
    Effect.flatMap((sealed) =>
      sealed
        ? Effect.void
        : toEvlogErrorSafely(error).pipe(
            Effect.flatMap((mapped) => Ref.set(event.error, Option.some(mapped))),
          ),
    ),
  );

/**
 * Merge `fields` into the current event; no-op when there is none.
 *
 * This is the API almost all instrumentation should use — it needs no event
 * parameter, so a function five frames deep can describe what it did without
 * its callers knowing telemetry exists.
 */
export const annotate = (fields: TelemetryFields): Effect.Effect<void> =>
  currentEvent.pipe(
    Effect.flatMap(
      Option.match({
        onNone: () => Effect.void,
        onSome: (event) => annotateOn(event, fields),
      }),
    ),
  );

/** Record `error` as the failure of the current event; no-op when there is none. */
export const annotateError = (error: unknown): Effect.Effect<void> =>
  currentEvent.pipe(
    Effect.flatMap(
      Option.match({
        onNone: () => Effect.void,
        onSome: (event) => annotateErrorOn(event, error),
      }),
    ),
  );
