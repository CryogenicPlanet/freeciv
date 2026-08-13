import { Clock, Effect, FiberRef, Option, Random, Ref } from 'effect';
import type { EvlogError, WideEvent as EvlogWideEvent } from 'evlog';
import { readTelemetryFields, toEvlogErrorSafely } from './evlog-adapter.ts';

type TelemetryFieldValue = EvlogWideEvent[string];
export type TelemetryFields = Readonly<Record<string, TelemetryFieldValue>>;

/** Mutable state scoped to one unit of work. */
export interface WideEvent {
  readonly id: string;
  readonly name: string;
  readonly startedAt: number;
  readonly fields: Ref.Ref<TelemetryFields>;
  readonly error: Ref.Ref<Option.Option<EvlogError>>;
}

/** Immutable value delivered by an observability backend. */
export interface SealedWideEvent {
  readonly id: string;
  readonly name: string;
  readonly startedAt: number;
  readonly durationMs: number;
  readonly fields: TelemetryFields;
  readonly error: Option.Option<EvlogError>;
}

/** Fiber-local current event; children inherit it and sibling roots do not. */
export const currentWideEvent: FiberRef.FiberRef<Option.Option<WideEvent>> = FiberRef.unsafeMake(
  Option.none<WideEvent>(),
);

const randomEventId: Effect.Effect<string> = Effect.map(
  Effect.all([Random.nextInt, Random.nextInt]),
  ([high, low]) =>
    `${(high >>> 0).toString(16).padStart(8, '0')}${(low >>> 0).toString(16).padStart(8, '0')}`,
);

export const makeWideEvent = (name: string): Effect.Effect<WideEvent> =>
  Effect.all({
    id: randomEventId,
    startedAt: Clock.currentTimeMillis,
    fields: Ref.make<TelemetryFields>({}),
    error: Ref.make<Option.Option<EvlogError>>(Option.none()),
  }).pipe(Effect.map((parts) => ({ ...parts, name })));

export const annotateOn = (event: WideEvent, fields: TelemetryFields): Effect.Effect<void> =>
  readTelemetryFields(fields).pipe(
    Effect.flatMap((safe) =>
      Ref.update(event.fields, (current) => ({ ...current, ...safe })),
    ),
  );

export const annotateErrorOn = (event: WideEvent, cause: unknown): Effect.Effect<void> =>
  toEvlogErrorSafely(cause).pipe(
    Effect.flatMap((error) => Ref.set(event.error, Option.some(error))),
  );

/** Merge fields into the current event; outside an event this is a no-op. */
export const annotate = (fields: TelemetryFields): Effect.Effect<void> =>
  FiberRef.get(currentWideEvent).pipe(
    Effect.flatMap(
      Option.match({
        onNone: () => Effect.void,
        onSome: (event) => annotateOn(event, fields),
      }),
    ),
  );
