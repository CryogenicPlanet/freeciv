import { Context, Effect, Layer, Option, Ref } from 'effect';
import { TelemetryConfig } from './config.ts';
import {
  emitWideEvent,
  type EvlogWideEvent,
  initializeEvlog,
  type TelemetryEmitError,
  type TelemetryInitError,
  type TelemetryWriteError,
  writeWideEvents,
} from './evlog-adapter.ts';
import type { SealedWideEvent } from './wide-event.ts';

export type TelemetryRecordError = TelemetryEmitError | TelemetryWriteError;

export interface ObservabilityService {
  readonly record: (
    event: SealedWideEvent,
  ) => Effect.Effect<Option.Option<EvlogWideEvent>, TelemetryRecordError>;
}

export class Observability extends Context.Tag('@arena/telemetry/Observability')<
  Observability,
  ObservabilityService
>() {}

export interface TelemetryCaptureService {
  readonly takeEvents: Effect.Effect<ReadonlyArray<EvlogWideEvent>>;
}

export class TelemetryCapture extends Context.Tag('@arena/telemetry/TelemetryCapture')<
  TelemetryCapture,
  TelemetryCaptureService
>() {}

/** Ordered, awaited NDJSON delivery. */
export const ObservabilityLive: Layer.Layer<Observability, TelemetryInitError, TelemetryConfig> =
  Layer.effect(
    Observability,
    Effect.gen(function* () {
      const config = yield* TelemetryConfig;
      yield* initializeEvlog(config);
      const writes = yield* Effect.makeSemaphore(1);
      return {
        record: (sealed) =>
          emitWideEvent(sealed, config).pipe(
            Effect.flatMap(
              Option.match({
                onNone: () => Effect.succeed(Option.none<EvlogWideEvent>()),
                onSome: (event) =>
                  writes
                    .withPermits(1)(writeWideEvents([event], config.ndjsonDir))
                    .pipe(Effect.as(Option.some(event))),
              }),
            ),
          ),
      };
    }),
  );

/** The same emission path as Live, captured in memory instead of written. */
export const ObservabilityTest: Layer.Layer<
  Observability | TelemetryCapture,
  TelemetryInitError,
  TelemetryConfig
> = Layer.effectContext(
  Effect.gen(function* () {
    const config = yield* TelemetryConfig;
    yield* initializeEvlog(config);
    const captured = yield* Ref.make<ReadonlyArray<EvlogWideEvent>>([]);
    return Context.make(Observability, {
      record: (sealed) =>
        emitWideEvent(sealed, config).pipe(
          Effect.tap(
            Option.match({
              onNone: () => Effect.void,
              onSome: (event) => Ref.update(captured, (events) => [...events, event]),
            }),
          ),
        ),
    }).pipe(
      Context.add(TelemetryCapture, {
        takeEvents: Ref.getAndSet<ReadonlyArray<EvlogWideEvent>>(captured, []),
      }),
    );
  }),
);

/** Telemetry disabled: no configuration, initialization, or filesystem work. */
export const ObservabilityNoop: Layer.Layer<Observability> = Layer.succeed(Observability, {
  record: () => Effect.succeed(Option.none<EvlogWideEvent>()),
});
