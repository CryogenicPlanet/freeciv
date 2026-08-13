import { Cause, Effect, Exit, Layer, Option } from 'effect';
import type { EvlogWideEvent, TelemetryInitError } from 'src/evlog-adapter.ts';
import {
  type Observability,
  ObservabilityTest,
  TelemetryCapture,
} from 'src/observability.ts';
import { telemetryConfigLayer } from 'src/config.ts';

export const captureLayer: Layer.Layer<
  Observability | TelemetryCapture,
  TelemetryInitError
> = Layer.provide(
  ObservabilityTest,
  telemetryConfigLayer({
    service: 'arena-telemetry-test',
    environment: 'test',
    ndjsonDir: '/nonexistent/@arena/telemetry/capture-only',
  }),
);

export const takeEvents: Effect.Effect<
  ReadonlyArray<EvlogWideEvent>,
  never,
  TelemetryCapture
> = Effect.flatMap(TelemetryCapture, (capture) => capture.takeEvents);

export interface ExitSummary {
  readonly value: Option.Option<unknown>;
  readonly failure: Option.Option<unknown>;
  readonly defect: Option.Option<unknown>;
  readonly interrupted: boolean;
}

export const summarize = <A, E>(exit: Exit.Exit<A, E>): ExitSummary =>
  Exit.isSuccess(exit)
    ? {
        value: Option.some(exit.value),
        failure: Option.none(),
        defect: Option.none(),
        interrupted: false,
      }
    : {
        value: Option.none(),
        failure: Cause.failureOption(exit.cause),
        defect: Cause.dieOption(exit.cause),
        interrupted: Exit.isInterrupted(exit),
      };
