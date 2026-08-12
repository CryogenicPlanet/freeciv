/**
 * Shared rigging for the telemetry suite.
 *
 * Two things live here and nothing else: the capture layer every behavioural
 * test runs against, and {@link summarize}, which is how the suite compares two
 * `Exit`s without comparing their fiber ids.
 */
import { Cause, Effect, Exit, Layer, Option } from 'effect';
import {
  type EvlogWideEvent,
  type Observability,
  ObservabilityTest,
  TelemetryCapture,
  type TelemetryConfigInput,
  type TelemetryInitError,
  telemetryConfigLayer,
} from 'src/index';

/**
 * The configuration every test runs with.
 *
 * `ndjsonDir` is a placeholder: `ObservabilityTest` captures instead of
 * writing, so nothing ever touches this path. The one test that does write —
 * `observability.test.ts` — builds its own config over a real temporary
 * directory.
 */
export const TEST_CONFIG: TelemetryConfigInput = {
  service: 'arena-telemetry-test',
  environment: 'test',
  ndjsonDir: '/nonexistent/@arena/telemetry/capture-only',
};

/** `ObservabilityTest` with its configuration already supplied. */
export const captureLayer: Layer.Layer<Observability | TelemetryCapture, TelemetryInitError> =
  Layer.provide(ObservabilityTest, telemetryConfigLayer(TEST_CONFIG));

/** Everything captured since the last take. */
export const takeEvents: Effect.Effect<
  ReadonlyArray<EvlogWideEvent>,
  never,
  TelemetryCapture
> = Effect.flatMap(TelemetryCapture, (capture) => capture.takeEvents);

/** A comparable, fiber-id-free summary of how an effect ended. */
export interface ExitSummary {
  /** `success`, or `failure` for every other way an effect can end. */
  readonly kind: 'success' | 'failure';
  /** The success value, when there is one. */
  readonly value: Option.Option<unknown>;
  /** The typed error, when there is one. */
  readonly failure: Option.Option<unknown>;
  /** The defect, when there is one. */
  readonly defect: Option.Option<unknown>;
  /** Whether interruption is anywhere in the cause. */
  readonly interrupted: boolean;
}

/**
 * Reduce an `Exit` to what a caller can actually observe.
 *
 * A raw `Exit` from an interrupted fiber carries that fiber's id, so two runs
 * of the same program never compare equal. Everything a caller can act on —
 * the value, the typed error, the defect, whether it was interrupted — is here,
 * and nothing else is.
 */
export const summarize = <A, E>(exit: Exit.Exit<A, E>): ExitSummary => {
  if (Exit.isSuccess(exit)) {
    return {
      kind: 'success',
      value: Option.some(exit.value),
      failure: Option.none(),
      defect: Option.none(),
      interrupted: false,
    };
  }
  return {
    kind: 'failure',
    value: Option.none(),
    failure: Cause.failureOption(exit.cause),
    defect: Cause.dieOption(exit.cause),
    interrupted: Exit.isInterrupted(exit),
  };
};
