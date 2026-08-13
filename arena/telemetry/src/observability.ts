/**
 * Where a sealed event goes: to disk, to a test's `Ref`, or nowhere.
 *
 * One tag, three layers, and — importantly — **one emission path**. All three
 * layers below are the same code up to the last step. `ObservabilityLive` and
 * `ObservabilityTest` both initialize `evlog` from the same
 * {@link TelemetryConfig} and both run the sealed event through the same
 * `emitWideEvent`; they differ only in what they do with the `WideEvent` that
 * comes back — write it, or keep it. That is what makes a test's assertion
 * about a captured event an assertion about what production would have
 * written, rather than about a parallel implementation that agrees with
 * production until the day it does not.
 *
 * ## Ordering
 *
 * The NDJSON corpus is written *before* the OTLP mirror, and a failed mirror
 * does not un-write the corpus. The file is the record of truth; the collector
 * is a convenience.
 *
 * Between events, the corpus is written under a one-permit semaphore, so the
 * order of the lines in the file is the order the events sealed. `evlog/fs`
 * does three awaits — `mkdir`, a `.gitignore` check, a `stat` — before its
 * `appendFile` (`evlog/dist/adapters/fs.mjs:100-107`), and each of them is a
 * point at which two concurrent recorders can swap places. The mirror is
 * deliberately left *outside* the permit: a slow collector must not hold up the
 * file that is the record of truth.
 *
 * @module
 */

import { Context, Effect, Layer, Option, Ref } from 'effect';
import { TelemetryConfig, type ResolvedTelemetryConfig } from './config.ts';
import {
  emitWideEvent,
  type EvlogWideEvent,
  exportWideEvents,
  initializeEvlog,
  type TelemetryEmitError,
  type TelemetryExportError,
  type TelemetryInitError,
  type TelemetryWriteError,
  writeWideEvents,
} from './evlog-adapter.ts';
import type { SealedWideEvent } from './wide-event.ts';

/** Everything that can go wrong while recording an event that was sealed successfully. */
export type TelemetryRecordError = TelemetryEmitError | TelemetryWriteError | TelemetryExportError;

/** The one thing an observability backend does. */
export interface ObservabilityService {
  /**
   * Emit and deliver a sealed event.
   *
   * Returns the `evlog` event that was delivered, or `None` when nothing was —
   * either because head sampling dropped it, or because this backend records
   * nothing at all. `None` is a success: it means "sealed, and deliberately not
   * recorded", never "try again".
   */
  readonly record: (
    event: SealedWideEvent,
  ) => Effect.Effect<Option.Option<EvlogWideEvent>, TelemetryRecordError>;
}

/** The observability backend in scope. Required by `withWideEvent`. */
export class Observability extends Context.Tag('@arena/telemetry/Observability')<
  Observability,
  ObservabilityService
>() {}

/** A test's window onto what {@link ObservabilityTest} captured. */
export interface TelemetryCaptureService {
  /**
   * Every event captured since the last take, oldest first, and clears the
   * buffer.
   *
   * Draining rather than peeking is the right default for assertions: a test
   * that asks "what happened during this call" wants the events from that call,
   * and a shared buffer that silently accumulates across a `describe` block is
   * the standard way for a telemetry test to start passing for the wrong
   * reason. Use {@link TelemetryCaptureService.peekEvents} when you genuinely
   * want the whole history.
   */
  readonly takeEvents: Effect.Effect<ReadonlyArray<EvlogWideEvent>>;
  /** Every event captured so far, oldest first, without clearing. */
  readonly peekEvents: Effect.Effect<ReadonlyArray<EvlogWideEvent>>;
}

/** Provided alongside {@link Observability} by {@link ObservabilityTest}. */
export class TelemetryCapture extends Context.Tag('@arena/telemetry/TelemetryCapture')<
  TelemetryCapture,
  TelemetryCaptureService
>() {}

/**
 * Mirror to OTLP when — and only when — an endpoint is configured.
 *
 * With no endpoint this package performs no network I/O whatsoever, which is
 * what lets the test suite run the real emission path with no collector and no
 * mocking.
 */
const mirror = (
  event: EvlogWideEvent,
  config: ResolvedTelemetryConfig,
): Effect.Effect<void, TelemetryExportError> =>
  Option.match(config.otlpEndpoint, {
    onNone: () => Effect.void,
    onSome: (endpoint) => exportWideEvents([event], endpoint, config.service),
  });

/**
 * The production backend: emit through `evlog`, append to the NDJSON corpus,
 * then mirror.
 *
 * `initializeEvlog` runs in the layer's acquire, so it runs once per built
 * layer no matter how many fibers record events — which is the contract
 * `initLogger` needs. The write semaphore is built there too, which makes it
 * per-layer state held in a closure rather than anything module-level: two
 * layers writing to two run directories do not queue behind each other.
 */
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
                    .pipe(Effect.zipRight(mirror(event, config)), Effect.as(Option.some(event))),
              }),
            ),
          ),
      };
    }),
  );

/**
 * The test backend: the live emission path, captured into a `Ref` instead of
 * written.
 *
 * Provides {@link Observability} *and* {@link TelemetryCapture} from one
 * `Ref`, which is why it is built with `Layer.effectContext` rather than two
 * `Layer.effect`s — two layers would each build their own `Ref` and the
 * accessor would read an empty one.
 *
 * It deliberately captures the `evlog` `WideEvent`, not this package's
 * `SealedWideEvent`: the interesting failures are in the translation between
 * the two — a field shadowed, a duration not stamped, an error that did not
 * raise the level — and a test that captured the input would see none of them.
 */
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
        peekEvents: Ref.get(captured),
      }),
    );
  }),
);

/**
 * The backend that records nothing.
 *
 * Not a stub — it is the supported way to run a program with telemetry
 * switched off, and the control in the test that proves `withWideEvent` does
 * not change what an effect returns. It requires no configuration and touches
 * no global state, so it is also the right choice for a library consumer that
 * has not opted in.
 */
export const ObservabilityNoop: Layer.Layer<Observability> = Layer.succeed(Observability, {
  record: () => Effect.succeed(Option.none<EvlogWideEvent>()),
});
