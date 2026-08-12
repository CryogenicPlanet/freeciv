/**
 * `@arena/telemetry` — one wide event per unit of work, written to an NDJSON
 * corpus.
 *
 * ```ts
 * import { Effect, Layer } from 'effect';
 * import { ObservabilityLive, annotate, telemetryConfigLayer, withWideEvent } from '@arena/telemetry';
 *
 * const telemetry = ObservabilityLive.pipe(
 *   Layer.provide(telemetryConfigLayer({
 *     service: 'arena-harness',
 *     environment: 'production',
 *     ndjsonDir: `${runDir}/events`,
 *   })),
 * );
 *
 * const playTurn = Effect.gen(function* () {
 *   yield* annotate({ turn: 42, nation: 'Romans' });   // no event? no-op.
 *   return yield* decide();
 * }).pipe(withWideEvent('turn.play'));
 *
 * playTurn.pipe(Effect.provide(telemetry));
 * ```
 *
 * That is the whole surface. `withWideEvent` opens an event, `annotate` writes
 * to whichever event the fiber is in, and the event is sealed and emitted
 * exactly once however the effect ends — including when it is interrupted.
 *
 * ## What this package promises
 *
 * - **Exactly one event per wrapped effect**, on success, typed failure,
 *   defect and interruption alike. The seal is a compare-and-set on a `Ref`,
 *   so it is a property of the code rather than of the review.
 * - **The wrapped effect is unchanged.** Same value, same error, same cause.
 *   Every telemetry failure is caught inside the finalizer and logged.
 * - **Every refusal names its remedy.** Failures are mapped through
 *   `./error-map.ts` into `{ message, why, fix }` before they are recorded, so
 *   a corpus row says what to do and not only what broke. Telemetry-only —
 *   those strings name internal files and never belong in a response.
 * - **No ambient configuration.** The run directory, service identity, build
 *   identity and optional collector are all explicit. `evlog` probes the
 *   environment on `initLogger` whatever it is handed — `APP_VERSION`,
 *   `GITHUB_SHA`, `AWS_REGION` — so this package stamps all five identity
 *   fields onto the finished event from its own configuration instead of
 *   trusting that state. Two recordings of the same program have the same field
 *   set on a laptop and in CI, which is what makes them comparable at all.
 *
 * ## Layout
 *
 * | Module | What lives there |
 * |---|---|
 * | `./config.ts` | `TelemetryConfig` — where events go and what they are stamped with |
 * | `./wide-event.ts` | the event, the `FiberRef` that carries it, and `annotate` |
 * | `./evlog-adapter.ts` | the only `Effect.try`/`tryPromise` in the package, and the sealed-once guard |
 * | `./error-map.ts` | tag → `{ why, fix }`, with an honest fallback |
 * | `./observability.ts` | the backend tag and its three layers |
 * | `./middleware.ts` | `withWideEvent` |
 *
 * @module
 */

/** Identity of this package, used by the harness to report its stack. */
export const TELEMETRY_PACKAGE = '@arena/telemetry' as const;

export {
  makeTelemetryConfig,
  TelemetryConfig,
  type TelemetryConfigInput,
  type TelemetryConfigShape,
  telemetryConfigLayer,
} from './config.ts';

export {
  causeOf,
  describeDetail,
  describeMessage,
  describeTag,
  errorFields,
  isRegisteredTag,
  type Remedy,
  REMEDIES,
  remedyFor,
  renderValue,
  tagOf,
  toEvlogError,
  UNREADABLE_FAILURE_TAG,
} from './error-map.ts';

export {
  describeFailure,
  emitWideEvent,
  type EvlogWideEvent,
  exportWideEvents,
  type FailureReport,
  initializeEvlog,
  readTelemetryFields,
  sealWideEvent,
  TelemetryEmitError,
  type TelemetryError,
  TelemetryExportError,
  TelemetryInitError,
  TelemetryWriteError,
  toEvlogErrorSafely,
  WideEventAlreadySealedError,
  writeWideEvents,
} from './evlog-adapter.ts';

export { withWideEvent, type WideEventOutcome } from './middleware.ts';

export {
  Observability,
  ObservabilityLive,
  ObservabilityNoop,
  type ObservabilityService,
  ObservabilityTest,
  TelemetryCapture,
  type TelemetryCaptureService,
  type TelemetryRecordError,
} from './observability.ts';

export {
  annotate,
  annotateError,
  annotateErrorOn,
  annotateOn,
  currentEvent,
  currentWideEvent,
  makeWideEvent,
  type SealedWideEvent,
  type TelemetryFields,
  type WideEvent,
} from './wide-event.ts';
