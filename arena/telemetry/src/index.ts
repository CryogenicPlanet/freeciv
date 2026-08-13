/** One wide event per unit of work, with ordered NDJSON delivery. */

export const TELEMETRY_PACKAGE = '@arena/telemetry' as const;

export { telemetryConfigLayer } from './config.ts';
export { withWideEvent } from './middleware.ts';
export {
  Observability,
  ObservabilityLive,
  ObservabilityNoop,
  ObservabilityTest,
  TelemetryCapture,
} from './observability.ts';
export { annotate } from './wide-event.ts';
