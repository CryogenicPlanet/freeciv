import { Context, Layer } from 'effect';

/** Explicit identity and destination for one telemetry layer. */
export interface TelemetryConfigInput {
  readonly service: string;
  readonly environment: string;
  readonly ndjsonDir: string;
}

export type ResolvedTelemetryConfig = TelemetryConfigInput;

export class TelemetryConfig extends Context.Tag('@arena/telemetry/TelemetryConfig')<
  TelemetryConfig,
  ResolvedTelemetryConfig
>() {}

/** Provide telemetry configuration without reading ambient process state. */
export const telemetryConfigLayer = (input: TelemetryConfigInput): Layer.Layer<TelemetryConfig> =>
  Layer.succeed(TelemetryConfig, { ...input });
