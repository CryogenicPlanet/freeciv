/**
 * What this package is pointed at: an identity to stamp on every event, a
 * directory to write NDJSON into, and — optionally — an OTLP collector to
 * mirror to.
 *
 * The whole configuration is one immutable value behind one {@link TelemetryConfig}
 * tag.  Nothing here reads `process.env` and nothing here has a default: a run
 * directory guessed from the environment is a run directory that silently
 * writes a corpus somewhere nobody looks, and `evlog`'s own adapters already
 * do enough env-probing on their own (`EVLOG_FS_DIR`, `OTEL_EXPORTER_OTLP_ENDPOINT`,
 * and a dynamic `import('nitropack/runtime')` when a field is left unresolved).
 * Passing every field explicitly is what keeps *those* probes from running.
 *
 * ## What passing every field explicitly does **not** buy
 *
 * `initLogger` probes the environment unconditionally, whatever it is handed:
 * it calls `detectEnvironment()` (`evlog/dist/audit-DZIXf7Z8.mjs:68` →
 * `dist/utils.mjs:33`) and merges the result *under* the `env` we pass, reading
 * `APP_VERSION`, `COMMIT_SHA`/`GITHUB_SHA`/`VERCEL_GIT_COMMIT_SHA`/
 * `CF_PAGES_COMMIT_SHA` and `VERCEL_REGION`/`AWS_REGION`/`FLY_REGION`/`CF_REGION`.
 * There is no configuration that switches that off — `??` cannot be defeated by
 * passing `undefined`.
 *
 * So this package does not try to.  The identity fields are stamped onto the
 * finished event from *this* value, in `./evlog-adapter.ts`, after `emit()` has
 * had its say: {@link TelemetryConfigShape.version},
 * {@link TelemetryConfigShape.commitHash} and {@link TelemetryConfigShape.region}
 * are present on the event exactly when they are `Some` here, and absent
 * otherwise, on a laptop and in CI alike.  `None` means "not on the event",
 * never "whatever the environment says".  That is what makes the byte-level
 * corpus comparison this package exists to enable survive a `GITHUB_SHA`.
 *
 * Optional fields are `Option`, not `string | undefined`.  The package compiles
 * under `exactOptionalPropertyTypes`, where `{ version: undefined }` and `{}`
 * are different types; `Option` makes "absent" a value we can pattern-match on
 * instead of a hole in the object shape.  {@link makeTelemetryConfig} accepts
 * the ergonomic `?:` form at the boundary and normalizes once.
 *
 * @module
 */

import { Context, Layer, Option } from 'effect';

/**
 * The resolved configuration, as every module in this package sees it.
 *
 * @see {@link TelemetryConfigInput} for the shape callers write.
 */
export interface ResolvedTelemetryConfig {
  /**
   * `service` on every wide event, stamped from this value onto every event
   * this layer records.  Dashboards facet on this, so it should name the
   * *process* (`arena-harness`, `arena-play-cli`), not the module that happened
   * to emit.
   *
   * Stamped by us rather than left to `evlog`'s `state.env`, which is
   * process-global: two layers alive at once would otherwise both write
   * whichever service initialized last.
   */
  readonly service: string;
  /** `environment` on every wide event: `production`, `test`, a run id — your vocabulary. */
  readonly environment: string;
  /**
   * Directory the NDJSON corpus is appended to, normally a per-run directory.
   *
   * This is a *directory*, not a file: `evlog/fs` owns the file name and writes
   * `YYYY-MM-DD.jsonl` (UTC) inside it, creating the directory and a
   * `.gitignore` containing `*` on first write.
   *
   * One `appendFile` per recorded event, and the on-disk order is the order
   * events sealed — but only because `ObservabilityLive` holds a one-permit
   * semaphore across the write.  `writeBatchToFs` awaits `mkdir`, a `.gitignore`
   * check and a `stat` *before* its `appendFile`
   * (`evlog/dist/adapters/fs.mjs:100-107`), so two concurrent recorders can
   * otherwise reach `appendFile` in the opposite order to their seals.
   */
  readonly ndjsonDir: string;
  /**
   * Build identity, stamped as `version` when present — and *only* when present.
   *
   * `None` means the event carries no `version` field at all, even where
   * `APP_VERSION` is set in the environment.  @see the module docstring.
   */
  readonly version: Option.Option<string>;
  /**
   * Commit that produced this build, stamped as `commitHash` when present.
   *
   * `None` means the event carries no `commitHash`, even in CI where
   * `GITHUB_SHA` is always set.  Pass it explicitly to record it: an event's
   * field set must not depend on which machine wrote it.
   */
  readonly commitHash: Option.Option<string>;
  /**
   * Deployment region, stamped as `region` when present.
   *
   * `None` means the event carries no `region`, even where `AWS_REGION` and
   * friends are set.  @see {@link TelemetryConfigShape.commitHash}.
   */
  readonly region: Option.Option<string>;
  /**
   * An OTLP logs endpoint to mirror events to, in addition to — never instead
   * of — the NDJSON corpus.
   *
   * `None` is the normal case and the only case the tests exercise: with `None`
   * this package makes no network calls at all.  When it is `Some`, a failed
   * export surfaces as a `TelemetryExportError`, which `withWideEvent` logs and
   * swallows rather than letting telemetry change a program's outcome.
   */
  readonly otlpEndpoint: Option.Option<string>;
}

/** The shape callers write; {@link makeTelemetryConfig} normalizes it. */
export interface TelemetryConfigInput {
  /** @see {@link TelemetryConfigShape.service} */
  readonly service: string;
  /** @see {@link TelemetryConfigShape.environment} */
  readonly environment: string;
  /** @see {@link TelemetryConfigShape.ndjsonDir} */
  readonly ndjsonDir: string;
  /** @see {@link TelemetryConfigShape.version} */
  readonly version?: string;
  /** @see {@link TelemetryConfigShape.commitHash} */
  readonly commitHash?: string;
  /** @see {@link TelemetryConfigShape.region} */
  readonly region?: string;
  /** @see {@link TelemetryConfigShape.otlpEndpoint} */
  readonly otlpEndpoint?: string;
}

/** Normalize the ergonomic input shape into the `Option`-typed resolved shape. */
export const makeTelemetryConfig = (input: TelemetryConfigInput): ResolvedTelemetryConfig => ({
  service: input.service,
  environment: input.environment,
  ndjsonDir: input.ndjsonDir,
  version: Option.fromNullable(input.version),
  commitHash: Option.fromNullable(input.commitHash),
  region: Option.fromNullable(input.region),
  otlpEndpoint: Option.fromNullable(input.otlpEndpoint),
});

/**
 * Where this package's configuration comes from.
 *
 * Required by `ObservabilityLive` and `ObservabilityTest` — both build the same
 * `evlog` state from it, which is what makes the test layer's capture a
 * faithful stand-in for what the live layer would have written.
 */
export class TelemetryConfig extends Context.Tag('@arena/telemetry/TelemetryConfig')<
  TelemetryConfig,
  ResolvedTelemetryConfig
>() {}

/** Backward-compatible name for the resolved configuration contract. */
export type { ResolvedTelemetryConfig as 'TelemetryConfigShape' };

/** A `Layer` providing {@link TelemetryConfig} from a literal configuration. */
export const telemetryConfigLayer = (input: TelemetryConfigInput): Layer.Layer<TelemetryConfig> =>
  Layer.succeed(TelemetryConfig, makeTelemetryConfig(input));
