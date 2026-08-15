import { Clock, Data, Effect, Option, Ref } from 'effect';
import type { AuditableLogger, EvlogError, WideEvent as EvlogWideEvent } from 'evlog';
import { createLogger, initLogger } from 'evlog';
import { writeBatchToFs } from 'evlog/fs';
import type { ResolvedTelemetryConfig } from './config.ts';
import { toEvlogError } from './error-map.ts';
import type { SealedWideEvent, TelemetryFields, WideEvent } from './wide-event.ts';

export type { EvlogWideEvent };

export class TelemetryInitError extends Data.TaggedError('TelemetryInitError')<{
  readonly service: string;
  readonly cause: unknown;
}> {}

export class TelemetryEmitError extends Data.TaggedError('TelemetryEmitError')<{
  readonly eventId: string;
  readonly name: string;
  readonly cause: unknown;
}> {}

export class TelemetryWriteError extends Data.TaggedError('TelemetryWriteError')<{
  readonly dir: string;
  readonly events: number;
  readonly cause: unknown;
}> {}

export type TelemetryError = TelemetryInitError | TelemetryEmitError | TelemetryWriteError;

/** Initialize evlog without a drain; writes are awaited explicitly below. */
export const initializeEvlog = (
  config: ResolvedTelemetryConfig,
): Effect.Effect<void, TelemetryInitError> =>
  Effect.try({
    try: () =>
      initLogger({
        enabled: true,
        silent: true,
        pretty: false,
        stringify: true,
        redact: false,
        _suppressDrainWarning: true,
        env: {
          service: config.service,
          environment: config.environment,
        },
      }),
    catch: (cause) => new TelemetryInitError({ service: config.service, cause }),
  });

/** Take the sole immutable snapshot used by withWideEvent's finalizer. */
export const snapshotWideEvent = (event: WideEvent): Effect.Effect<SealedWideEvent> =>
  Effect.all({
    now: Clock.currentTimeMillis,
    fields: Ref.get(event.fields),
    error: Ref.get(event.error),
  }).pipe(
    Effect.map((snapshot) => ({
      id: event.id,
      name: event.name,
      startedAt: event.startedAt,
      durationMs: snapshot.now - event.startedAt,
      fields: snapshot.fields,
      error: snapshot.error,
    })),
  );

const toEvlogContext = (event: SealedWideEvent) => ({
  ...event.fields,
  event: event.name,
  eventId: event.id,
  durationMs: event.durationMs,
});

const applyError = (logger: AuditableLogger, error: Option.Option<EvlogError>): void => {
  if (Option.isSome(error)) logger.error(error.value);
};

const formatDuration = (milliseconds: number): string =>
  milliseconds < 1000
    ? `${Math.round(milliseconds)}ms`
    : `${(milliseconds / 1000).toFixed(2)}s`;

const AMBIENT_IDENTITY = new Set(['version', 'commitHash', 'region']);

/** Override evlog's process-global identity with this layer's explicit identity. */
const stampIdentity = (
  event: EvlogWideEvent,
  durationMs: number,
  config: ResolvedTelemetryConfig,
): EvlogWideEvent => ({
  ...Object.fromEntries(
    Object.entries(event).filter(([key]) => !AMBIENT_IDENTITY.has(key)),
  ),
  timestamp: event.timestamp,
  level: event.level,
  service: config.service,
  environment: config.environment,
  durationMs,
  duration: formatDuration(durationMs),
});

const proveSerializable = (event: EvlogWideEvent): EvlogWideEvent => {
  JSON.stringify(event);
  return event;
};

/** Translate through evlog and reject unserializable event values before delivery. */
export const emitWideEvent = (
  sealed: SealedWideEvent,
  config: ResolvedTelemetryConfig,
): Effect.Effect<Option.Option<EvlogWideEvent>, TelemetryEmitError> =>
  Effect.try({
    try: () => {
      const logger = createLogger(toEvlogContext(sealed));
      applyError(logger, sealed.error);
      return Option.map(Option.fromNullable(logger.emit()), (event) =>
        proveSerializable(stampIdentity(event, sealed.durationMs, config)),
      );
    },
    catch: (cause) => new TelemetryEmitError({
      eventId: sealed.id,
      name: sealed.name,
      cause,
    }),
  });

/** Append complete, compact NDJSON records. */
export const writeWideEvents = (
  events: ReadonlyArray<EvlogWideEvent>,
  dir: string,
): Effect.Effect<void, TelemetryWriteError> =>
  Effect.tryPromise({
    try: () => writeBatchToFs([...events], { dir, pretty: false }),
    catch: (cause) => new TelemetryWriteError({ dir, events: events.length, cause }),
  });

const copyFields = (fields: TelemetryFields): TelemetryFields => {
  try {
    return { ...fields };
  } catch {
    let keys: ReadonlyArray<string>;
    try {
      keys = Object.keys(fields);
    } catch {
      return {};
    }
    const copied: Record<string, TelemetryFields[string]> = {};
    for (const key of keys) {
      try {
        copied[key] = fields[key];
      } catch (error) {
        copied[key] = `@arena/telemetry: unreadable value (${error instanceof Error ? error.name : 'threw'})`;
      }
    }
    return copied;
  }
};

/** Copy caller-owned fields without allowing hostile getters to defect. */
export const readTelemetryFields = (fields: TelemetryFields): Effect.Effect<TelemetryFields> =>
  Effect.sync(() => copyFields(fields));

/** Normalize failure values without allowing hostile getters to defect. */
export const toEvlogErrorSafely = (cause: unknown): Effect.Effect<EvlogError> =>
  Effect.succeed(toEvlogError(cause));
