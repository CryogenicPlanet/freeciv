/**
 * Wrapping an effect in `withWideEvent` does not change the effect.
 *
 * Same value, same typed error, same defect, same interruption — against a
 * recording backend, against the no-op backend, and against a backend that
 * fails on every call. If any of these diverge, `@arena/telemetry` is not an
 * observer: it is a participant, and a program's behaviour depends on whether
 * telemetry is switched on. That is the failure mode this file exists to
 * prevent, and it is the reason `Effect.onExit`'s cleanup is typed
 * `Effect<X, never, R2>` — the compiler refuses to let `middleware.ts` forget.
 */

// `bun:test` bodies are promises, and every test is its own entry point: both
// diagnostics below describe correct-but-unidiomatic shapes that a test file
// cannot avoid. `strict-effect-provide` says so itself ("If this is an entry
// point, you can safely disable this diagnostic").
// oxlint-disable effecttsgo/async-function
// oxlint-disable effecttsgo/strict-effect-provide
import { describe, expect, test } from 'bun:test';
import { Data, Deferred, Effect, Exit, Fiber, Layer, Logger, LogLevel, Option } from 'effect';
import {
  Observability,
  ObservabilityNoop,
  type TelemetryInitError,
  TelemetryWriteError,
  withWideEvent,
} from 'src/index';
import { captureLayer, summarize } from 'test/support';

class TurnRefused extends Data.TaggedError('TurnRefused')<{ readonly reason: string }> {}

const refusal = new TurnRefused({ reason: 'no legal move' });
const defect = new RangeError('kaboom');

/**
 * A backend that fails every write.
 *
 * The interesting control: a telemetry failure must be *absorbed*, not
 * propagated. A package that turns a full disk into a failed turn is worse than
 * no package.
 */
const ObservabilityBroken: Layer.Layer<Observability> = Layer.succeed(Observability, {
  record: () =>
    Effect.fail(new TelemetryWriteError({ dir: '/full', events: 1, cause: 'ENOSPC' })),
});

/** A backend that dies rather than fails — the other half of the same promise. */
const ObservabilityRabid: Layer.Layer<Observability> = Layer.succeed(Observability, {
  record: () => Effect.die(new Error('the observability layer itself is broken')),
});

/**
 * Every backend is compared against the same four exits. `Layer<Observability>`
 * is the common supertype — `captureLayer` also provides `TelemetryCapture`,
 * which these tests do not need.
 */
const backends: ReadonlyArray<readonly [string, Layer.Layer<Observability, TelemetryInitError>]> = [
  ['capture', captureLayer],
  ['noop', ObservabilityNoop],
  ['broken', ObservabilityBroken],
  ['rabid', ObservabilityRabid],
];

/**
 * The broken backends log a warning per dropped event, which is exactly what
 * they should do and is a wall of noise in a test run. It is silenced here and
 * asserted on once, at the bottom of the file, where the assertion is the point.
 */
const quiet = <A, E, R>(self: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
  Logger.withMinimumLogLevel(self, LogLevel.None);

const suite = (name: string, layer: Layer.Layer<Observability, TelemetryInitError>): void => {
  describe(`with the ${name} backend`, () => {
    const run = <A, E>(self: Effect.Effect<A, E>): Promise<Exit.Exit<A, E | TelemetryInitError>> =>
      Effect.runPromiseExit(quiet(withWideEvent(self, 'unit.work').pipe(Effect.provide(layer))));

    const compare = async <A, E>(self: Effect.Effect<A, E>): Promise<void> => {
      const bare = await Effect.runPromiseExit(self);
      const wrapped = await run(self);
      expect(summarize(wrapped)).toEqual(summarize(bare));
    };

    test('success passes the value through', async () => {
      await compare(Effect.succeed({ moves: 3 }));
    });

    test('a typed failure passes the error through', async () => {
      await compare(Effect.fail(refusal));
      // The identical instance, not a copy: nothing in the pipeline reconstructs it.
      expect(summarize(await run(Effect.fail(refusal))).failure).toEqual(Option.some(refusal));
    });

    test('a defect stays a defect', async () => {
      await compare(Effect.die(defect));
      const summary = summarize(await run(Effect.die(defect)));
      // A defect must not be quietly demoted into the typed error channel.
      expect(summary.defect).toEqual(Option.some(defect));
      expect(Option.isNone(summary.failure)).toBe(true);
    });

    test('interruption stays interruption', async () => {
      const program = Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        const fiber = yield* Effect.fork(
          withWideEvent(
            Deferred.succeed(started, undefined).pipe(Effect.zipRight(Effect.never)),
            'unit.work',
          ),
        );
        yield* Deferred.await(started);
        yield* Fiber.interrupt(fiber);
        return yield* Fiber.await(fiber);
      });

      const exit = await Effect.runPromise(quiet(program.pipe(Effect.provide(layer))));
      const summary = summarize(exit);
      // Not converted to a failure, not swallowed into a success.
      expect(summary.interrupted).toBe(true);
      expect(Option.isNone(summary.failure)).toBe(true);
      expect(Option.isNone(summary.defect)).toBe(true);
    });

    test('the requirement added is Observability and nothing else', async () => {
      // If the wrapper leaked a Scope or a Clock requirement, this would not
      // compile — the assertion is the type, the runtime check is a formality.
      const program: Effect.Effect<number, never, Observability> = withWideEvent(
        Effect.succeed(1),
        'unit.work',
      );
      expect(await Effect.runPromise(quiet(program.pipe(Effect.provide(layer))))).toBe(1);
    });
  });
};

backends.forEach(([name, layer]) => suite(name, layer));

describe('telemetry failures are reported without changing the program result', () => {
  test('a corpus write loss is announced as a dropped event', async () => {
    const lines: Array<string> = [];
    const capture = Logger.make<unknown, void>(({ logLevel, message, annotations }) => {
      lines.push(
        `${logLevel.label} ${String(message)} ${JSON.stringify(Object.fromEntries(annotations))}`,
      );
    });

    const exit = await Effect.runPromiseExit(
      withWideEvent(Effect.succeed('fine'), 'unit.work').pipe(
        Effect.provide(Layer.merge(ObservabilityBroken, Logger.replace(Logger.defaultLogger, capture))),
      ),
    );

    expect(Exit.isSuccess(exit)).toBe(true);
    expect(exit).toEqual(Exit.succeed('fine'));
    expect(lines).toHaveLength(1);
    expect(lines[0]).toContain('dropped a wide event');
    expect(lines[0]).not.toContain('OTLP mirror failed');
    expect(lines[0]).toContain('TelemetryWriteError');
  });
});

describe('a dropped event is reported', () => {
  test('as exactly one warning, carrying the event it lost', async () => {
    // A corpus with a hole in it must not look identical to a complete one.
    // This is the one place that hole is announced.
    const lines: Array<string> = [];
    const capture = Logger.make<unknown, void>(({ logLevel, message, annotations }) => {
      lines.push(
        `${logLevel.label} ${String(message)} ${JSON.stringify(Object.fromEntries(annotations))}`,
      );
    });

    await Effect.runPromise(
      withWideEvent(Effect.succeed('fine'), 'unit.work').pipe(
        Effect.provide(Layer.merge(ObservabilityBroken, Logger.replace(Logger.defaultLogger, capture))),
      ),
    );

    expect(lines).toHaveLength(1);
    expect(lines[0]).toContain('WARN');
    expect(lines[0]).toContain('dropped a wide event');
    // Named, not anonymous: which unit of work, and which failure.
    expect(lines[0]).toContain('unit.work');
    expect(lines[0]).toContain('TelemetryWriteError');
    // And *which* write: the tag alone is the same word this backend would
    // print for any reason at all, so the payload it was carrying — the
    // directory, the count, the underlying cause — goes on the line too.
    expect(lines[0]).toContain('/full');
    expect(lines[0]).toContain('ENOSPC');
    expect(lines[0]).toContain('failure.events');
  });

  test('and a backend that dies is absorbed the same way', async () => {
    const lines: Array<string> = [];
    const capture = Logger.make<unknown, void>(({ message }) => {
      lines.push(String(message));
    });

    const exit = await Effect.runPromiseExit(
      withWideEvent(Effect.succeed('fine'), 'unit.work').pipe(
        Effect.provide(Layer.merge(ObservabilityRabid, Logger.replace(Logger.defaultLogger, capture))),
      ),
    );

    // A defect inside the observability layer stays inside it.
    expect(Exit.isSuccess(exit)).toBe(true);
    expect(lines).toHaveLength(1);
  });
});
