// oxlint-disable effecttsgo/async-function
// oxlint-disable effecttsgo/strict-effect-provide
import { describe, expect, test } from 'bun:test';
import {
  Data,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Logger,
  TestClock,
  TestContext,
} from 'effect';
import type { EvlogWideEvent } from 'src/evlog-adapter.ts';
import { TelemetryWriteError } from 'src/evlog-adapter.ts';
import { withWideEvent } from 'src/middleware.ts';
import { Observability } from 'src/observability.ts';
import { annotate } from 'src/wide-event.ts';
import { captureLayer, summarize, takeEvents } from 'test/support';

class TurnRefused extends Data.TaggedError('TurnRefused')<{
  readonly reason: string;
}> {}

const capturedExit = <A, E>(
  self: Effect.Effect<A, E>,
): Promise<{ readonly exit: Exit.Exit<A, E>; readonly events: ReadonlyArray<EvlogWideEvent> }> =>
  Effect.gen(function* () {
    const exit = yield* Effect.exit(withWideEvent(self, 'unit.work'));
    const events = yield* takeEvents;
    return { exit, events };
  }).pipe(Effect.provide(captureLayer), Effect.runPromise);

const only = (events: ReadonlyArray<EvlogWideEvent>): EvlogWideEvent => {
  expect(events).toHaveLength(1);
  return events[0]!;
};

describe('four exits', () => {
  test('success emits once and preserves the value', async () => {
    const self = Effect.succeed({ moves: 3 });
    const result = await capturedExit(self);
    expect(summarize(result.exit)).toEqual(summarize(await Effect.runPromiseExit(self)));
    expect(only(result.events)).toMatchObject({
      event: 'unit.work',
      outcome: 'success',
      level: 'info',
    });
  });

  test('typed failure emits once and preserves the same error', async () => {
    const refusal = new TurnRefused({ reason: 'no legal move' });
    const self = Effect.fail(refusal);
    const result = await capturedExit(self);
    expect(summarize(result.exit)).toEqual(summarize(await Effect.runPromiseExit(self)));
    expect(summarize(result.exit).failure).toEqual(summarize(await Effect.runPromiseExit(self)).failure);
    expect(only(result.events)).toMatchObject({
      outcome: 'failure',
      level: 'error',
      error: { code: 'TurnRefused', message: 'TurnRefused' },
    });
  });

  test('defect emits once and stays a defect', async () => {
    const defect = new RangeError('kaboom');
    const self = Effect.die(defect);
    const result = await capturedExit(self);
    expect(summarize(result.exit)).toEqual(summarize(await Effect.runPromiseExit(self)));
    expect(only(result.events)).toMatchObject({
      outcome: 'defect',
      level: 'error',
      error: { code: 'RangeError', message: 'kaboom' },
    });
  });

  test('interruption emits once and stays interruption', async () => {
    const program = Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const fiber = yield* Effect.fork(
        withWideEvent(
          Deferred.succeed(started, undefined).pipe(Effect.zipRight(Effect.never)),
          'unit.work',
        ),
      );
      yield* Deferred.await(started);
      const exit = yield* Fiber.interrupt(fiber);
      const events = yield* takeEvents;
      return { exit, events };
    });
    const result = await Effect.runPromise(program.pipe(Effect.provide(captureLayer)));
    expect(summarize(result.exit).interrupted).toBe(true);
    expect(only(result.events)).toMatchObject({ outcome: 'interrupt', level: 'info' });
  });
});

describe('annotation scope', () => {
  test('ambient fields reach the event and reserved fields cannot be shadowed', async () => {
    const result = await capturedExit(
      annotate({
        turn: 42,
        event: 'imposter',
        eventId: 'imposter',
        durationMs: -1,
        outcome: 'imposter',
        service: 'imposter',
        environment: 'imposter',
      }),
    );
    const event = only(result.events);
    expect(event['turn']).toBe(42);
    expect(event['event']).toBe('unit.work');
    expect(event['eventId']).not.toBe('imposter');
    expect(event['durationMs']).not.toBe(-1);
    expect(event['outcome']).toBe('success');
    expect(event['service']).toBe('arena-telemetry-test');
    expect(event['environment']).toBe('test');
  });

  test('outside a wide event annotate is a no-op', () => {
    expect(Effect.runSyncExit(annotate({ turn: 1 }))).toEqual(Exit.succeed(undefined));
  });

  test('concurrent roots are isolated', async () => {
    const program = Effect.all(
      ['alpha', 'beta', 'gamma'].map((name) =>
        withWideEvent(annotate({ who: name }), name),
      ),
      { concurrency: 3 },
    ).pipe(Effect.zipRight(takeEvents));
    const events = await Effect.runPromise(program.pipe(Effect.provide(captureLayer)));
    expect(events).toHaveLength(3);
    expect(
      events
        .map((event) => `${String(event['event'])}/${String(event['who'])}`)
        .toSorted(),
    ).toEqual(['alpha/alpha', 'beta/beta', 'gamma/gamma']);
  });

  test('forked children inherit their parent event', async () => {
    const result = await capturedExit(
      Effect.forEach(
        [1, 2, 3],
        (number) => annotate({ [`child${String(number)}`]: number }),
        { concurrency: 3 },
      ),
    );
    expect(only(result.events)).toMatchObject({ child1: 1, child2: 2, child3: 3 });
  });

  test('duration uses the Effect Clock', async () => {
    const program = Effect.gen(function* () {
      const fiber = yield* Effect.fork(withWideEvent(Effect.sleep('5 seconds'), 'slow.work'));
      yield* TestClock.adjust('5 seconds');
      yield* Fiber.join(fiber);
      return yield* takeEvents;
    });
    const events = await Effect.runPromise(
      program.pipe(Effect.provide(Layer.merge(captureLayer, TestContext.TestContext))),
    );
    expect(only(events)['durationMs']).toBe(5000);
  });

  test('capture drains between assertions', async () => {
    const program = Effect.gen(function* () {
      yield* withWideEvent(Effect.void, 'first');
      const first = yield* takeEvents;
      const empty = yield* takeEvents;
      return { first, empty };
    });
    const result = await Effect.runPromise(program.pipe(Effect.provide(captureLayer)));
    expect(result.first).toHaveLength(1);
    expect(result.empty).toHaveLength(0);
  });
});

const ObservabilityBroken = Layer.succeed(Observability, {
  record: () =>
    Effect.fail(new TelemetryWriteError({ dir: '/full', events: 1, cause: 'ENOSPC' })),
});

const ObservabilityRabid = Layer.succeed(Observability, {
  record: () => Effect.die(new Error('backend defect')),
});

const runWithWarnings = async (
  backend: Layer.Layer<Observability>,
): Promise<{ readonly exit: Exit.Exit<string>; readonly lines: ReadonlyArray<string> }> => {
  const lines: Array<string> = [];
  const logger = Logger.make<unknown, void>(({ message, annotations }) => {
    lines.push(`${String(message)} ${JSON.stringify(Object.fromEntries(annotations))}`);
  });
  const exit = await Effect.runPromiseExit(
    withWideEvent(Effect.succeed('unchanged'), 'unit.work').pipe(
      Effect.provide(
        Layer.merge(backend, Logger.replace(Logger.defaultLogger, logger)),
      ),
    ),
  );
  return { exit, lines };
};

describe('backend failures', () => {
  test('typed failure is swallowed and reported once', async () => {
    const result = await runWithWarnings(ObservabilityBroken);
    expect(result.exit).toEqual(Exit.succeed('unchanged'));
    expect(result.lines).toHaveLength(1);
    expect(result.lines[0]).toContain('dropped a wide event');
    expect(result.lines[0]).toContain('TelemetryWriteError');
    expect(result.lines[0]).toContain('unit.work');
  });

  test('backend defect is swallowed and reported once', async () => {
    const result = await runWithWarnings(ObservabilityRabid);
    expect(result.exit).toEqual(Exit.succeed('unchanged'));
    expect(result.lines).toHaveLength(1);
  });
});
