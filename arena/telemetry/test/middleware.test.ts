/**
 * `withWideEvent` — exactly one event, on every way an effect can end.
 *
 * The four exits are not four variations on a theme. Success and typed failure
 * are the paths anyone writes a test for; **defect** and **interruption** are
 * the paths that decide whether a telemetry package is useful during an
 * incident, and they are the two that a `tap`-based implementation silently
 * misses. Each has its own test below, and each asserts the same two things:
 * one event, and the right `outcome` on it.
 */

// `bun:test` bodies are promises, and every test is its own entry point: both
// diagnostics below describe correct-but-unidiomatic shapes that a test file
// cannot avoid. `strict-effect-provide` says so itself ("If this is an entry
// point, you can safely disable this diagnostic").
// oxlint-disable effecttsgo/async-function
// oxlint-disable effecttsgo/strict-effect-provide
import { describe, expect, test } from 'bun:test';
import { Data, Deferred, Effect, Exit, Fiber, Layer, Option, TestClock, TestContext } from 'effect';
import { annotate, annotateError, TelemetryCapture, withWideEvent } from 'src/index';
import { captureLayer, takeEvents } from 'test/support';

class TurnRefused extends Data.TaggedError('TurnRefused')<{
  readonly reason: string;
}> {}

/** Run `self` under a fresh capture layer and return the events it produced. */
const eventsOf = <A, E>(self: Effect.Effect<A, E>): Promise<ReadonlyArray<Record<string, unknown>>> =>
  Effect.exit(withWideEvent(self, 'unit.work'))
    .pipe(
      Effect.zipRight(takeEvents),
      Effect.provide(captureLayer),
      Effect.map((events) => events.map((event) => ({ ...event }))),
      Effect.runPromise,
    );

const only = (events: ReadonlyArray<Record<string, unknown>>): Record<string, unknown> => {
  expect(events).toHaveLength(1);
  return events[0] ?? {};
};

describe('withWideEvent seals exactly once', () => {
  test('success', async () => {
    const event = only(await eventsOf(Effect.succeed(7)));
    expect(event['event']).toBe('unit.work');
    expect(event['outcome']).toBe('success');
    expect(event['level']).toBe('info');
    expect(event['error']).toBeUndefined();
  });

  test('typed failure', async () => {
    const event = only(await eventsOf(Effect.fail(new TurnRefused({ reason: 'no legal move' }))));
    expect(event['outcome']).toBe('failure');
    expect(event['level']).toBe('error');
    // `tapError` ran inside the FiberRef scope, so the ambient `annotateError`
    // found the event — and the tag was mapped to a remedy on the way in.
    // A `Data.TaggedError` has an empty `message`; its payload is its fields,
    // and those are what land in the corpus.
    expect(event['error']).toMatchObject({
      code: 'TurnRefused',
      message: 'TurnRefused',
      internal: { tag: 'TurnRefused', registeredRemedy: false, fields: { reason: 'no legal move' } },
    });
  });

  test('defect', async () => {
    const event = only(await eventsOf(Effect.die(new RangeError('kaboom'))));
    // No tap fires on a defect. This event exists only because the finalizer
    // reads the Exit rather than trusting the taps.
    expect(event['outcome']).toBe('defect');
    expect(event['level']).toBe('error');
    expect(event['error']).toMatchObject({ code: 'RangeError', message: 'kaboom' });
  });

  test('interruption', async () => {
    const program = Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const fiber = yield* Effect.fork(
        withWideEvent(
          Deferred.succeed(started, undefined).pipe(Effect.zipRight(Effect.never)),
          'unit.work',
        ),
      );
      // Wait for the body to actually be running: interrupting a fiber that has
      // not started yet would prove nothing about the finalizer.
      yield* Deferred.await(started);
      yield* Fiber.interrupt(fiber);
      return yield* takeEvents;
    });

    const events = await Effect.runPromise(program.pipe(Effect.provide(captureLayer)));

    expect(events).toHaveLength(1);
    // Interruption is the case `Effect.ensuring`-style cleanup loses, because
    // the cleanup is itself interrupted. `Effect.onExit` runs uninterruptibly.
    expect(events[0]?.['outcome']).toBe('interrupt');
    expect(events[0]?.['level']).toBe('info');
  });

  test('a second exit is impossible: the wrapped effect is entered once', async () => {
    // Belt and braces on top of the seal — an effect that succeeds and is then
    // interrupted while its finalizer runs still yields exactly one event.
    const program = Effect.gen(function* () {
      const fiber = yield* Effect.fork(withWideEvent(Effect.succeed('done'), 'unit.work'));
      yield* Fiber.join(fiber);
      yield* Fiber.interrupt(fiber);
      return yield* takeEvents;
    });
    const events = await Effect.runPromise(program.pipe(Effect.provide(captureLayer)));
    expect(events).toHaveLength(1);
  });
});

describe('annotation', () => {
  test('annotate reaches the event from inside the effect', async () => {
    const event = only(
      await eventsOf(annotate({ turn: 42, nation: 'Romans' }).pipe(Effect.as('ok'))),
    );
    expect(event['turn']).toBe(42);
    expect(event['nation']).toBe('Romans');
  });

  test('annotate outside any event is a no-op, not a failure', () => {
    // Instrumented code has to stay callable from uninstrumented callers.
    expect(Exit.isSuccess(Effect.runSyncExit(annotate({ turn: 1 })))).toBe(true);
    expect(Exit.isSuccess(Effect.runSyncExit(annotateError(new Error('nobody listening'))))).toBe(
      true,
    );
  });

  test('the wrapper\u2019s own keys cannot be shadowed by instrumentation', async () => {
    // `event`, `eventId` and `durationMs` are written after the caller's fields;
    // `outcome` is written by the finalizer, which runs after everything.
    const event = only(
      await eventsOf(
        annotate({
          event: 'imposter',
          eventId: 'imposter',
          durationMs: -1,
          outcome: 'imposter',
        }).pipe(Effect.as('ok')),
      ),
    );
    expect(event['event']).toBe('unit.work');
    expect(event['eventId']).not.toBe('imposter');
    expect(event['durationMs']).not.toBe(-1);
    expect(event['outcome']).toBe('success');
  });
});

describe('fiber isolation', () => {
  test('concurrent units of work get their own events', async () => {
    // Each fiber annotates with only its own name; a shared event would show
    // all three keys on one row.
    const program = Effect.all(
      [
        withWideEvent(annotate({ who: 'alpha' }), 'alpha'),
        withWideEvent(annotate({ who: 'beta' }), 'beta'),
        withWideEvent(annotate({ who: 'gamma' }), 'gamma'),
      ],
      { concurrency: 3 },
    ).pipe(Effect.zipRight(takeEvents));

    const events = await Effect.runPromise(program.pipe(Effect.provide(captureLayer)));

    expect(events).toHaveLength(3);
    // `name/who` per event: a shared event would show up as one row with three
    // `who` values fighting over the same key.
    const pairs = events.map((event) => `${String(event['event'])}/${String(event['who'])}`).toSorted();
    expect(pairs).toEqual(['alpha/alpha', 'beta/beta', 'gamma/gamma']);
  });

  test('a forked child annotates its parent unit of work', async () => {
    // The FiberRef's default fork behaviour copies the value to children, which
    // is what makes `Effect.forEach(..., { concurrency })` inside a unit of work
    // still describe that unit.
    const event = only(
      await eventsOf(
        Effect.forEach([1, 2, 3], (n) => annotate({ [`child${n}`]: n }), { concurrency: 3 }),
      ),
    );
    expect(event['child1']).toBe(1);
    expect(event['child2']).toBe(2);
    expect(event['child3']).toBe(3);
  });
});

describe('duration', () => {
  test('durationMs is measured on the Clock, so TestClock controls it', async () => {
    const program = Effect.gen(function* () {
      const fiber = yield* Effect.fork(withWideEvent(Effect.sleep('5 seconds'), 'slow.work'));
      yield* TestClock.adjust('5 seconds');
      yield* Fiber.join(fiber);
      return yield* takeEvents;
    });

    const events = await Effect.runPromise(
      program.pipe(Effect.provide(Layer.merge(captureLayer, TestContext.TestContext))),
    );

    expect(events).toHaveLength(1);
    // Exactly 5000, not "about 5000": nothing here reads a wall clock.
    expect(events[0]?.['durationMs']).toBe(5000);
  });
});

describe('capture accessor', () => {
  test('takeEvents drains, so one test cannot see another test’s events', async () => {
    const program = Effect.gen(function* () {
      yield* withWideEvent(Effect.void, 'first');
      const first = yield* takeEvents;
      const second = yield* takeEvents;
      yield* withWideEvent(Effect.void, 'third');
      const third = yield* Effect.flatMap(TelemetryCapture, (capture) => capture.peekEvents);
      return { first, second, third };
    });

    const result = await Effect.runPromise(program.pipe(Effect.provide(captureLayer)));
    expect(result.first).toHaveLength(1);
    expect(result.second).toHaveLength(0);
    expect(result.third).toHaveLength(1);
    expect(Option.fromNullable(result.third[0]?.['event'])).toEqual(Option.some('third'));
  });
});
