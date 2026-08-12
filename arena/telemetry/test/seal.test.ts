/**
 * The sealed-once guard.
 *
 * `withWideEvent` seals from a single finalizer, so in normal use the guard
 * never fires. It exists for the case where it does: an event handed somewhere
 * it should not have been, and two owners each believing they are finishing the
 * unit of work. The guard makes the second one *fail* rather than hand back a
 * snapshot of an event that was already emitted — because the second owner's
 * annotations are, by then, already lost, and telling it otherwise would turn a
 * visible bug into an invisible one.
 */

// `bun:test` bodies are promises, and every test is its own entry point: both
// diagnostics below describe correct-but-unidiomatic shapes that a test file
// cannot avoid. `strict-effect-provide` says so itself ("If this is an entry
// point, you can safely disable this diagnostic").
// oxlint-disable effecttsgo/async-function
// oxlint-disable effecttsgo/strict-effect-provide
import { describe, expect, test } from 'bun:test';
import { Cause, Effect, Exit, Option, Ref, TestClock, TestContext } from 'effect';
import {
  annotateErrorOn,
  annotateOn,
  makeWideEvent,
  sealWideEvent,
  WideEventAlreadySealedError,
} from 'src/index';

describe('sealWideEvent', () => {
  test('the first seal snapshots the event', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const event = yield* makeWideEvent('unit.work');
        yield* annotateOn(event, { turn: 3 });
        return yield* sealWideEvent(event);
      }),
    );

    expect(result.name).toBe('unit.work');
    expect(result.fields).toEqual({ turn: 3 });
    expect(Option.isNone(result.error)).toBe(true);
  });

  test('the second seal fails rather than handing back a snapshot', async () => {
    const exit = await Effect.runPromiseExit(
      Effect.gen(function* () {
        const event = yield* makeWideEvent('unit.work');
        yield* sealWideEvent(event);
        return yield* sealWideEvent(event);
      }),
    );

    // A typed failure in the error channel — not a defect, and emphatically not
    // a success carrying a second snapshot of an event that already went out.
    expect(Exit.isFailure(exit)).toBe(true);
    expect(
      Exit.isFailure(exit) ? Option.map(Cause.failureOption(exit.cause), (e) => e._tag) : undefined,
    ).toEqual(Option.some('WideEventAlreadySealed'));
  });

  test('the failure carries the event identity, not just a tag', async () => {
    const outcome = await Effect.runPromise(
      Effect.gen(function* () {
        const event = yield* makeWideEvent('unit.work');
        yield* sealWideEvent(event);
        const second = yield* Effect.either(sealWideEvent(event));
        return { event, second };
      }),
    );

    expect(outcome.second._tag).toBe('Left');
    const error = outcome.second._tag === 'Left' ? outcome.second.left : undefined;
    expect(error).toBeInstanceOf(WideEventAlreadySealedError);
    expect(error?.eventId).toBe(outcome.event.id);
    expect(error?.name).toBe('unit.work');
  });

  test('exactly one of two racing sealers wins', async () => {
    // `Ref.modify` is the whole guard: read-and-set in one step, so two fibers
    // cannot both observe `sealed === false`.
    const results = await Effect.runPromise(
      Effect.gen(function* () {
        const event = yield* makeWideEvent('unit.work');
        return yield* Effect.all(
          [
            Effect.exit(sealWideEvent(event)),
            Effect.exit(sealWideEvent(event)),
            Effect.exit(sealWideEvent(event)),
          ],
          { concurrency: 3 },
        );
      }),
    );

    expect(results.filter(Exit.isSuccess)).toHaveLength(1);
    expect(results.filter(Exit.isFailure)).toHaveLength(2);
  });

  test('annotation after the seal is ignored, not an error', async () => {
    // A fiber that outlived its unit of work is not a defect. Its late writes
    // are simply not observations of anything that is still being recorded.
    const after = await Effect.runPromise(
      Effect.gen(function* () {
        const event = yield* makeWideEvent('unit.work');
        yield* annotateOn(event, { turn: 3 });
        const sealed = yield* sealWideEvent(event);
        yield* annotateOn(event, { turn: 99, late: true });
        yield* annotateErrorOn(event, new Error('too late'));
        return {
          sealed,
          fields: yield* Ref.get(event.fields),
          error: yield* Ref.get(event.error),
        };
      }),
    );

    expect(after.sealed.fields).toEqual({ turn: 3 });
    expect(after.fields).toEqual({ turn: 3 });
    expect(Option.isNone(after.error)).toBe(true);
  });

  test('durationMs comes from the Clock service', async () => {
    const sealed = await Effect.runPromise(
      Effect.gen(function* () {
        const event = yield* makeWideEvent('unit.work');
        yield* TestClock.adjust('90 seconds');
        return yield* sealWideEvent(event);
      }).pipe(Effect.provide(TestContext.TestContext)),
    );

    expect(sealed.startedAt).toBe(0);
    expect(sealed.durationMs).toBe(90_000);
  });
});
