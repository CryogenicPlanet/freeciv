import { test, type TestOptions } from 'bun:test';
import { Effect, Layer } from 'effect';

/**
 * Provide a test layer inside a scope, so acquisition and release happen at
 * the test boundary rather than at each operation under test.
 */
export const provideTestLayer = <A, E, R, RProvided, ELayer, RIn>(
  effect: Effect.Effect<A, E, R>,
  layer: Layer.Layer<RProvided, ELayer, RIn>
): Effect.Effect<A, E | ELayer, RIn | Exclude<R, RProvided>> =>
  Effect.scoped(
    Effect.gen(function* () {
      const context = yield* Layer.build(layer);
      return yield* Effect.provide(effect, context);
    })
  );

/** Run an Effect-valued Bun test without introducing a Promise callback. */
export const effectTest = <E>(
  name: string,
  body: () => Effect.Effect<void, E>,
  options?: number | TestOptions
): void => {
  test(name, () => Effect.runPromise(Effect.orDie(body())), options);
};

export interface TestAwait {
  <A>(promise: PromiseLike<A>): Effect.Effect<A>;
  <A, E>(effect: Effect.Effect<A, E>): Effect.Effect<A>;
}

const testAwait: TestAwait = <A, E>(
  input: PromiseLike<A> | Effect.Effect<A, E>
): Effect.Effect<A> =>
  Effect.isEffect(input) ? Effect.orDie(input) : Effect.promise(() => input);

/**
 * Adapt existing Promise-returning test helpers once at Bun's runner boundary.
 * Each test remains a typed generator, so differently typed waits retain their
 * own result types without declaring an async function.
 */
export const awaitTest = <A>(
  name: string,
  body: (wait: TestAwait) => Effect.Effect<A, object> | Effect.fn.Return<A, object>,
  options?: number | TestOptions
): void => {
  test(
    name,
    () => {
      const result = body(testAwait);
      const effect = Effect.isEffect(result) ? result : Effect.gen(() => result);
      return Effect.runPromise(Effect.orDie(effect));
    },
    options
  );
};
