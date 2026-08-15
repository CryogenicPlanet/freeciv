import { Effect } from 'effect';

export interface CapturedOutput {
  readonly out: ReadonlyArray<string>;
  readonly err: ReadonlyArray<string>;
}

export interface CapturedResult<A> {
  readonly value: A;
  readonly captured: CapturedOutput;
}

/** Capture the process console while one Effect runs, restoring it on every exit. */
export const captureEffect = <A, E, R>(
  effect: Effect.Effect<A, E, R>
): Effect.Effect<CapturedResult<A>, E, R> =>
  Effect.suspend(() => {
    const out: string[] = [];
    const err: string[] = [];
    const originalLog = console.log;
    const originalError = console.error;
    console.log = (...parts: ReadonlyArray<unknown>) => out.push(parts.join(' '));
    console.error = (...parts: ReadonlyArray<unknown>) => err.push(parts.join(' '));
    return effect.pipe(
      Effect.map((value) => ({ value, captured: { out, err } })),
      Effect.ensuring(Effect.sync(() => {
        console.log = originalLog;
        console.error = originalError;
      }))
    );
  });
