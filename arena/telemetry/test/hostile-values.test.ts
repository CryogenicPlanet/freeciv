/**
 * Values that fight back.
 *
 * `annotate()` takes `unknown`s from anywhere in the program, and a wide-event
 * package that is only correct for well-behaved values has two failure modes,
 * both of which are worse than the value:
 *
 * - **A defect where a no-op was promised.** `annotate` is documented as a
 *   no-op outside a unit of work and a field merge inside one. If a getter that
 *   throws turns the second case into a defect, then a program's outcome
 *   depends on whether telemetry is switched on — the one thing this package
 *   promises it never does.
 * - **The wrong remedy.** A `BigInt` in an event is an `annotate()` bug. If it
 *   is reported as a failed *write*, the operator reading the warning is sent
 *   to check disk space, and the corpus's hole is attributed to the wrong
 *   cause. The dropped-event warning is the only report of that hole; it has to
 *   be worth reading.
 */

// `bun:test` bodies are promises, and every test is its own entry point: both
// diagnostics below describe correct-but-unidiomatic shapes that a test file
// cannot avoid. `strict-effect-provide` says so itself ("If this is an entry
// point, you can safely disable this diagnostic").
// oxlint-disable effecttsgo/async-function
// oxlint-disable effecttsgo/strict-effect-provide
// oxlint-disable effecttsgo/node-builtin-import
import { afterAll, describe, expect, test } from 'bun:test';
import { mkdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Effect, Exit, Layer, Logger } from 'effect';
import {
  annotate,
  annotateError,
  ObservabilityLive,
  ObservabilityNoop,
  REMEDIES,
  telemetryConfigLayer,
  withWideEvent,
} from 'src/index';
import { captureLayer, takeEvents } from 'test/support';

const ROOT = join(tmpdir(), `arena-telemetry-hostile-${String(process.pid)}`);

afterAll(() => rm(ROOT, { recursive: true, force: true }));

/** An object whose own enumerable getter throws when anything copies it. */
const withThrowingGetter = (): Record<string, unknown> => ({
  safe: 1,
  get boom(): never {
    throw new TypeError('this getter refuses');
  },
});

/** A failure value that resists every attempt to describe it. */
const hostileFailure = (): unknown => ({
  get _tag(): never {
    throw new TypeError('no tag for you');
  },
});

/** Collects the log lines a program produces, message and annotations. */
const logging = <A, E, R>(
  self: Effect.Effect<A, E, R>,
  lines: Array<string>,
): Effect.Effect<A, E, R> =>
  self.pipe(
    Effect.provide(
      Logger.replace(
        Logger.defaultLogger,
        Logger.make<unknown, void>(({ message, annotations }) => {
          lines.push(
            `${String(message)} ${JSON.stringify(Object.fromEntries(annotations))}`,
          );
        }),
      ),
    ),
  );

describe('a value whose getter throws', () => {
  test('does not change what the wrapped effect returns', async () => {
    const hostile = withThrowingGetter();

    // The control: outside a unit of work, `annotate` never reads the value.
    const bare = await Effect.runPromiseExit(annotate(hostile).pipe(Effect.as('ok')));
    // Inside one, it does — and must still come back the same way.
    const wrapped = await Effect.runPromiseExit(
      withWideEvent(annotate(hostile).pipe(Effect.as('ok')), 'unit.work').pipe(
        Effect.provide(ObservabilityNoop),
      ),
    );

    expect(Exit.isSuccess(bare)).toBe(true);
    expect(wrapped).toEqual(bare);
  });

  test('costs that one field and no other', async () => {
    const events = await Effect.runPromise(
      annotate(withThrowingGetter()).pipe(
        Effect.zipRight(annotate({ turn: 7 })),
        withWideEvent('unit.work'),
        Effect.zipRight(takeEvents),
        Effect.provide(captureLayer),
      ),
    );

    expect(events).toHaveLength(1);
    // The readable siblings survive; the unreadable one is recorded as
    // unreadable rather than silently dropped, so the corpus says what happened.
    expect(events[0]?.['safe']).toBe(1);
    expect(events[0]?.['turn']).toBe(7);
    expect(String(events[0]?.['boom'])).toContain('unreadable value');
  });

  test('is also survivable as the failure of the unit of work', async () => {
    const events = await Effect.runPromise(
      annotateError(hostileFailure()).pipe(
        withWideEvent('unit.work'),
        Effect.zipRight(takeEvents),
        Effect.provide(captureLayer),
      ),
    );

    expect(events).toHaveLength(1);
    // Recorded under a tag the remedy table has an entry for: an unreadable
    // failure is a fact about the program, not a reason to record nothing.
    expect(events[0]?.['error']).toMatchObject({
      code: 'UnreadableFailure',
      why: REMEDIES['UnreadableFailure']?.why,
    });
  });
});

describe('a value JSON refuses', () => {
  const bigintRun = async (): Promise<ReadonlyArray<string>> => {
    const dir = join(ROOT, 'bigint');
    await mkdir(dir, { recursive: true });
    const lines: Array<string> = [];

    const exit = await Effect.runPromiseExit(
      logging(
        withWideEvent(annotate({ big: 1n }).pipe(Effect.as('ok')), 'unit.work').pipe(
          Effect.provide(
            Layer.provide(
              ObservabilityLive,
              telemetryConfigLayer({ service: 's', environment: 'e', ndjsonDir: dir }),
            ),
          ),
        ),
        lines,
      ),
    );

    // Whatever else is true, the unit of work returned its value.
    expect(exit).toEqual(Exit.succeed('ok'));
    return lines;
  };

  test('is reported as an emit failure, with the remedy that names the cause', async () => {
    const lines = await bigintRun();

    expect(lines).toHaveLength(1);
    // Not `TelemetryWriteError`: nothing is wrong with the disk, and its remedy
    // ("check the volume has space") would send the reader nowhere.
    expect(lines[0]).toContain('TelemetryEmitError');
    expect(lines[0]).not.toContain('TelemetryWriteError');
    expect(lines[0]).toContain(REMEDIES['TelemetryEmitError']?.fix ?? '<missing remedy>');
  });

  test('and the warning says which value, not only which tag', async () => {
    const lines = await bigintRun();

    // The failure this warning is announcing is `BigInt`-shaped, and the line
    // says so. Before, `reason` and `detail` were both the tag, and a full disk
    // and this produced identical text.
    expect(lines[0]).toContain('BigInt');
    expect(lines[0]).toContain('failure.cause');
    expect(lines[0]).toContain('unit.work');
  });
});

describe('a failed write', () => {
  test('reports the directory it could not write to', async () => {
    const lines: Array<string> = [];

    await Effect.runPromise(
      logging(
        withWideEvent(Effect.succeed('ok'), 'unit.work').pipe(
          Effect.provide(
            Layer.provide(
              ObservabilityLive,
              telemetryConfigLayer({
                service: 's',
                environment: 'e',
                // A path under a regular file: `mkdir` cannot create it.
                ndjsonDir: '/dev/null/nowhere',
              }),
            ),
          ),
        ),
        lines,
      ),
    );

    expect(lines).toHaveLength(1);
    expect(lines[0]).toContain('TelemetryWriteError');
    // The payload the error carried all along, on the line that lost the event.
    expect(lines[0]).toContain('/dev/null/nowhere');
    expect(lines[0]).toContain('failure.events');
    expect(lines[0]).toContain(REMEDIES['TelemetryWriteError']?.fix ?? '<missing remedy>');
  });
});
