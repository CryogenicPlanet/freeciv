// oxlint-disable effecttsgo/async-function
// oxlint-disable effecttsgo/strict-effect-provide
// oxlint-disable effecttsgo/node-builtin-import
import { afterAll, describe, expect, test } from 'bun:test';
import { mkdir, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Effect, Exit, Layer, Logger } from 'effect';
import { telemetryConfigLayer } from 'src/config.ts';
import { withWideEvent } from 'src/middleware.ts';
import { ObservabilityLive, ObservabilityNoop } from 'src/observability.ts';
import { annotate } from 'src/wide-event.ts';
import { captureLayer, takeEvents } from 'test/support';

const ROOT = join(tmpdir(), `arena-telemetry-hostile-${String(process.pid)}`);

afterAll(() => rm(ROOT, { recursive: true, force: true }));

const throwingFields = () => ({
  safe: 1,
  get boom(): never {
    throw new TypeError('getter refuses');
  },
});

const withLogs = <A, E, R>(
  self: Effect.Effect<A, E, R>,
  lines: Array<string>,
): Effect.Effect<A, E, R> =>
  self.pipe(
    Effect.provide(
      Logger.replace(
        Logger.defaultLogger,
        Logger.make<unknown, void>(({ message, annotations }) => {
          lines.push(`${String(message)} ${JSON.stringify(Object.fromEntries(annotations))}`);
        }),
      ),
    ),
  );

describe('hostile annotations', () => {
  test('a throwing getter cannot change the result and only costs its field', async () => {
    const bare = await Effect.runPromiseExit(annotate(throwingFields()).pipe(Effect.as('ok')));
    const wrapped = await Effect.runPromiseExit(
      withWideEvent(annotate(throwingFields()).pipe(Effect.as('ok')), 'unit.work').pipe(
        Effect.provide(ObservabilityNoop),
      ),
    );
    expect(wrapped).toEqual(bare);

    const events = await Effect.runPromise(
      annotate(throwingFields()).pipe(
        withWideEvent('unit.work'),
        Effect.zipRight(takeEvents),
        Effect.provide(captureLayer),
      ),
    );
    expect(events).toHaveLength(1);
    expect(events[0]?.['safe']).toBe(1);
    expect(String(events[0]?.['boom'])).toContain('unreadable value');
  });

  test('an unreadable typed failure is still recorded', async () => {
    const hostile = {
      get _tag(): never {
        throw new TypeError('no tag for you');
      },
    };
    const events = await Effect.runPromise(
      Effect.exit(withWideEvent(Effect.fail(hostile), 'unit.work')).pipe(
        Effect.zipRight(takeEvents),
        Effect.provide(captureLayer),
      ),
    );
    expect(events).toHaveLength(1);
    expect(events[0]?.['error']).toMatchObject({ code: 'UnreadableFailure' });
  });
});

describe('delivery failures', () => {
  test('an unserializable event preserves the result, warns once, and writes no line', async () => {
    const dir = join(ROOT, 'bigint');
    await mkdir(dir, { recursive: true });
    const lines: Array<string> = [];
    const exit = await Effect.runPromiseExit(
      withLogs(
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
    expect(exit).toEqual(Exit.succeed('ok'));
    expect(lines).toHaveLength(1);
    expect(lines[0]).toContain('TelemetryEmitError');
    expect((await readdir(dir)).filter((name) => name.endsWith('.jsonl'))).toEqual([]);
  });

  test('a filesystem failure preserves the result and warns once', async () => {
    const lines: Array<string> = [];
    const exit = await Effect.runPromiseExit(
      withLogs(
        withWideEvent(Effect.succeed('ok'), 'unit.work').pipe(
          Effect.provide(
            Layer.provide(
              ObservabilityLive,
              telemetryConfigLayer({
                service: 's',
                environment: 'e',
                ndjsonDir: '/dev/null/nowhere',
              }),
            ),
          ),
        ),
        lines,
      ),
    );
    expect(exit).toEqual(Exit.succeed('ok'));
    expect(lines).toHaveLength(1);
    expect(lines[0]).toContain('TelemetryWriteError');
  });
});
