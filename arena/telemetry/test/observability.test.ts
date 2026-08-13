// oxlint-disable effecttsgo/async-function
// oxlint-disable effecttsgo/strict-effect-provide
// oxlint-disable effecttsgo/node-builtin-import
// oxlint-disable effecttsgo/process-env
import { afterAll, describe, expect, test } from 'bun:test';
import { mkdir, readdir, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Deferred, Effect, Fiber, Layer, Predicate, TestContext } from 'effect';
import { telemetryConfigLayer } from 'src/config.ts';
import type { EvlogWideEvent } from 'src/evlog-adapter.ts';
import { withWideEvent } from 'src/middleware.ts';
import {
  ObservabilityLive,
  ObservabilityNoop,
  ObservabilityTest,
  TelemetryCapture,
} from 'src/observability.ts';
import { annotate } from 'src/wide-event.ts';

const ROOT = join(tmpdir(), `arena-telemetry-${String(process.pid)}`);

const runDir = async (name: string): Promise<string> => {
  const dir = join(ROOT, name);
  await mkdir(dir, { recursive: true });
  return dir;
};

const readCorpus = async (dir: string): Promise<ReadonlyArray<EvlogWideEvent>> => {
  const names = (await readdir(dir)).filter((name) => name.endsWith('.jsonl')).toSorted();
  const files = await Promise.all(names.map((name) => readFile(join(dir, name), 'utf8')));
  return files
    .flatMap((text) => text.split('\n'))
    .filter((line) => line.trim() !== '')
    .map((line): EvlogWideEvent => JSON.parse(line));
};

const stable = (event: EvlogWideEvent | undefined): Partial<EvlogWideEvent> =>
  Object.fromEntries(
    Object.entries(event ?? {}).filter(([key]) => !['timestamp', 'eventId'].includes(key)),
  );

afterAll(async () => {
  for (const key of ['APP_VERSION', 'GITHUB_SHA', 'AWS_REGION']) {
    Reflect.deleteProperty(process.env, key);
  }
  await rm(ROOT, { recursive: true, force: true });
});

describe('live NDJSON', () => {
  test('writes one complete line per event in seal order', async () => {
    const dir = await runDir('ordered');
    const layer = Layer.provide(
      ObservabilityLive,
      telemetryConfigLayer({ service: 'arena-suite', environment: 'test', ndjsonDir: dir }),
    );
    const program = Effect.gen(function* () {
      const gate = yield* Deferred.make<void>();
      const fibers = yield* Effect.forEach(
        Array.from({ length: 20 }, (_, index) => index),
        (index) =>
          Effect.fork(
            withWideEvent(
              Deferred.await(gate).pipe(Effect.zipRight(annotate({ seq: index }))),
              'ordered.work',
            ),
          ),
      );
      yield* Deferred.succeed(gate, undefined);
      yield* Effect.forEach(fibers, Fiber.join);
    });
    await Effect.runPromise(program.pipe(Effect.provide(layer)));

    const corpus = await readCorpus(dir);
    expect(corpus).toHaveLength(20);
    expect(corpus.map((event) => event['seq'])).toEqual(
      Array.from({ length: 20 }, (_, index) => index),
    );
    expect(corpus.every((event) => Predicate.isString(event['eventId']))).toBe(true);
    expect(corpus[0]).toMatchObject({
      event: 'ordered.work',
      service: 'arena-suite',
      environment: 'test',
      outcome: 'success',
    });
  });

  test('Live and Capture expose the same event shape', async () => {
    const dir = await runDir('parity');
    const config = telemetryConfigLayer({
      service: 'arena-suite',
      environment: 'test',
      ndjsonDir: dir,
    });
    const work = annotate({ turn: 12, nation: 'Carthage', moves: ['a', 'b'] }).pipe(
      Effect.as('done'),
      withWideEvent('turn.play'),
    );
    await Effect.runPromise(
      work.pipe(
        Effect.provide(
          Layer.merge(Layer.provide(ObservabilityLive, config), TestContext.TestContext),
        ),
      ),
    );
    const captured = await Effect.runPromise(
      work.pipe(
        Effect.zipRight(Effect.flatMap(TelemetryCapture, (capture) => capture.takeEvents)),
        Effect.provide(
          Layer.merge(Layer.provide(ObservabilityTest, config), TestContext.TestContext),
        ),
      ),
    );
    const written = await readCorpus(dir);
    expect(written).toHaveLength(1);
    expect(captured).toHaveLength(1);
    expect(stable(captured[0])).toEqual(stable(written[0]));
  });
});

describe('identity isolation', () => {
  test('two live layers retain their own service and environment', async () => {
    const firstDir = await runDir('identity-first');
    const secondDir = await runDir('identity-second');
    const first = Layer.provide(
      ObservabilityLive,
      telemetryConfigLayer({ service: 'first', environment: 'prod-a', ndjsonDir: firstDir }),
    );
    const second = Layer.provide(
      ObservabilityLive,
      telemetryConfigLayer({ service: 'second', environment: 'prod-b', ndjsonDir: secondDir }),
    );
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const firstContext = yield* Layer.build(first);
          const secondContext = yield* Layer.build(second);
          yield* withWideEvent(Effect.void, 'first.work').pipe(Effect.provide(firstContext));
          yield* withWideEvent(Effect.void, 'second.work').pipe(Effect.provide(secondContext));
        }),
      ),
    );
    expect(await readCorpus(firstDir)).toMatchObject([
      { event: 'first.work', service: 'first', environment: 'prod-a' },
    ]);
    expect(await readCorpus(secondDir)).toMatchObject([
      { event: 'second.work', service: 'second', environment: 'prod-b' },
    ]);
  });

  test('ambient build identity cannot change the event shape', async () => {
    process.env['APP_VERSION'] = 'ambient-version';
    process.env['GITHUB_SHA'] = 'ambient-commit';
    process.env['AWS_REGION'] = 'ambient-region';
    const dir = await runDir('ambient');
    await Effect.runPromise(
      withWideEvent(annotate({ turn: 1 }), 'probe.work').pipe(
        Effect.provide(
          Layer.provide(
            ObservabilityLive,
            telemetryConfigLayer({ service: 'probe', environment: 'test', ndjsonDir: dir }),
          ),
        ),
      ),
    );
    const event = (await readCorpus(dir))[0]!;
    expect(event['service']).toBe('probe');
    expect(event['environment']).toBe('test');
    expect(event['version']).toBeUndefined();
    expect(event['commitHash']).toBeUndefined();
    expect(event['region']).toBeUndefined();
  });
});

describe('Noop', () => {
  test('requires no configuration and performs no delivery', async () => {
    const result = await Effect.runPromise(
      withWideEvent(annotate({ turn: 1 }).pipe(Effect.as(99)), 'turn.play').pipe(
        Effect.provide(ObservabilityNoop),
      ),
    );
    expect(result).toBe(99);
  });
});
