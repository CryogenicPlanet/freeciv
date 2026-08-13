/**
 * `ObservabilityTest` captures what `ObservabilityLive` would write.
 *
 * This is the test that makes every other test in the suite mean something. The
 * rest of them assert against events captured in memory; that is only worth
 * doing if a captured event is the same event that would have landed on disk.
 * So this file runs the same unit of work twice — once through the live layer
 * onto a real temporary directory, once through the test layer — and compares
 * the parsed NDJSON line against the captured object.
 *
 * The comparison excludes exactly two fields, both for stated reasons:
 * `timestamp`, which `evlog` stamps from the wall clock at emit, and `eventId`,
 * which is drawn from the `Random` service and differs per run by design.
 * Everything else — including the field set — must match.
 */

// `bun:test` bodies are promises, and every test is its own entry point: both
// diagnostics below describe correct-but-unidiomatic shapes that a test file
// cannot avoid. `strict-effect-provide` says so itself ("If this is an entry
// point, you can safely disable this diagnostic").
// oxlint-disable effecttsgo/async-function
// oxlint-disable effecttsgo/strict-effect-provide
import { afterAll, describe, expect, test } from 'bun:test';
import { mkdir, readdir, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Logger, Option, TestContext } from 'effect';
import {
  annotate,
  exportWideEvents,
  Observability,
  ObservabilityLive,
  ObservabilityNoop,
  ObservabilityTest,
  TelemetryCapture,
  telemetryConfigLayer,
  withWideEvent,
} from 'src/index';

/**
 * One root per test process, removed wholesale afterwards — so this file keeps
 * no mutable module state to track what it created.
 */
const ROOT = join(tmpdir(), `arena-telemetry-${String(process.pid)}`);

const runDir = async (name: string): Promise<string> => {
  const dir = join(ROOT, name);
  await mkdir(dir, { recursive: true });
  return dir;
};

afterAll(() => rm(ROOT, { recursive: true, force: true }));

/** An arbitrary object as a plain record, without asserting anything about it. */
const asRecord = (value: unknown): Record<string, unknown> => {
  if (typeof value !== 'object' || value === null) return {};
  return Object.fromEntries(
    Object.keys(value).map((key): readonly [string, unknown] => [key, Reflect.get(value, key)]),
  );
};

/** Every JSON object on every line of the corpus in `dir`, oldest first. */
const readCorpus = async (dir: string): Promise<ReadonlyArray<Record<string, unknown>>> => {
  const names = (await readdir(dir)).filter((name) => name.endsWith('.jsonl')).toSorted();
  const files = await Promise.all(names.map((name) => readFile(join(dir, name), 'utf8')));
  return files
    .flatMap((text) => text.split('\n'))
    .filter((line) => line.trim() !== '')
    .map((line): Record<string, unknown> => asRecord(JSON.parse(line)));
};

/**
 * `timestamp` is stamped by `evlog` from the wall clock; `eventId` is drawn
 * from the `Random` service. Both differ between two runs of the same program
 * by design, and nothing else does.
 */
const VOLATILE: ReadonlyArray<string> = ['timestamp', 'eventId'];

const stable = (event: Record<string, unknown> | undefined): Record<string, unknown> =>
  Object.fromEntries(
    Object.entries(event ?? {}).filter(([key]) => !VOLATILE.includes(key)),
  );

/** The unit of work both layers record. Identical on both sides, on purpose. */
const unitOfWork = annotate({ turn: 12, nation: 'Carthage', moves: ['a', 'b'] }).pipe(
  Effect.zipRight(Effect.succeed('done')),
  withWideEvent('turn.play'),
);

describe('ObservabilityLive', () => {
  test('writes one NDJSON line per event into the run directory', async () => {
    const dir = await runDir('ndjson');
    const config = telemetryConfigLayer({
      service: 'arena-telemetry-suite',
      environment: 'test',
      version: '0.1.0',
      ndjsonDir: dir,
    });

    await Effect.runPromise(
      Effect.all([unitOfWork, unitOfWork, unitOfWork], { concurrency: 3 }).pipe(
        Effect.provide(Layer.provide(ObservabilityLive, config)),
      ),
    );

    const corpus = await readCorpus(dir);
    expect(corpus).toHaveLength(3);
    // NDJSON means one event per line — `pretty: true` would break that, which
    // is why the adapter hard-codes `pretty: false`.
    expect(corpus.map((event) => event['event'])).toEqual(['turn.play', 'turn.play', 'turn.play']);
    // Whole lines, not fragments: `writeBatchToFs` appends one `appendFile` per
    // call, and two of those interleaving would leave a line no reader can parse.
    expect(corpus.every((event) => typeof event['eventId'] === 'string')).toBe(true);
    expect(corpus[0]).toMatchObject({
      service: 'arena-telemetry-suite',
      environment: 'test',
      version: '0.1.0',
      level: 'info',
      outcome: 'success',
      turn: 12,
      nation: 'Carthage',
      moves: ['a', 'b'],
    });
    // No drain, no `waitUntil`, no exit race: the write is awaited by the
    // effect that produced it, so the file is complete the moment it returns.
    expect(typeof corpus[0]?.['timestamp']).toBe('string');
  });

  test('writes the lines in the order the events sealed', async () => {
    const dir = await runDir('ordering');
    const config = telemetryConfigLayer({
      service: 'arena-telemetry-suite',
      environment: 'test',
      ndjsonDir: dir,
    });

    // Twenty units of work released in one burst, so they seal in a known order
    // — `Deferred.await` wakes its waiters FIFO — and then race each other into
    // `writeBatchToFs`, whose `mkdir` + `.gitignore` + `stat` prelude is three
    // opportunities to overtake before the `appendFile` that matters. The
    // one-permit semaphore in `ObservabilityLive` is what closes them.
    const program = Effect.gen(function* () {
      const gate = yield* Deferred.make<void>();
      const fibers = yield* Effect.forEach(
        Array.from({ length: 20 }, (_, index) => index),
        (index) =>
          Effect.fork(
            withWideEvent(Deferred.await(gate).pipe(Effect.zipRight(annotate({ seq: index }))), 'ordered'),
          ),
      );
      yield* Deferred.succeed(gate, undefined);
      yield* Effect.forEach(fibers, Fiber.join);
    });

    await Effect.runPromise(program.pipe(Effect.provide(Layer.provide(ObservabilityLive, config))));

    const corpus = await readCorpus(dir);
    expect(corpus.map((event) => event['seq'])).toEqual(
      Array.from({ length: 20 }, (_, index) => index),
    );
  });

  test('the live event and the captured event are the same event', async () => {
    const dir = await runDir('parity');
    const config = telemetryConfigLayer({
      service: 'arena-telemetry-suite',
      environment: 'test',
      version: '0.1.0',
      ndjsonDir: dir,
    });

    // Both runs are on `TestClock`, so `durationMs` is exactly 0 on both sides
    // and stays inside the comparison instead of being excused from it.
    await Effect.runPromise(
      unitOfWork.pipe(
        Effect.provide(
          Layer.merge(Layer.provide(ObservabilityLive, config), TestContext.TestContext),
        ),
      ),
    );
    const captured = await Effect.runPromise(
      unitOfWork.pipe(
        Effect.zipRight(Effect.flatMap(TelemetryCapture, (capture) => capture.takeEvents)),
        Effect.provide(
          Layer.merge(Layer.provide(ObservabilityTest, config), TestContext.TestContext),
        ),
      ),
    );

    const written = await readCorpus(dir);
    expect(written).toHaveLength(1);
    expect(captured).toHaveLength(1);

    const live = stable(written[0]);
    const captured0 = stable(captured[0]);

    // Same keys — a field the live path adds and the test path does not would
    // make every capture-based assertion in this suite a fiction.
    expect(Object.keys(captured0).toSorted()).toEqual(Object.keys(live).toSorted());
    expect(captured0).toEqual(live);
  });
});

/**
 * The mirror, against an endpoint nothing is listening on.
 *
 * No collector is started and none is needed: the assertions here are about
 * what happens when the collector is *not* there, which is the case a corpus
 * recorder has to survive. `127.0.0.1:9` is the discard port — the connection
 * is refused immediately, so this costs a few milliseconds rather than the
 * retry-and-timeout budget an unroutable address would.
 */
const DEAD_COLLECTOR = 'http://127.0.0.1:9';

describe('the OTLP mirror', () => {
  test('a refused collector is a TelemetryExportError naming the endpoint', async () => {
    const exit = await Effect.runPromiseExit(
      exportWideEvents(
        [{ timestamp: '2026-01-01T00:00:00.000Z', level: 'info', service: 's', environment: 'e' }],
        DEAD_COLLECTOR,
        's',
      ),
    );

    expect(Exit.isFailure(exit)).toBe(true);
    const failure = Exit.isFailure(exit) ? Option.getOrUndefined(Cause.failureOption(exit.cause)) : undefined;
    expect(failure?._tag).toBe('TelemetryExportError');
    expect(failure?.endpoint).toBe(DEAD_COLLECTOR);
    expect(failure?.events).toBe(1);
  });

  test('the corpus is written first, so a failed mirror does not un-write it', async () => {
    const dir = await runDir('mirror');
    const lines: Array<string> = [];

    const result = await Effect.runPromise(
      withWideEvent(annotate({ turn: 3 }).pipe(Effect.as('ok')), 'turn.play').pipe(
        Effect.provide(
          Layer.merge(
            Layer.provide(
              ObservabilityLive,
              telemetryConfigLayer({
                service: 'arena-telemetry-suite',
                environment: 'test',
                ndjsonDir: dir,
                otlpEndpoint: DEAD_COLLECTOR,
              }),
            ),
            Logger.replace(
              Logger.defaultLogger,
              Logger.make<unknown, void>(({ message, annotations }) => {
                lines.push(`${String(message)} ${JSON.stringify(Object.fromEntries(annotations))}`);
              }),
            ),
          ),
        ),
      ),
    );

    // The unit of work is untouched, the line is on disk, and the *only*
    // casualty is the mirror — reported, with the endpoint that refused.
    expect(result).toBe('ok');
    expect(await readCorpus(dir)).toMatchObject([{ event: 'turn.play', turn: 3 }]);
    expect(lines).toHaveLength(1);
    expect(lines[0]).toContain('OTLP mirror failed for a wide event');
    expect(lines[0]).not.toContain('dropped a wide event');
    expect(lines[0]).toContain('TelemetryExportError');
    expect(lines[0]).toContain(DEAD_COLLECTOR);
  });

  test('with no endpoint configured there is nothing to fail', async () => {
    // The normal case: `None` means this package performs no network I/O at
    // all, which is why the rest of the suite can run the real emission path.
    const dir = await runDir('no-mirror');
    const lines: Array<string> = [];

    await Effect.runPromise(
      withWideEvent(Effect.void, 'turn.play').pipe(
        Effect.provide(
          Layer.merge(
            Layer.provide(
              ObservabilityLive,
              telemetryConfigLayer({
                service: 'arena-telemetry-suite',
                environment: 'test',
                ndjsonDir: dir,
              }),
            ),
            Logger.replace(
              Logger.defaultLogger,
              Logger.make<unknown, void>(({ message }) => {
                lines.push(String(message));
              }),
            ),
          ),
        ),
      ),
    );

    expect(await readCorpus(dir)).toHaveLength(1);
    expect(lines).toEqual([]);
  });
});

describe('ObservabilityNoop', () => {
  test('records nothing and reports it as None', async () => {
    const recorded = await Effect.runPromise(
      Effect.flatMap(Observability, (observability) =>
        observability.record({
          id: 'e1',
          name: 'turn.play',
          startedAt: 0,
          durationMs: 1,
          fields: { turn: 1 },
          error: Option.none(),
        }),
      ).pipe(Effect.provide(ObservabilityNoop)),
    );

    // `None` is "sealed and deliberately not recorded" — a success, never a
    // signal to retry.
    expect(Option.isNone(recorded)).toBe(true);
  });

  test('needs no configuration and touches no global state', async () => {
    // The consumer that has not opted in still gets a working `withWideEvent`.
    const result = await Effect.runPromise(
      withWideEvent(annotate({ turn: 1 }).pipe(Effect.as(99)), 'turn.play').pipe(
        Effect.provide(ObservabilityNoop),
      ),
    );
    expect(result).toBe(99);
  });
});
