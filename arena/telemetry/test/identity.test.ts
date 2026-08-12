/**
 * Who wrote this event, and does the answer depend on anything but the layer
 * that wrote it?
 *
 * `evlog`'s identity state — `service`, `environment`, `version`, `commitHash`,
 * `region` — is process-global, seeded from `process.env`, and overwritten in
 * full by every `initLogger` call anywhere in the process. Two things follow,
 * and both of them are tested here rather than reasoned about:
 *
 * - **A second layer must not relabel the first layer's events.** A run
 *   directory whose rows name the wrong service is worse than an empty one:
 *   nothing else on the row identifies the producer, so the file cannot be
 *   repaired after the fact.
 * - **A `GITHUB_SHA` must not change the shape of the corpus.** The package
 *   exists to make two recordings comparable byte for byte; a field set that
 *   grows a `commitHash` in CI and loses it on a laptop makes that impossible,
 *   and `observability.test.ts`'s live-vs-captured parity test cannot see it,
 *   because both of its sides run in one process with one environment.
 *
 * The third case here is the same theme from the other end: what this package
 * does when somebody *else*'s `initLogger` turns emission off underneath it.
 */

// `bun:test` bodies are promises, and every test is its own entry point: both
// diagnostics below describe correct-but-unidiomatic shapes that a test file
// cannot avoid. `strict-effect-provide` says so itself ("If this is an entry
// point, you can safely disable this diagnostic").
// oxlint-disable effecttsgo/async-function
// oxlint-disable effecttsgo/strict-effect-provide
// Reading and writing `process.env` is the subject of this file, not an
// accident in it: the point is that these variables reach nothing.
// oxlint-disable effecttsgo/process-env
// oxlint-disable effecttsgo/node-builtin-import
import { afterAll, describe, expect, test } from 'bun:test';
import { mkdir, readdir, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Effect, Layer, Option } from 'effect';
import { initLogger } from 'evlog';
import {
  annotate,
  initializeEvlog,
  makeTelemetryConfig,
  ObservabilityLive,
  ObservabilityTest,
  TelemetryCapture,
  type TelemetryConfigInput,
  telemetryConfigLayer,
  withWideEvent,
} from 'src/index';
import { TEST_CONFIG } from 'test/support';

const ROOT = join(tmpdir(), `arena-telemetry-identity-${String(process.pid)}`);

const runDir = async (name: string): Promise<string> => {
  const dir = join(ROOT, name);
  await mkdir(dir, { recursive: true });
  return dir;
};

/** Every JSON object written into `dir`, oldest first. */
const readCorpus = async (dir: string): Promise<ReadonlyArray<Record<string, unknown>>> => {
  const names = (await readdir(dir)).filter((name) => name.endsWith('.jsonl')).toSorted();
  const files = await Promise.all(names.map((name) => readFile(join(dir, name), 'utf8')));
  return files
    .flatMap((text) => text.split('\n'))
    .filter((line) => line.trim() !== '')
    .map((line): Record<string, unknown> => {
      const parsed: unknown = JSON.parse(line);
      return typeof parsed === 'object' && parsed !== null
        ? Object.fromEntries(
            Object.keys(parsed).map((key): readonly [string, unknown] => [
              key,
              Reflect.get(parsed, key),
            ]),
          )
        : {};
    });
};

/** The environment variables `evlog`'s `detectEnvironment()` reads. */
const PROBED: ReadonlyArray<string> = ['APP_VERSION', 'GITHUB_SHA', 'AWS_REGION'];

afterAll(async () => {
  PROBED.forEach((key) => {
    Reflect.deleteProperty(process.env, key);
  });
  // Every layer build re-initializes evlog, but this file also calls
  // `initLogger` directly, so it puts the global back the way it found it.
  await Effect.runPromise(initializeEvlog(makeTelemetryConfig(TEST_CONFIG)));
  await rm(ROOT, { recursive: true, force: true });
});

describe('two layers in one process', () => {
  test('each labels its own events, whichever initialized last', async () => {
    const harnessDir = await runDir('harness');
    const cliDir = await runDir('cli');
    const harness = telemetryConfigLayer({
      service: 'arena-harness',
      environment: 'prod-a',
      version: '1.0.0',
      ndjsonDir: harnessDir,
    });
    const cli = telemetryConfigLayer({
      service: 'arena-play-cli',
      environment: 'prod-b',
      version: '2.0.0',
      ndjsonDir: cliDir,
    });

    // Both layers built, both alive, and the second one's `initLogger` has
    // already overwritten the first one's `state.env` by the time either
    // records anything.
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const first = yield* Layer.build(Layer.provide(ObservabilityLive, harness));
          const second = yield* Layer.build(Layer.provide(ObservabilityLive, cli));
          yield* withWideEvent(Effect.void, 'harness.work').pipe(Effect.provide(first));
          yield* withWideEvent(Effect.void, 'cli.work').pipe(Effect.provide(second));
        }),
      ),
    );

    expect(await readCorpus(harnessDir)).toMatchObject([
      { event: 'harness.work', service: 'arena-harness', environment: 'prod-a', version: '1.0.0' },
    ]);
    expect(await readCorpus(cliDir)).toMatchObject([
      { event: 'cli.work', service: 'arena-play-cli', environment: 'prod-b', version: '2.0.0' },
    ]);
  });
});

describe('the environment cannot reach the event', () => {
  /** The keys every event carries, whatever the machine it was recorded on. */
  const BASE_KEYS: ReadonlyArray<string> = [
    'duration',
    'durationMs',
    'environment',
    'event',
    'eventId',
    'level',
    'outcome',
    'service',
    'timestamp',
  ];

  const record = async (config: TelemetryConfigInput): Promise<Record<string, unknown>> => {
    await Effect.runPromise(
      annotate({ turn: 1 }).pipe(
        withWideEvent('probe.work'),
        Effect.provide(Layer.provide(ObservabilityLive, telemetryConfigLayer(config))),
      ),
    );
    const corpus = await readCorpus(config.ndjsonDir);
    expect(corpus).toHaveLength(1);
    return corpus[0] ?? {};
  };

  test('APP_VERSION, GITHUB_SHA and AWS_REGION do not appear on an event', async () => {
    // Exactly the CI case: GitHub Actions always sets GITHUB_SHA.
    process.env['APP_VERSION'] = '9.9.9-from-env';
    process.env['GITHUB_SHA'] = 'deadbeefcafe';
    process.env['AWS_REGION'] = 'us-west-2';

    const event = await record({
      service: 'probe-svc',
      environment: 'probe-env',
      ndjsonDir: await runDir('probe'),
    });

    // `version: None` means no `version` field — not "whatever APP_VERSION says".
    expect(Object.keys(event).toSorted()).toEqual([...BASE_KEYS, 'turn'].toSorted());
    expect(event['service']).toBe('probe-svc');
    expect(event['environment']).toBe('probe-env');
  });

  test('and the same three fields do appear when the configuration carries them', async () => {
    // The other half: suppressing the probe must not mean the fields are
    // unavailable, only that they are ours to set.
    const event = await record({
      service: 'probe-svc',
      environment: 'probe-env',
      version: '1.2.3',
      commitHash: 'abc123',
      region: 'eu-central-1',
      ndjsonDir: await runDir('explicit'),
    });

    expect(Object.keys(event).toSorted()).toEqual(
      [...BASE_KEYS, 'turn', 'version', 'commitHash', 'region'].toSorted(),
    );
    expect(event['version']).toBe('1.2.3');
    expect(event['commitHash']).toBe('abc123');
    expect(event['region']).toBe('eu-central-1');
  });
});

describe('an event evlog declines to emit', () => {
  /**
   * Turn head sampling all the way down *after* the layer has been built —
   * which is the only way it can happen, since the layer's own `initLogger`
   * would reset `sampling` on the way in. This is the documented hazard from
   * `initializeEvlog`'s docstring, driven rather than described.
   */
  const dropEverything = Effect.sync(() =>
    initLogger({
      enabled: true,
      silent: true,
      pretty: false,
      stringify: true,
      redact: false,
      _suppressDrainWarning: true,
      sampling: { rates: { info: 0 } },
      env: { service: 'arena-telemetry-test', environment: 'test' },
    }),
  );

  test('is a success with nothing captured, never a retry', async () => {
    const config = telemetryConfigLayer(TEST_CONFIG);

    const captured = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(Layer.provide(ObservabilityTest, config));
          yield* dropEverything;
          return yield* withWideEvent(Effect.succeed('done'), 'unit.work').pipe(
            Effect.zipRight(Effect.flatMap(TelemetryCapture, (capture) => capture.takeEvents)),
            Effect.provide(context),
          );
        }),
      ),
    );

    // `None` is "sealed, and deliberately not recorded". The unit of work
    // returned normally and nothing was dropped: there is no warning to file.
    expect(captured).toHaveLength(0);
  });

  test('writes no line to the corpus and no empty file either', async () => {
    const dir = await runDir('sampled-out');
    const config = telemetryConfigLayer({ ...TEST_CONFIG, ndjsonDir: dir });

    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(Layer.provide(ObservabilityLive, config));
          yield* dropEverything;
          return yield* withWideEvent(Effect.succeed('done'), 'unit.work').pipe(
            Effect.provide(context),
          );
        }),
      ),
    );

    // The write is skipped entirely, so a sampled-out run does not even create
    // the run directory's `.jsonl`.
    expect(result).toBe('done');
    expect(await readCorpus(dir)).toHaveLength(0);
    expect(Option.fromNullable((await readdir(dir)).find((n) => n.endsWith('.jsonl')))).toEqual(
      Option.none(),
    );
  });
});
