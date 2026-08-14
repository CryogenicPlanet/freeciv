/**
 * Private-CA trust (NOTES §I.3.1): `caTrustedFetch` resolves the CA per
 * request from PLAY_TLS_CA or the workspace `.playconfig.json`, and a
 * configured-but-unreadable CA refuses with the path in the message instead of
 * degrading to the untrusted default.
 */
import { afterEach, describe, expect } from 'bun:test';
import { Effect, Either, Option, Schema } from 'effect';
import { caTrustedFetch } from 'src/services/http';
import { scratchWorkspace, type Scratch } from 'test/_fixtures';
import { awaitTest } from 'test/_effect-test';
import { path, withTestFileSystem } from 'test/_test-platform';

interface Captured {
  tlsCa: string | undefined;
}

const tlsInitSchema = Schema.Struct({ tls: Schema.Struct({ ca: Schema.String }) });

const capturing = () => {
  const captured: Captured = { tlsCa: undefined };
  const fetchImpl = Object.assign(
    (_input: string | URL | Request, init?: RequestInit): Promise<Response> => {
      const decoded = Schema.decodeUnknownOption(tlsInitSchema)(init);
      captured.tlsCa = Option.isSome(decoded) ? decoded.value.tls.ca : undefined;
      return Promise.resolve(new Response('{}'));
    },
    { preconnect: fetch.preconnect }
  );
  return { fetchImpl, captured };
};

const scratches: Scratch[] = [];
const savedEnv = {
  PLAY_TLS_CA: Bun.env['PLAY_TLS_CA'],
  PLAY_ROOT: Bun.env['PLAY_ROOT'],
};

afterEach(() => {
  for (const [name, value] of Object.entries(savedEnv)) {
    if (value === undefined) {
      delete Bun.env[name];
    } else {
      Bun.env[name] = value;
    }
  }
  return Effect.runPromise(
    Effect.forEach(scratches.splice(0), (scratch) => scratch.cleanup, { discard: true })
  );
});

const scratchDir = (): Effect.Effect<string> =>
  Effect.map(scratchWorkspace(), (scratch) => {
    scratches.push(scratch);
    return scratch.workspace.root;
  });

const writeFile = (target: string, contents: string): Effect.Effect<void> =>
  Effect.orDie(withTestFileSystem((files) => files.writeFileString(target, contents)));

describe('caTrustedFetch', () => {
  awaitTest('no configuration leaves the request untouched', function* (wait) {
    delete Bun.env['PLAY_TLS_CA'];
    Bun.env['PLAY_ROOT'] = yield* scratchDir(); // no .playconfig.json inside
    const { fetchImpl, captured } = capturing();
    yield* wait(caTrustedFetch(fetchImpl)('https://supervisor.test/health'));
    expect(captured.tlsCa).toBeUndefined();
  });

  awaitTest('PLAY_TLS_CA injects the file contents as tls.ca', function* (wait) {
    const dir = yield* scratchDir();
    const caPath = path.join(dir, 'ca.pem');
    yield* writeFile(caPath, 'FAKE CA PEM\n');
    Bun.env['PLAY_TLS_CA'] = caPath;
    const { fetchImpl, captured } = capturing();
    yield* wait(caTrustedFetch(fetchImpl)('https://supervisor.test/health'));
    expect(captured.tlsCa).toBe('FAKE CA PEM\n');
  });

  awaitTest('the workspace .playconfig.json tls_ca is the fallback, relative to the root', function* (wait) {
    const dir = yield* scratchDir();
    yield* writeFile(path.join(dir, 'stack-ca.pem'), 'WORKSPACE CA\n');
    yield* writeFile(
      path.join(dir, '.playconfig.json'),
      JSON.stringify({ schema_version: 1, game_id: 'game_x', name: 'n', tls_ca: 'stack-ca.pem' })
    );
    delete Bun.env['PLAY_TLS_CA'];
    Bun.env['PLAY_ROOT'] = dir;
    const { fetchImpl, captured } = capturing();
    yield* wait(caTrustedFetch(fetchImpl)('https://supervisor.test/health'));
    expect(captured.tlsCa).toBe('WORKSPACE CA\n');
  });

  awaitTest('an empty PLAY_TLS_CA opts out of the workspace fallback', function* (wait) {
    const dir = yield* scratchDir();
    yield* writeFile(path.join(dir, 'stack-ca.pem'), 'WORKSPACE CA\n');
    yield* writeFile(
      path.join(dir, '.playconfig.json'),
      JSON.stringify({ schema_version: 1, game_id: 'game_x', name: 'n', tls_ca: 'stack-ca.pem' })
    );
    Bun.env['PLAY_TLS_CA'] = '';
    Bun.env['PLAY_ROOT'] = dir;
    const { fetchImpl, captured } = capturing();
    yield* wait(caTrustedFetch(fetchImpl)('https://supervisor.test/health'));
    expect(captured.tlsCa).toBeUndefined();
  });

  awaitTest('a configured but unreadable CA refuses with the path in the message', function* () {
    const missing = path.join(yield* scratchDir(), 'gone.pem');
    Bun.env['PLAY_TLS_CA'] = missing;
    const { fetchImpl } = capturing();
    const outcome = yield* Effect.either(
      Effect.tryPromise({
        try: () => caTrustedFetch(fetchImpl)('https://supervisor.test/health'),
        catch: (cause) => cause instanceof Error ? cause.message : String(cause),
      })
    );
    expect(Either.isLeft(outcome)).toBe(true);
    if (Either.isLeft(outcome)) {
      expect(outcome.left).toContain(missing);
    }
  });
});
