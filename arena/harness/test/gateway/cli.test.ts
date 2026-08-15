/** CLI/config behavior and Python-compatibility coverage. */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { CliConfig, CommandDescriptor, HelpDoc } from '@effect/cli';
import { type FileSystem } from '@effect/platform';
import { NodeFileSystem, NodePath, NodeTerminal } from '@effect/platform-node';
import { decodeGameId, Gateway } from '@arena/wire';
import { Effect, Either, Layer, Option, Predicate, Redacted, Schema } from 'effect';
import { mkdir, mkdtemp, realpath, rm, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  GATEWAY_CONFIG_MESSAGES,
  GatewayConfig,
  GatewayConfigError,
  expandUser,
  gatewayConfigLayer,
  gatewayIdentity,
  loopbackHost,
  makeGatewayConfig,
  normalizeServiceUrl,
  pythonFloat,
  pythonInt,
  pythonRepr,
  pythonStrip,
  resolvePath,
} from '../../src/gateway/config.ts';
import type { GatewayConfigInput } from '../../src/gateway/config.ts';
import {
  DATABASE_URL_FLAG,
  DATABASE_URL_WITHOUT_POSTGRES,
  DEFAULT_GATEWAY_BACKEND,
  DEFAULT_GATEWAY_HOST,
  DEFAULT_GATEWAY_PORT,
  DEFAULT_REPO_ROOT,
  DEFAULT_UPSTREAM_TIMEOUT_SECONDS,
  GATEWAY_BACKENDS,
  GATEWAY_BACKEND_FLAG,
  GATEWAY_CLI_ERROR_EXIT_CODE,
  GATEWAY_CLI_NAME,
  PYTHON_GATEWAY_PROG,
  formatStartupError,
  gatewayCommand,
  missingOptionMessage,
} from '../../src/gateway/cli.ts';
import type { GatewayCliArgs } from '../../src/gateway/cli.ts';
import type { PostgresBackendInput } from '../../src/gateway/config.ts';
import { terminalArchiveView } from '../../src/gateway/archive.ts';
import {
  GATEWAY_DB_APPLICATION_NAME,
  GATEWAY_MAX_DB_CONNECTIONS,
  POSTGRES_BACKEND_UNAVAILABLE,
  UNUSABLE_DATABASE_URL,
  archiveServices,
  describeRepositoryFailure,
  describeStartupError,
  withFailureLog,
} from '../../src/gateway/main.ts';
import { ReplayDerivation } from '../../src/gateway/services/derivation.ts';
import { RunsRepository } from '../../src/gateway/services/runs.ts';
import type { RunsRepositoryApi, TerminalArchive } from '../../src/gateway/services/runs.ts';

// ---------------------------------------------------------------------------
// The Python oracle
// ---------------------------------------------------------------------------

const REPO_ROOT = decodeURIComponent(new URL('../../../../', import.meta.url).pathname).replace(
  /\/$/,
  '',
);
const PYTHON_ROOT = join(REPO_ROOT, 'arena', 'archive');

const ORACLE_SOURCE = `
import json, os, struct, sys
sys.path.insert(0, sys.argv[1])
from pathlib import Path
from agent_eval.replay_gateway import (
    _normalize_service_url, _loopback_host, _identity, gateway_config,
)

def attempt(fn, arg):
    try:
        return {"ok": True, "value": fn(arg)}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}

def build(spec):
    value = gateway_config(
        spec["service_url"], spec["runs_root"], spec["cache_root"],
        repo_root=spec["repo_root"],
        upstream_timeout_s=spec["upstream_timeout_s"],
        viewer_public_url=spec.get("viewer_public_url"),
    )
    return {
        "repo_root": str(value.repo_root),
        "upstream_service_url": value.upstream_service_url,
        "runs_root": str(value.runs_root),
        "cache_root": str(value.cache_root),
        "identity": value.identity,
        "upstream_timeout_s": struct.pack("<d", value.upstream_timeout_s).hex(),
        "viewer_public_url": value.viewer_public_url,
    }

job = json.load(sys.stdin)
out = {
    "normalize": [attempt(_normalize_service_url, v) for v in job.get("normalize", [])],
    "loopback": [attempt(_loopback_host, v) for v in job.get("loopback", [])],
    "int": [attempt(lambda v: str(int(v)), v) for v in job.get("int", [])],
    "float": [attempt(lambda v: struct.pack("<d", float(v)).hex(), v) for v in job.get("float", [])],
    "repr": [repr(v) for v in job.get("repr", [])],
    "identity": [
        _identity(Path(m[0]), m[1], Path(m[2]), Path(m[3]), m[4])
        for m in job.get("identity", [])
    ],
    "config": [attempt(build, spec) for spec in job.get("config", [])],
}
if "resolve" in job:
    os.chdir(job["resolve"]["cwd"])
    out["resolve"] = [
        attempt(lambda v: str(Path(v).expanduser().resolve()), v)
        for v in job["resolve"]["paths"]
    ]
json.dump(out, sys.stdout)
`;

/** One `attempt()` result: what Python returned, or the `str(exc)` it raised. */
interface OracleOutcome {
  readonly ok: boolean;
  readonly value?: unknown;
  readonly error?: string;
}

interface OracleConfigSpec {
  readonly service_url: string;
  readonly runs_root: string;
  readonly cache_root: string;
  readonly repo_root: string;
  /** A string here is deliberate: `gateway_config` rejects a non-number. */
  readonly upstream_timeout_s: number | string;
  readonly viewer_public_url: string | null;
}

interface OracleJob {
  readonly normalize: readonly string[];
  readonly loopback: readonly string[];
  readonly int: readonly string[];
  readonly float: readonly string[];
  readonly repr: readonly string[];
  readonly identity: readonly IdentityMaterial[];
  readonly config: readonly OracleConfigSpec[];
  readonly resolve: { readonly cwd: string; readonly paths: readonly string[] };
}

interface OracleAnswer {
  readonly normalize: readonly OracleOutcome[];
  readonly loopback: readonly OracleOutcome[];
  readonly int: readonly OracleOutcome[];
  readonly float: readonly OracleOutcome[];
  readonly repr: readonly string[];
  readonly identity: readonly string[];
  readonly config: readonly OracleOutcome[];
  readonly resolve: readonly OracleOutcome[];
}

const askOracle = async (job: OracleJob): Promise<OracleAnswer> => {
  const child = Bun.spawn(['python3', '-c', ORACLE_SOURCE, PYTHON_ROOT], {
    cwd: REPO_ROOT,
    stdin: new TextEncoder().encode(JSON.stringify(job)),
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const [stdout, stderr, code] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  expect({ code, stderr }).toEqual({ code: 0, stderr: '' });
  // SAFETY: ORACLE_SOURCE is fixed above and always emits the OracleAnswer contract.
  // oxlint-disable-next-line no-unsafe-type-assertion
  return JSON.parse(stdout) as OracleAnswer;
};

// ---------------------------------------------------------------------------
// Comparing an `Either` to an oracle outcome
// ---------------------------------------------------------------------------

/** Preserve a value while retaining its inferred domain type. */
const identity = <Value>(value: Value): Value => value;

/** Render an `Either` the way the oracle renders a Python call. */
const asOutcome = <A, Rendered>(
  result: Either.Either<A, GatewayConfigError>,
  render: (value: A) => Rendered,
): OracleOutcome =>
  Either.match(result, {
    onLeft: (error) => ({ ok: false, error: error.message }),
    onRight: (value) => ({ ok: true, value: render(value) }),
  });

/** A comparable outcome: exactly one of `value` / `error`, never both. */
type Outcome =
  | { readonly ok: true; readonly value: unknown }
  | { readonly ok: false; readonly error: string };

/** Drop the absent key so `{ok:true}` and `{ok:true,error:undefined}` compare. */
const tidy = (outcome: OracleOutcome): Outcome =>
  outcome.ok
    ? { ok: true, value: outcome.value }
    : { ok: false, error: outcome.error ?? '<no message>' };

/** Keyed by input, so a failure names the value that diverged. */
const keyed = <A>(inputs: readonly string[], values: readonly A[]): Record<string, A | undefined> =>
  Object.fromEntries(inputs.map((input, index) => [JSON.stringify(input), values[index]]));

/** IEEE-754 bits, so `inf`/`nan`/`-0.0` compare exactly with no formatting. */
const floatBits = (value: number): string => {
  const view = new DataView(new ArrayBuffer(8));
  view.setFloat64(0, value, true);
  return [...new Uint8Array(view.buffer)].map((byte) => byte.toString(16).padStart(2, '0')).join('');
};

const FILE_SYSTEM = NodeFileSystem.layer;

const runFs = <A>(effect: Effect.Effect<A, never, FileSystem.FileSystem>): Promise<A> =>
  Effect.runPromise(Effect.provide(effect, FILE_SYSTEM));

// ---------------------------------------------------------------------------
// Corpora
// ---------------------------------------------------------------------------

const SERVICE_URLS: readonly string[] = [
  'http://127.0.0.1:8080',
  'HTTP://LocalHost:80/',
  'https://h:443/a/',
  'http://[::1]:8000/x/y//',
  'http://h',
  'http://h/',
  'http://h//',
  'http://user@h',
  'http://:pw@h',
  'http://@h',
  'http://h?q',
  'http://h?',
  'http://h#f',
  'http://h#',
  'http://h/a/./b',
  'http://h/a/../b',
  'http://h/.',
  'ftp://h',
  'file:///x',
  'http:/h',
  '//h/x',
  'h',
  '',
  '  http://h/p  ',
  '\u3000http://h/p\u00a0',
  'http://h:99999',
  'http://h:abc',
  'http://h:-1',
  'http://h/a b',
  'http://h/\u00e9',
  'http://h.:80',
  'http://H%2F/x',
  'http://[::ffff:1.2.3.4]/',
  'http://h:0',
  'http://h:080',
  'http://[foo]/',
  'http://[1.2.3.4]/',
  'http://[::1',
  'http://h]/',
  'http://\u2100/x',
  'http://h:',
  'http://h:/p',
  'htTP://h',
  'http://h/\u0001',
  'http://h/\tx',
  'http://h\n/x',
  'http://h/%2e/',
  'http://h/..x',
  'http://h/x..',
  'http://[::1%25lo0]/',
  'http://[fe80::1%lo0]/',
  '\u001chttp://h',
  '\u0000http://h',
  'http://h/p\u001c',
  'https://h:80/x',
  'http://h:443/x',
  'HTTPS://EXAMPLE.COM:443/Freeciv/',
  'http://127.0.0.1:5000/freeciv',
  'http://[::1]',
  'http://[::1]/',
];

const LOOPBACK_HOSTS: readonly string[] = [
  '127.0.0.1',
  '::1',
  '127.0.0.53',
  '127.255.255.254',
  '0.0.0.0',
  'localhost',
  '127.1',
  '127.000.000.1',
  '::0001',
  '0:0:0:0:0:0:0:1',
  '::ffff:127.0.0.1',
  '::ffff:7f00:1',
  '::1%lo0',
  '[::1]',
  '127.0.0.1 ',
  '',
  '::',
  '1:0:0:2:0:0:0:3',
  '2001:db8::0:1',
  '::ffff:0:1.2.3.4',
  '64:ff9b::1.2.3.4',
  '127.0.0.256',
  '127.0.0.01',
  'fe80::1%lo0',
  '::ffff:0.0.0.0',
  '::ffff:127.1.2.3',
  '1::2::3',
  '1:2:3:4:5:6:7:8:9',
  ':1',
  '1:',
  '::1%',
  '1.2.3.4',
  '255.255.255.255',
  '10.0.0.1',
  '0:0:0:0:0:0:0:0',
  '1:2:3:4:5:6:7:8',
];

const INTS: readonly string[] = [
  '5',
  '5_0',
  '+3',
  ' 5 ',
  '0',
  '0x10',
  '\u0663',
  '\uff11\uff12',
  '-4',
  '',
  ' ',
  '1__0',
  '_5',
  '5_',
  '\u00a05',
  '\u00855',
  '\u001c5',
  '\uff10',
  '00',
  '007',
  '99999999999999999999',
  '1 2',
  '+',
  '-',
  '\t8\n',
  '65535',
  '65536',
  '-1',
  '1.0',
  '1e3',
  '\u0f20\u0f21',
  '\u1d7ce\u1d7cf',
];

const FLOATS: readonly string[] = [
  '10',
  '1e3',
  '.5',
  '5.',
  'inf',
  '-inf',
  'nan',
  'Infinity',
  '1_0.5',
  '  2.5\n',
  '0',
  '-1',
  '1e400',
  '0.0',
  '-0.0',
  '\u0663.\u0665',
  '5_0',
  '1e',
  'e5',
  '.',
  '1.2.3',
  '+.5',
  '1_.5',
  '1._5',
  'INF',
  'NaN',
  '  -infinity ',
  '1e+3',
  '1e_3',
  '0.5e-2',
  '',
  '1_000_000.5',
  '0.1',
  '  \u00a03.5\u0085',
  '.e3',
  '1.5e',
];

const REPRS: readonly string[] = [
  '0x10',
  "it's",
  'say "hi"',
  'both\'"',
  'tab\there',
  'new\nline',
  'back\\slash',
  '\u0000',
  'plain',
];

/** `[repo_root, upstream, runs_root, cache_root, viewer_public_url | null]`. */
type IdentityMaterial = readonly [string, string, string, string, string | null];

const IDENTITY_MATERIAL: readonly IdentityMaterial[] = [
  ['/a', 'http://h', '/b', '/c', null],
  ['/a', 'http://h', '/b', '/c', 'http://v'],
  ['/a', '', '/b', '/c', ''],
  ['/\u00e9', 'https://h/p', '/r r', '/c\u0000c', 'http://v:1'],
];

const RESOLVE_PATHS: readonly string[] = [
  'real',
  'real/',
  'link/sub',
  'link/sub/../x',
  'rel/sub',
  'chain/sub',
  'loop',
  'loop/x',
  'missing/deep',
  '.',
  '',
  './/real//sub',
  'link/..',
  'dangling',
  'dangling/y',
  '/a//b/',
  '..',
  '../',
  'real/../real/sub',
];

/** A valid input with one or more fields swapped out. */
const baseInput = (
  overrides: Partial<GatewayConfigInput> & Pick<GatewayConfigInput, 'serviceUrl'>,
): GatewayConfigInput => ({
  host: '127.0.0.1',
  port: 0n,
  runsRoot: '/r',
  cacheRoot: '/c',
  repoRoot: '/p',
  readyFile: '/ready.json',
  upstreamTimeoutSeconds: 10,
  viewerPublicUrl: Option.none(),
  ...overrides,
});

// ---------------------------------------------------------------------------

interface OracleAnswerState {
  value?: OracleAnswer;
}

interface ConfigSpecsState {
  value: readonly GatewayConfigInput[];
}

describe('the Python differential', () => {
  const fixture = { created: '', real: '' };
  const answer: OracleAnswerState = {};
  const specs: ConfigSpecsState = { value: [] };

  beforeAll(async () => {
    fixture.created = await mkdtemp(join(tmpdir(), 'arena-gateway-config-'));
    fixture.real = await realpath(fixture.created);
    await mkdir(join(fixture.real, 'real', 'sub'), { recursive: true });
    await symlink(join(fixture.real, 'real'), join(fixture.real, 'link'));
    await symlink('real', join(fixture.real, 'rel'));
    await symlink('loop', join(fixture.real, 'loop'));
    await symlink('link', join(fixture.real, 'chain'));
    await symlink('/nowhere/x', join(fixture.real, 'dangling'));

    // Every config spec is absolute: `gateway_config` resolves against the
    // process working directory, and the oracle's differs from bun's.  The
    // `created` (pre-realpath) forms still exercise symlink resolution, since
    // macOS puts `mkdtemp` under `/var/folders` -> `/private/var/folders`.
    specs.value = [
      baseInput({
        serviceUrl: 'http://127.0.0.1:5000/freeciv/',
        runsRoot: join(fixture.created, 'runs'),
        cacheRoot: join(fixture.created, 'cache'),
        repoRoot: REPO_ROOT,
      }),
      baseInput({
        serviceUrl: 'HTTP://LocalHost:80',
        runsRoot: join(fixture.created, 'link', 'sub'),
        cacheRoot: join(fixture.real, 'chain', 'missing', 'cache'),
        repoRoot: join(fixture.real, 'rel'),
        upstreamTimeoutSeconds: 2.5,
        viewerPublicUrl: Option.some('https://viewer.example:443/x/'),
      }),
      baseInput({ serviceUrl: 'ftp://h' }),
      baseInput({ serviceUrl: 'http://h', upstreamTimeoutSeconds: 0 }),
      baseInput({ serviceUrl: 'http://h', upstreamTimeoutSeconds: Number.NaN }),
      baseInput({ serviceUrl: 'http://h', viewerPublicUrl: Option.some('http://v?q') }),
      baseInput({
        serviceUrl: 'http://h/x/',
        runsRoot: join(fixture.real, 'loop'),
        cacheRoot: join(fixture.real, 'dangling', 'c'),
        repoRoot: join(fixture.real, '..'),
      }),
    ];

    answer.value = await askOracle({
      normalize: SERVICE_URLS,
      loopback: LOOPBACK_HOSTS,
      int: INTS,
      float: FLOATS,
      repr: REPRS,
      identity: IDENTITY_MATERIAL,
      resolve: { cwd: fixture.real, paths: RESOLVE_PATHS },
      config: specs.value.map((spec) => ({
        service_url: spec.serviceUrl,
        runs_root: spec.runsRoot,
        cache_root: spec.cacheRoot,
        repo_root: spec.repoRoot,
        // JSON cannot carry NaN, and a string is rejected by the same check
        // that rejects a NaN (`not isinstance(..., (int, float))`, `:196`).
        upstream_timeout_s: Number.isNaN(spec.upstreamTimeoutSeconds)
          ? 'nan'
          : spec.upstreamTimeoutSeconds,
        viewer_public_url: Option.getOrNull(spec.viewerPublicUrl),
      })),
    });
  });

  afterAll(async () => {
    if (fixture.created !== '') await rm(fixture.created, { recursive: true, force: true });
  });

  const oracle = (): OracleAnswer => {
    const value = answer.value;
    if (value === undefined) throw new Error('the oracle did not run');
    return value;
  };

  test('_normalize_service_url agrees on every corpus URL', () => {
    expect(keyed(SERVICE_URLS, SERVICE_URLS.map((u) => tidy(asOutcome(normalizeServiceUrl(u), identity))))).toEqual(
      keyed(SERVICE_URLS, oracle().normalize.map(tidy)),
    );
  });

  test('_loopback_host agrees on every corpus host', () => {
    expect(keyed(LOOPBACK_HOSTS, LOOPBACK_HOSTS.map((h) => tidy(asOutcome(loopbackHost(h), identity))))).toEqual(
      keyed(LOOPBACK_HOSTS, oracle().loopback.map(tidy)),
    );
  });

  test('int() agrees, including underscores and Unicode digits', () => {
    const actual = INTS.map((text) =>
      Either.match(pythonInt(text), { onLeft: () => 'ERROR', onRight: String }),
    );
    const expected = oracle().int.map((outcome) => (outcome.ok ? String(outcome.value) : 'ERROR'));
    expect(keyed(INTS, actual)).toEqual(keyed(INTS, expected));
  });

  test('float() agrees bit for bit', () => {
    const actual = FLOATS.map((text) =>
      Either.match(pythonFloat(text), { onLeft: () => 'ERROR', onRight: floatBits }),
    );
    const expected = oracle().float.map((outcome) => (outcome.ok ? String(outcome.value) : 'ERROR'));
    expect(keyed(FLOATS, actual)).toEqual(keyed(FLOATS, expected));
  });

  test('repr() agrees on the values argparse would quote', () => {
    expect(keyed(REPRS, REPRS.map(pythonRepr))).toEqual(keyed(REPRS, oracle().repr));
  });

  test('_identity agrees on the digest material', () => {
    const actual = IDENTITY_MATERIAL.map(([repoRoot, upstreamServiceUrl, runsRoot, cacheRoot, viewer]) =>
      gatewayIdentity({
        repoRoot,
        upstreamServiceUrl,
        runsRoot,
        cacheRoot,
        viewerPublicUrl: Option.fromNullable(viewer),
      }),
    );
    expect(actual).toEqual([...oracle().identity]);
    expect(actual.every((digest) => Gateway.GATEWAY_IDENTITY_RE.test(digest))).toBe(true);
  });

  test('Path.expanduser().resolve() agrees, symlinks and all', async () => {
    const environment = { cwd: fixture.real, home: Option.some('/home/nobody') };
    const actual = await runFs(
      Effect.forEach(RESOLVE_PATHS, (path) =>
        Effect.map(Effect.either(resolvePath(path, environment)), (result) => tidy(asOutcome(result, identity))),
      ),
    );
    expect(keyed(RESOLVE_PATHS, actual)).toEqual(keyed(RESOLVE_PATHS, oracle().resolve.map(tidy)));
  });

  test('gateway_config agrees field for field, including the identity', async () => {
    const actual = await runFs(
      Effect.forEach(specs.value, (spec) =>
        Effect.map(Effect.either(makeGatewayConfig(spec)), (result) =>
          tidy(
            asOutcome(result, (config) => ({
              repo_root: config.repoRoot,
              upstream_service_url: config.upstreamServiceUrl,
              runs_root: config.runsRoot,
              cache_root: config.cacheRoot,
              identity: config.identity,
              upstream_timeout_s: floatBits(config.upstreamTimeoutSeconds),
              viewer_public_url: Option.getOrNull(config.viewerPublicUrl),
            })),
          ),
        ),
      ),
    );
    expect(actual).toEqual(oracle().config.map(tidy));
  });

  test('every URL the port normalizes is one @arena/wire calls normal', () => {
    const accepted = SERVICE_URLS.map(normalizeServiceUrl)
      .filter(Either.isRight)
      .map((either) => either.right);
    expect(accepted.length).toBeGreaterThan(20);
    /**
     * `Gateway.isNormalizedServiceUrl` decides by round-tripping through
     * `URL`, and its own doc comment names the consequence: anything `URL`
     * *rewrites* is reported as non-normal even though `urlsplit` produced it
     * and the gateway will happily serve it.  Pinning the exact set here means
     * a change on either side of that seam shows up as a test failure rather
     * than as a `/health` payload that suddenly fails to decode.
     *
     * None of these seven is reachable from `local_stack.py`, which passes an
     * `http://127.0.0.1:{port}` origin.
     */
    expect(accepted.filter((url) => !Gateway.isNormalizedServiceUrl(url))).toEqual([
      'http://h/a b', // a literal space becomes `%20`
      'http://h/\u00e9', // a non-ASCII path byte becomes `%c3%a9`
      'http://h%2f/x', // `URL` decodes `%2f` in a host and then rejects the `/`
      'http://[::ffff:1.2.3.4]', // `URL` re-renders the mapped form as `[::ffff:102:304]`
      'http://h/%2e', // `URL` decodes `%2e` and then removes the dot segment
      'http://[::1%25lo0]', // `URL` rejects a zone id outright
      'http://[fe80::1%lo0]',
    ]);
    // Everything a real invocation produces round-trips.
    expect(
      ['http://127.0.0.1:5000/freeciv', 'http://[::1]', 'https://example.com/freeciv'].every((url) =>
        Gateway.isNormalizedServiceUrl(url),
      ),
    ).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// The flag surface
// ---------------------------------------------------------------------------

const CLI_ENVIRONMENT = Layer.mergeAll(NodeFileSystem.layer, NodePath.layer, NodeTerminal.layer);

/**
 * Parse an argv tail through the real command descriptor.
 *
 * `CommandDescriptor.parse` is the layer under `Command.run`; going through it
 * keeps the flag matrix quiet (`Command.run` renders every failure to the
 * console on its way out) while exercising exactly the same parser.
 */
const parseArgs = (args: readonly string[]): Promise<Either.Either<GatewayCliArgs, string>> =>
  Effect.runPromise(
    Effect.provide(
      Effect.match(
        CommandDescriptor.parse(
          gatewayCommand.descriptor,
          [GATEWAY_CLI_NAME, ...args],
          CliConfig.defaultConfig,
        ),
        {
          onFailure: (error) => Either.left(HelpDoc.toAnsiText(error.error).trim()),
          onSuccess: (directive) =>
            directive._tag === 'UserDefined'
              ? Either.right(directive.value)
              : Either.left(`built-in: ${directive._tag}`),
        },
      ),
      CLI_ENVIRONMENT,
    ),
  );

const messageOf = (result: Either.Either<GatewayCliArgs, string>): string =>
  Either.match(result, { onLeft: (message) => message, onRight: () => 'parsed' });

const REQUIRED: readonly string[] = [
  '--service-url',
  'http://h',
  '--runs-root',
  '/r',
  '--cache-root',
  '/c',
  '--ready-file',
  '/ready.json',
];

describe('_parser() — the flag surface', () => {
  test('the four required flags alone produce every default', async () => {
    expect(Either.getOrThrow(await parseArgs(REQUIRED))).toEqual({
      host: DEFAULT_GATEWAY_HOST,
      port: 0n,
      serviceUrl: 'http://h',
      runsRoot: '/r',
      cacheRoot: '/c',
      repoRoot: DEFAULT_REPO_ROOT,
      readyFile: '/ready.json',
      upstreamTimeoutSeconds: 10,
      viewerPublicUrl: Option.none(),
    } satisfies GatewayCliArgs);
  });

  test('the declared defaults are argparse\u2019s', () => {
    expect([DEFAULT_GATEWAY_HOST, DEFAULT_GATEWAY_PORT, DEFAULT_UPSTREAM_TIMEOUT_SECONDS]).toEqual([
      '127.0.0.1',
      '0',
      '10',
    ]);
    expect(PYTHON_GATEWAY_PROG).toBe('python3 -m agent_eval.replay_gateway');
    expect(GATEWAY_CLI_ERROR_EXIT_CODE).toBe(2);
  });

  test('--repo-root defaults to the same checkout REPO_ROOT names', async () => {
    expect(
      await Bun.file(
        join(DEFAULT_REPO_ROOT, 'arena', 'archive', 'agent_eval', 'replay_gateway.py'),
      ).exists(),
    ).toBe(true);
    const child = Bun.spawnSync([
      'python3',
      '-c',
      'import sys; sys.path.insert(0, sys.argv[1]); from agent_eval.replay_gateway import REPO_ROOT; print(REPO_ROOT, end="")',
      PYTHON_ROOT,
    ]);
    expect(DEFAULT_REPO_ROOT).toBe(child.stdout.toString());
  });

  test('every flag together parses to every value', async () => {
    const parsed = await parseArgs([
      '--host',
      '::1',
      '--port',
      '5000',
      '--service-url',
      'http://up:1/x',
      '--runs-root',
      '/runs',
      '--cache-root',
      '/cache',
      '--repo-root',
      '/repo',
      '--ready-file',
      '/ready.json',
      '--upstream-timeout-s',
      '2.5',
      '--viewer-public-url',
      'https://v',
    ]);
    expect(Either.getOrThrow(parsed)).toEqual({
      host: '::1',
      port: 5000n,
      serviceUrl: 'http://up:1/x',
      runsRoot: '/runs',
      cacheRoot: '/cache',
      repoRoot: '/repo',
      readyFile: '/ready.json',
      upstreamTimeoutSeconds: 2.5,
      viewerPublicUrl: Option.some('https://v'),
    } satisfies GatewayCliArgs);
  });

  test('the argv local_stack.py actually spawns parses', async () => {
    // Copied from a live `python3 -m agent_eval.replay_gateway` command line
    // (`local_stack.py:540-548`): eight flags, `--upstream-timeout-s` omitted.
    const parsed = await parseArgs([
      '--host',
      '127.0.0.1',
      '--port',
      '0',
      '--service-url',
      'http://127.0.0.1:62188',
      '--viewer-public-url',
      'https://freeciv.localhost',
      '--runs-root',
      '/repo/.agent-eval/runs',
      '--cache-root',
      '/repo/.agent-eval/replay-cache',
      '--repo-root',
      '/repo',
      '--ready-file',
      '/repo/.agent-eval/local-stack/gateway-77873-d06bcf4656.json',
    ]);
    expect(Either.getOrThrow(parsed)).toEqual({
      host: '127.0.0.1',
      port: 0n,
      serviceUrl: 'http://127.0.0.1:62188',
      runsRoot: '/repo/.agent-eval/runs',
      cacheRoot: '/repo/.agent-eval/replay-cache',
      repoRoot: '/repo',
      readyFile: '/repo/.agent-eval/local-stack/gateway-77873-d06bcf4656.json',
      upstreamTimeoutSeconds: 10,
      viewerPublicUrl: Option.some('https://freeciv.localhost'),
    } satisfies GatewayCliArgs);
  });

  test('--flag=value is accepted, as argparse accepts it', async () => {
    const parsed = await parseArgs([
      '--service-url=http://h',
      '--runs-root=/r',
      '--cache-root=/c',
      '--ready-file=/ready.json',
      '--port=7',
    ]);
    expect(Either.getOrThrow(parsed).port).toBe(7n);
  });

  test('there are exactly nine flags, and no positional arguments', () => {
    expect(Object.keys(gatewayCommand.descriptor).length).toBeGreaterThan(0);
    const usage = HelpDoc.toAnsiText(CommandDescriptor.getHelp(gatewayCommand.descriptor, CliConfig.defaultConfig));
    const declared = [
      '--host',
      '--port',
      '--service-url',
      '--runs-root',
      '--cache-root',
      '--repo-root',
      '--ready-file',
      '--upstream-timeout-s',
      '--viewer-public-url',
    ];
    expect(declared.filter((flag) => usage.includes(flag))).toEqual(declared);
  });

  test('each required flag is required', async () => {
    const withoutEach: readonly (readonly [string, readonly string[]])[] = [
      ['service-url', ['--runs-root', '/r', '--cache-root', '/c', '--ready-file', '/f']],
      ['runs-root', ['--service-url', 'http://h', '--cache-root', '/c', '--ready-file', '/f']],
      ['cache-root', ['--service-url', 'http://h', '--runs-root', '/r', '--ready-file', '/f']],
      ['ready-file', ['--service-url', 'http://h', '--runs-root', '/r', '--cache-root', '/c']],
    ];
    const results = await Promise.all(withoutEach.map(([, args]) => parseArgs(args)));
    expect(results.map(Either.isLeft)).toEqual([true, true, true, true]);
    expect(results.map((result, index) => messageOf(result).includes(withoutEach[index]![0]))).toEqual(
      [true, true, true, true],
    );
  });

  test('each optional flag is optional, and takes a value when given', async () => {
    const optional: readonly (readonly [string, string])[] = [
      ['--host', '::1'],
      ['--port', '1'],
      ['--repo-root', '/elsewhere'],
      ['--upstream-timeout-s', '1'],
      ['--viewer-public-url', 'http://v'],
    ];
    const results = await Promise.all(
      optional.map(([flag, value]) => parseArgs([...REQUIRED, flag, value])),
    );
    expect(results.map(Either.isRight)).toEqual([true, true, true, true, true]);
    expect(Either.isRight(await parseArgs(REQUIRED))).toBe(true);
  });

  test('--port carries Python int() semantics all the way through', async () => {
    const cases: readonly (readonly [string, bigint])[] = [
      ['5_0', 50n],
      ['+3', 3n],
      [' 5 ', 5n],
      ['\u0663', 3n],
      ['99999999999999999999', 99999999999999999999n],
    ];
    const parsed = await Promise.all(cases.map(([text]) => parseArgs([...REQUIRED, '--port', text])));
    expect(parsed.map((result) => Either.getOrThrow(result).port)).toEqual(
      cases.map(([, value]) => value),
    );
  });

  test('--upstream-timeout-s carries Python float() semantics', async () => {
    const parsed = await Promise.all(
      ['1e3', '.5', '1_0.5', 'inf'].map((text) =>
        parseArgs([...REQUIRED, '--upstream-timeout-s', text]),
      ),
    );
    expect(parsed.map((result) => Either.getOrThrow(result).upstreamTimeoutSeconds)).toEqual([
      1000, 0.5, 10.5, Number.POSITIVE_INFINITY,
    ]);
  });

  test('a value int() rejects is reported the way argparse reports it', async () => {
    expect(messageOf(await parseArgs([...REQUIRED, '--port', '0x10']))).toContain(
      "argument --port: invalid int value: '0x10'",
    );
  });

  test('a value float() rejects is reported the way argparse reports it', async () => {
    expect(messageOf(await parseArgs([...REQUIRED, '--upstream-timeout-s', 'soon']))).toContain(
      "argument --upstream-timeout-s: invalid float value: 'soon'",
    );
  });
});

// ---------------------------------------------------------------------------
// Construction order, the layer, and the helper `main.ts` will need
// ---------------------------------------------------------------------------

const BASE: GatewayConfigInput = {
  host: '127.0.0.1',
  port: 0n,
  serviceUrl: 'http://up:1',
  runsRoot: '/runs',
  cacheRoot: '/cache',
  repoRoot: '/repo',
  readyFile: '/ready.json',
  upstreamTimeoutSeconds: 10,
  viewerPublicUrl: Option.none(),
};

const failureOf = (input: GatewayConfigInput): Promise<string> =>
  runFs(
    Effect.map(Effect.either(makeGatewayConfig(input)), (result) =>
      Either.match(result, { onLeft: (error) => error.message, onRight: () => 'no failure' }),
    ),
  );

describe('makeGatewayConfig', () => {
  test('reports host, then port, then timeout, then the URLs', async () => {
    // Each call below leaves one more field valid than the last, so the
    // message that changes is the one whose check just started passing.
    expect(
      await failureOf({
        ...BASE,
        host: 'localhost',
        port: -1n,
        upstreamTimeoutSeconds: 0,
        serviceUrl: 'ftp://h',
      }),
    ).toBe(GATEWAY_CONFIG_MESSAGES.hostNotLiteral);
    expect(
      await failureOf({ ...BASE, port: -1n, upstreamTimeoutSeconds: 0, serviceUrl: 'ftp://h' }),
    ).toBe(GATEWAY_CONFIG_MESSAGES.portOutOfRange);
    expect(await failureOf({ ...BASE, upstreamTimeoutSeconds: 0, serviceUrl: 'ftp://h' })).toBe(
      GATEWAY_CONFIG_MESSAGES.timeoutNotPositive,
    );
    expect(await failureOf({ ...BASE, serviceUrl: 'ftp://h' })).toBe(
      GATEWAY_CONFIG_MESSAGES.serviceUrlNotHttp,
    );
    expect(await failureOf({ ...BASE, viewerPublicUrl: Option.some('http://v#f') })).toBe(
      GATEWAY_CONFIG_MESSAGES.serviceUrlCredentials,
    );
    expect(await failureOf(BASE)).toBe('no failure');
  });

  test('a non-loopback literal and a hostname fail differently', async () => {
    expect(await failureOf({ ...BASE, host: '0.0.0.0' })).toBe(
      GATEWAY_CONFIG_MESSAGES.hostNotLoopback,
    );
    expect(await failureOf({ ...BASE, host: 'localhost' })).toBe(
      GATEWAY_CONFIG_MESSAGES.hostNotLiteral,
    );
    expect(await failureOf({ ...BASE, host: '::1' })).toBe('no failure');
  });

  test('the port range is [0, 65535] and Python-int wide', async () => {
    const rejected = await Promise.all(
      [-1n, 65536n, 99999999999999999999n].map((port) => failureOf({ ...BASE, port })),
    );
    expect(rejected).toEqual([
      GATEWAY_CONFIG_MESSAGES.portOutOfRange,
      GATEWAY_CONFIG_MESSAGES.portOutOfRange,
      GATEWAY_CONFIG_MESSAGES.portOutOfRange,
    ]);
    const accepted = await Promise.all(
      [0n, 1n, 65535n].map((port) => failureOf({ ...BASE, port })),
    );
    expect(accepted).toEqual(['no failure', 'no failure', 'no failure']);
  });

  test('the timeout must be finite and positive', async () => {
    const rejected = await Promise.all(
      [0, -1, Number.NaN, Number.POSITIVE_INFINITY].map((upstreamTimeoutSeconds) =>
        failureOf({ ...BASE, upstreamTimeoutSeconds }),
      ),
    );
    expect(rejected).toEqual(Array.from({ length: 4 }, () => GATEWAY_CONFIG_MESSAGES.timeoutNotPositive));
    expect(await failureOf({ ...BASE, upstreamTimeoutSeconds: 0.001 })).toBe('no failure');
  });

  test('the identity hashes exactly what @arena/wire reads back off /health', async () => {
    const config = await runFs(
      Effect.orDie(makeGatewayConfig({ ...BASE, viewerPublicUrl: Option.some('https://viewer:443/x/') })),
    );
    expect(config.viewerPublicUrl).toEqual(Option.some('https://viewer/x'));
    const identity: Gateway.GatewayIdentity = {
      schema_version: 1n,
      ok: true,
      kind: Gateway.GATEWAY_KIND,
      protocol_version: BigInt(Gateway.GATEWAY_PROTOCOL_VERSION),
      identity: Gateway.GatewayIdentityToken.make(config.identity),
      pid: 1n,
      host: config.host,
      port: 8080n,
      url: Gateway.gatewaySelfUrl(config.host, 8080),
      repo_root: config.repoRoot,
      upstream_service_url: config.upstreamServiceUrl,
      runs_root: config.runsRoot,
      cache_root: config.cacheRoot,
      viewer_public_url: Option.getOrThrow(config.viewerPublicUrl),
    };
    expect(Gateway.gatewayIdentityMaterial(identity).split('\0')).toEqual([
      config.repoRoot,
      config.upstreamServiceUrl,
      config.runsRoot,
      config.cacheRoot,
      'https://viewer/x',
    ]);
    expect(Gateway.isGatewayIdentity(identity)).toBe(true);
  });

  test('an absent viewer URL hashes as the empty string, not as "None"', () => {
    const material = {
      repoRoot: '/a',
      upstreamServiceUrl: 'http://h',
      runsRoot: '/b',
      cacheRoot: '/c',
    };
    expect(gatewayIdentity({ ...material, viewerPublicUrl: Option.none() })).toBe(
      gatewayIdentity({ ...material, viewerPublicUrl: Option.some('') }),
    );
    expect(gatewayIdentity({ ...material, viewerPublicUrl: Option.some('http://v') })).not.toBe(
      gatewayIdentity({ ...material, viewerPublicUrl: Option.none() }),
    );
  });

  test('the layer hands one validated configuration to its consumer', async () => {
    const values = await Effect.runPromise(
      Effect.provide(
        Effect.provide(GatewayConfig, gatewayConfigLayer(BASE)).pipe(Effect.orDie),
        FILE_SYSTEM,
      ),
    );
    expect(values.host).toBe('127.0.0.1');
    expect(values.upstreamServiceUrl).toBe('http://up:1');
    expect(values.port).toBe(0);
    expect(values.readyFile).toBe('/ready.json');
    expect(values.identity).toMatch(Gateway.GATEWAY_IDENTITY_RE);
  });

  test('a rejected configuration fails the layer rather than throwing', async () => {
    const outcome = await Effect.runPromise(
      Effect.provide(
        Effect.either(Effect.provide(Effect.void, gatewayConfigLayer({ ...BASE, host: '8.8.8.8' }))),
        FILE_SYSTEM,
      ),
    );
    expect(Either.isLeft(outcome)).toBe(true);
    expect(
      Either.match(outcome, {
        onLeft: (error) => formatStartupError(error),
        onRight: () => 'no failure',
      }),
    ).toBe('error: gateway host must be a loopback address');
  });
});

describe('the Python primitives the rest of the port will reuse', () => {
  test('pythonStrip removes what str.strip() removes, and nothing else', () => {
    expect(pythonStrip('\u001c\u0085 x \u00a0')).toBe('x');
    // JavaScript's trim() would also eat U+FEFF; Python's strip() does not.
    expect(pythonStrip('\ufeffx\ufeff')).toBe('\ufeffx\ufeff');
    expect('\ufeffx\ufeff'.trim()).toBe('x');
    expect(pythonStrip('   ')).toBe('');
  });

  test('expandUser expands ~ and only as the first component', () => {
    const home = Option.some('/home/me');
    expect(expandUser('~', home)).toEqual(Either.right('/home/me'));
    expect(expandUser('~/runs', home)).toEqual(Either.right('/home/me/runs'));
    expect(expandUser('/a/~/b', home)).toEqual(Either.right('/a/~/b'));
    expect(Either.isLeft(expandUser('~', Option.none()))).toBe(true);
    expect(Either.isLeft(expandUser('~someone/x', home))).toBe(true);
  });

  test('a GatewayConfigError is a value carrying the Python text', () => {
    const failure = Either.flip(normalizeServiceUrl('ftp://h'));
    const error = Either.getOrThrow(failure);
    expect(error).toBeInstanceOf(GatewayConfigError);
    expect(error._tag).toBe('GatewayConfigError');
    expect(formatStartupError(error)).toBe('error: service URL must be an http(s) URL');
  });
});

// ---------------------------------------------------------------------------
// `--backend` / `--database-url` — the two TypeScript-only flags
// ---------------------------------------------------------------------------

/**
 * The record the nine shared flags produced *before* a backend could be
 * selected, copied from `_parser() — the flag surface`'s first test.
 *
 * Its job here is the opposite of that test's: not "the defaults are
 * argparse's" but "adding two flags did not move one of them".  A parse of an
 * argv that names neither flag must still equal this object — `backend` is
 * `undefined`, which is exactly what `toEqual` ignores and what the layer
 * selection reads as "the filesystem".
 */
const NINE_FLAG_SNAPSHOT: GatewayCliArgs = {
  host: DEFAULT_GATEWAY_HOST,
  port: 0n,
  serviceUrl: 'http://h',
  runsRoot: '/r',
  cacheRoot: '/c',
  repoRoot: DEFAULT_REPO_ROOT,
  readyFile: '/ready.json',
  upstreamTimeoutSeconds: 10,
  viewerPublicUrl: Option.none(),
};

const DATABASE_URL = 'postgres://arena:hunter2@127.0.0.1:5432/arena_wf_demo';

const backendOf = (
  result: Either.Either<GatewayCliArgs, string>,
): PostgresBackendInput | undefined => Either.getOrThrow(result).backend;

describe('--backend / --database-url — additive, TypeScript-only, default off', () => {
  test('an argv naming neither flag parses to the record it always did', async () => {
    const parsed = Either.getOrThrow(await parseArgs(REQUIRED));
    expect(parsed).toEqual(NINE_FLAG_SNAPSHOT);
    // Spelled out, because `toEqual` is what ignores an undefined value and
    // this is the property the whole scoping argument rests on: the key exists
    // and carries nothing.
    expect(parsed.backend).toBeUndefined();
    expect(Object.keys(parsed).toSorted()).toEqual(
      [...Object.keys(NINE_FLAG_SNAPSHOT), 'backend'].toSorted(),
    );
  });

  test('--backend fs is the default spelled out, and needs no database URL', async () => {
    expect(DEFAULT_GATEWAY_BACKEND).toBe('fs');
    expect(GATEWAY_BACKENDS).toEqual(['fs', 'postgres']);
    const parsed = Either.getOrThrow(await parseArgs([...REQUIRED, GATEWAY_BACKEND_FLAG, 'fs']));
    expect(parsed).toEqual(NINE_FLAG_SNAPSHOT);
    expect(parsed.backend).toBeUndefined();
  });

  test('--backend postgres carries the URL, redacted, and nothing else changes', async () => {
    const parsed = Either.getOrThrow(
      await parseArgs([...REQUIRED, GATEWAY_BACKEND_FLAG, 'postgres', DATABASE_URL_FLAG, DATABASE_URL]),
    );
    expect(parsed.backend?._tag).toBe('Postgres');
    expect(parsed).toEqual({
      ...NINE_FLAG_SNAPSHOT,
      backend: { _tag: 'Postgres', databaseUrl: Redacted.make(DATABASE_URL) },
    });
    // The backend carries a URL and **nothing else**.  It used to carry
    // `--materialize-root`; a pg gateway reads `<--runs-root>/<game-id>` now,
    // so a path here would be a path the gateway does not use.
    expect(Object.keys(parsed.backend ?? {}).toSorted()).toEqual(['_tag', 'databaseUrl']);
    const backend = parsed.backend;
    expect(backend === undefined ? '' : Redacted.value(backend.databaseUrl)).toBe(DATABASE_URL);
  });

  test('the URL is not printable: it survives only through Redacted.value', async () => {
    const backend = backendOf(
      await parseArgs([...REQUIRED, GATEWAY_BACKEND_FLAG, 'postgres', DATABASE_URL_FLAG, DATABASE_URL]),
    );
    // The three renderings a leak would travel by: a serialized record, the
    // value on its own, and the one function that turns a startup failure into
    // the `error: …` line on stderr.
    const rendered = [
      JSON.stringify(backend),
      JSON.stringify(backend?.databaseUrl),
      describeStartupError(backend),
    ].join('\n');
    expect(rendered).not.toContain('hunter2');
    expect(rendered).not.toContain(DATABASE_URL);
    expect(JSON.stringify(backend?.databaseUrl)).toContain('redacted');
  });

  test('--backend postgres without --database-url fails exactly as a missing required flag does', async () => {
    // The differential: the message for a flag argparse-style *declares*
    // required, with the flag name swapped.  If `@effect/cli` rewords one, the
    // two expectations move together or this test fails.
    const declaredRequired = messageOf(
      await parseArgs(['--runs-root', '/r', '--cache-root', '/c', '--ready-file', '/f']),
    );
    expect(declaredRequired).toContain(missingOptionMessage('--service-url'));
    const conditional = messageOf(await parseArgs([...REQUIRED, GATEWAY_BACKEND_FLAG, 'postgres']));
    expect(conditional).toContain(missingOptionMessage(DATABASE_URL_FLAG));
    expect(conditional).toContain(DATABASE_URL_FLAG);
  });

  test('--database-url without --backend postgres is refused, not ignored', async () => {
    const messages = await Promise.all(
      [[...REQUIRED], [...REQUIRED, GATEWAY_BACKEND_FLAG, 'fs']].map(async (base) =>
        messageOf(await parseArgs([...base, DATABASE_URL_FLAG, DATABASE_URL])),
      ),
    );
    expect(messages.map((message) => message.includes(DATABASE_URL_WITHOUT_POSTGRES))).toEqual([
      true,
      true,
    ]);
    expect(messages.join('\n')).not.toContain('hunter2');
  });

  test('a backend that is neither fs nor postgres is a validation error', async () => {
    const message = messageOf(await parseArgs([...REQUIRED, GATEWAY_BACKEND_FLAG, 'sqlite']));
    expect(message).not.toBe('parsed');
    // `@effect/cli` reports the alternatives rather than echoing the value.
    expect(message).toBe(`Expected one of the following cases: ${GATEWAY_BACKENDS.join(', ')}`);
  });

  test('both flags are discoverable in the usage block, beside the nine', async () => {
    const usage = HelpDoc.toAnsiText(
      CommandDescriptor.getHelp(gatewayCommand.descriptor, CliConfig.defaultConfig),
    );
    expect([GATEWAY_BACKEND_FLAG, DATABASE_URL_FLAG].filter((flag) => usage.includes(flag))).toEqual(
      [GATEWAY_BACKEND_FLAG, DATABASE_URL_FLAG],
    );
    // …and the nine `_parser()` shares are still there, unmoved.
    expect(usage.includes('--service-url') && usage.includes('--viewer-public-url')).toBe(true);
  });

  test('--materialize-root is gone, loudly: an unknown argument, not an ignored one', () => {
    // The flag named where a pg gateway staged the savegames the python bridge
    // reads.  There is no such place any more — both backends point the bridge
    // at `--runs-root` — so the flag was **removed** rather than accepted and
    // ignored, which is the failure mode this test exists to forbid: an
    // operator who passes it must be told, not silently obeyed in a way that
    // does nothing.
    const usage = HelpDoc.toAnsiText(
      CommandDescriptor.getHelp(gatewayCommand.descriptor, CliConfig.defaultConfig),
    );
    expect(usage).not.toContain('--materialize-root');
    // Through the **real** entry point, not through `parseArgs`.  Measured, and
    // it is why: `CommandDescriptor.parse` is the option matcher and answers
    // `UserDefined` with the stray token merely *unconsumed*, so a
    // `parseArgs`-based assertion here would have read "parsed" and passed for
    // the wrong reason.  `Command.run` is the layer that refuses leftovers, and
    // it is the layer `main` actually uses.
    const child = Bun.spawnSync(
      ['bun', join(REPO_ROOT, 'arena/harness/src/gateway/main.ts'), ...REQUIRED, '--materialize-root', '/elsewhere'],
      { cwd: join(REPO_ROOT, 'arena/harness') },
    );
    const output = `${child.stdout.toString()}${child.stderr.toString()}`;
    expect(output).toContain('--materialize-root');
    expect(output.toLowerCase()).toContain('unknown');
    expect(child.exitCode).toBe(GATEWAY_CLI_ERROR_EXIT_CODE);
  });

  test('the config carries the backend through, and the identity digest still ignores it', async () => {
    const backend: PostgresBackendInput = {
      _tag: 'Postgres',
      databaseUrl: Redacted.make(DATABASE_URL),
    };
    const [plain, postgres] = await Promise.all([
      runFs(Effect.orDie(makeGatewayConfig(BASE))),
      runFs(Effect.orDie(makeGatewayConfig({ ...BASE, backend }))),
    ]);
    expect(plain.backend).toBeUndefined();
    expect(postgres.backend?.databaseUrl).toBe(backend.databaseUrl);
    // `local_stack.py` derives `replay-gateway-{identity}.json` from this
    // digest, and the URL may hold a password: neither may enter it.
    expect(postgres.identity).toBe(plain.identity);
  });
});

// ---------------------------------------------------------------------------
// The backend adds no construction-time failure — the ordering contract, now
// provable by exhaustion rather than by ordering
// ---------------------------------------------------------------------------

const postgresInput = (): GatewayConfigInput => ({
  ...BASE,
  backend: { _tag: 'Postgres', databaseUrl: Redacted.make(DATABASE_URL) },
});

describe('--backend postgres cannot be the reason a configuration is refused', () => {
  test('every construction-time message is one CPython could have produced', () => {
    // The strongest form of the ordering contract `makeGatewayConfig` used to
    // keep by *running the backend check last*: there is no TypeScript-only
    // message left to order.  `materializeRootNestsCacheRoot` was the only one,
    // and it died with the flag that produced it.
    expect(Object.keys(GATEWAY_CONFIG_MESSAGES).toSorted()).toEqual(
      [
        'hostNotLiteral',
        'hostNotLoopback',
        'noHomeDirectory',
        'portOutOfRange',
        'serviceUrlControlCharacters',
        'serviceUrlCredentials',
        'serviceUrlDotSegments',
        'serviceUrlInvalidPort',
        'serviceUrlNotHttp',
        'timeoutNotPositive',
      ].toSorted(),
    );
  });

  test('a valid nine-flag argv stays valid when a backend is added to it', async () => {
    expect(await failureOf(postgresInput())).toBe('no failure');
  });

  test('an argv that is wrong reports the Python failure, backend or no backend', async () => {
    const wrong = { host: 'localhost' } as const;
    expect([
      await failureOf({ ...BASE, ...wrong }),
      await failureOf({ ...postgresInput(), ...wrong }),
    ]).toEqual([GATEWAY_CONFIG_MESSAGES.hostNotLiteral, GATEWAY_CONFIG_MESSAGES.hostNotLiteral]);
  });

  test('the resolved config is the plain one plus exactly the backend field', async () => {
    const [plain, postgres] = await Promise.all([
      runFs(Effect.orDie(makeGatewayConfig(BASE))),
      runFs(Effect.orDie(makeGatewayConfig(postgresInput()))),
    ]);
    // No path, no root, no derived directory: `--backend postgres` changes
    // *where the manifest comes from* and not one resolved value.
    expect({ ...postgres, backend: undefined }).toEqual({ ...plain, backend: undefined });
  });
});

// ---------------------------------------------------------------------------
// main.ts: the layer selection, and the module graph the fs path keeps
// ---------------------------------------------------------------------------

/** `archiveServices`' requirements: both arms resolve real paths through them. */
const PLATFORM = Layer.mergeAll(NodeFileSystem.layer, NodePath.layer);

/** A URL `@arena/db` refuses before it opens a socket — no server is contacted. */
const NOT_POSTGRES_URL = 'mysql://arena:hunter2@127.0.0.1:3306/arena_wf_demo';

describe('main.ts — which layers the backend selects', () => {
  test('no backend builds the filesystem repository and the python derivation bridge', async () => {
    const config = await runFs(Effect.orDie(makeGatewayConfig(BASE)));
    const services = await Effect.runPromise(
      Effect.provide(
        Effect.provide(Effect.all([RunsRepository, ReplayDerivation]), archiveServices(config)),
        PLATFORM,
      ).pipe(Effect.orDie),
    );
    expect(services[0].runsRoot).toBe(config.runsRoot);
    expect(
      [services[1].replay, services[1].board, services[1].events].every(Predicate.isFunction),
    ).toBe(true);
  });

  test('--backend postgres builds @arena/db’s layers, and an unusable URL is exit 2', async () => {
    const config = await runFs(
      Effect.orDie(
        makeGatewayConfig({
          ...BASE,
          backend: { _tag: 'Postgres', databaseUrl: Redacted.make(NOT_POSTGRES_URL) },
        }),
      ),
    );
    const outcome = await Effect.runPromise(
      Effect.provide(
        Effect.either(Effect.provide(RunsRepository, archiveServices(config))),
        PLATFORM,
      ),
    );
    expect(Either.isLeft(outcome)).toBe(true);
    const message = Either.match(outcome, {
      onLeft: (error) => describeStartupError(error),
      onRight: () => 'no failure',
    });
    // The layer was really built — this is `@arena/db`'s own refusal, reported
    // as the gateway's startup error.
    expect(message).toContain('DatabaseUnavailable');
    expect(message).toContain(UNUSABLE_DATABASE_URL);
    expect(message).toContain('postgres://');
    // …and it names no credential, no host and no URL of its own.
    expect(message).not.toContain('hunter2');
    expect(message).not.toContain(NOT_POSTGRES_URL);
  });

  test('the pool is pinned small enough for a matrix of gateways', () => {
    // `max_connections` is 100 on a stock server; the rig boots one gateway per
    // scenario, nine of them, and can run two matrices at once.  The driver's
    // unset default of 10 asks for 180 — measured as a rig failure.
    const SCENARIOS = 9;
    const CONCURRENT_MATRICES = 2;
    const STOCK_MAX_CONNECTIONS = 100;
    expect(GATEWAY_MAX_DB_CONNECTIONS * SCENARIOS * CONCURRENT_MATRICES).toBeLessThan(
      STOCK_MAX_CONNECTIONS,
    );
    // …and still more than one, so a request that reads two rows concurrently
    // does not serialize on the pool.
    expect(GATEWAY_MAX_DB_CONNECTIONS).toBeGreaterThan(1);
    expect(GATEWAY_DB_APPLICATION_NAME).toContain('gateway');
  });

  test('a failed @arena/db load says what to do about it', () => {
    expect(POSTGRES_BACKEND_UNAVAILABLE).toContain('bun install');
    expect(POSTGRES_BACKEND_UNAVAILABLE).toContain(`${GATEWAY_BACKEND_FLAG} postgres`);
  });

  test('the filesystem gateway never resolves @arena/db or a database driver', async () => {
    // The claim the dynamic import exists for, as a measurement: a Bun plugin
    // records every module resolution the gateway's entry point performs, and
    // the pg half must not appear among them.  A static import would.
    const probe = `
      const seen = [];
      Bun.plugin({
        name: 'module-graph-probe',
        setup(build) {
          build.onResolve({ filter: /.*/ }, (args) => { seen.push(args.path); return undefined; });
        },
      });
      await import(${JSON.stringify(join(REPO_ROOT, 'arena/harness/src/gateway/main.ts'))});
      const hit = (needle) => seen.filter((path) => path.includes(needle)).length;
      console.log(JSON.stringify({
        total: seen.length,
        db: hit('@arena/db'),
        sqlPg: hit('sql-pg'),
        driver: seen.filter((path) => path === 'pg' || path.startsWith('pg/')).length,
      }));
    `;
    const child = Bun.spawnSync(['bun', '-e', probe], { cwd: join(REPO_ROOT, 'arena/harness') });
    const ModuleGraphReport = Schema.parseJson(
      Schema.Struct({
        total: Schema.Number,
        db: Schema.Number,
        sqlPg: Schema.Number,
        driver: Schema.Number,
      }),
    );
    const report = Either.getOrThrowWith(
      Schema.decodeUnknownEither(ModuleGraphReport)(child.stdout.toString().trim()),
      (error) => new Error(String(error)),
    );
    expect(child.exitCode).toBe(0);
    // A real graph was walked, and none of it was the database.
    expect(report.total).toBeGreaterThan(100);
    expect([report.db, report.sqlPg, report.driver]).toEqual([0, 0, 0]);
  });
});

// ---------------------------------------------------------------------------
// The pg repository decorator: logging, and nothing else
// ---------------------------------------------------------------------------

/**
 * A repository whose reads record their overlap.
 *
 * It answers after a tick, and counts how many callers were inside it at once.
 * The decorator used to hold a per-game semaphore and this probe pinned it at
 * `1`, back when a pg gateway staged savegames into a materialization root and
 * two concurrent reads of one game could race to write the same directory.
 * Nothing is staged any more — both backends point the python bridge at
 * `<--runs-root>/<game-id>` — so this probe pins the *opposite* claim: the
 * decorator logs and does nothing else.  `terminalArchive` reads rows and a
 * `victory.json`, and `frameFile`/`videoFile` are directory listings and an
 * `lstat` on a tree no gateway writes, so a lock around any of them would be a
 * queue in front of reads that cannot conflict.
 */
interface OverlapProbe {
  readonly repository: RunsRepositoryApi;
  readonly peak: () => number;
}

const archiveOf = (gameId: string): TerminalArchive => {
  const decoded = Either.getOrThrowWith(decodeGameId(gameId), (error) =>
    new Error(String(error)),
  );
  const view = Either.getOrThrowWith(
    terminalArchiveView(
      decoded,
      { state: 'completed' },
      { manifest: { game_id: decoded } },
      Option.none(),
    ),
    (error) => new Error(error),
  );
  return { ...view, runRoot: `/runs/${gameId}` };
};

const overlapProbe = (): OverlapProbe => {
  const state = { inside: 0, peak: 0 };
  const enter = (): void => {
    state.inside += 1;
    state.peak = Math.max(state.peak, state.inside);
  };
  const guarded = <A>(value: A): Effect.Effect<A> =>
    Effect.acquireUseRelease(
      Effect.sync(enter),
      () => Effect.as(Effect.sleep('5 millis'), value),
      () =>
        Effect.sync(() => {
          state.inside -= 1;
        }),
    );
  const repository = {
    runsRoot: '/runs',
    readManifest: () => Effect.succeed({}),
    decodeManifest: () => Effect.die('unused'),
    terminalArchive: (gameId: string) => guarded(archiveOf(gameId)),
    lastReplayTurn: () => Effect.succeedNone,
    diskGamesIndex: () => Effect.die('unused'),
    diskRowsWithInterrupted: () => Effect.die('unused'),
    frameFile: (archive: TerminalArchive) => guarded(`/runs/${archive.gameId}/frame.png`),
    videoFile: (archive: TerminalArchive) => guarded(`/runs/${archive.gameId}/game.mp4`),
  } satisfies RunsRepositoryApi;
  return { repository, peak: () => state.peak };
};

describe('the pg repository decorator adds a log line and no lock', () => {
  test('a burst on one game is not queued by the gateway', async () => {
    // The serialization this used to assert had one subject — the per-game
    // staging directory — and that subject no longer exists.  Twenty-four
    // concurrent reads of one game must all be in flight at once; a peak of
    // `1` here would mean the gateway had grown a queue in front of reads
    // that share nothing.
    const probe = overlapProbe();
    const guarded = withFailureLog(probe.repository);
    await Effect.runPromise(
      Effect.all(
        Array.from({ length: 24 }, () => guarded.terminalArchive('game_parity_terminal_valid_01')),
        { concurrency: 'unbounded' },
      ),
    );
    expect(probe.peak()).toBe(24);
  });

  test('the frame and video reads are not queued behind the archive read', async () => {
    const probe = overlapProbe();
    const gameId = 'game_parity_terminal_valid_01';
    const archive = archiveOf(gameId);
    const guarded = withFailureLog(probe.repository);
    await Effect.runPromise(
      Effect.all(
        [
          guarded.terminalArchive(gameId),
          guarded.frameFile(archive, Option.none()),
          guarded.videoFile(archive),
        ],
        { concurrency: 'unbounded' },
      ),
    );
    expect(probe.peak()).toBe(3);
  });

  test('the failure line names the operation and the cause, and nothing else', () => {
    const line = describeRepositoryFailure('terminalArchive', 'game_x', {
      _tag: 'ArchiveUnavailable',
      problem: 'manifestUnavailable',
    });
    expect(line).toContain('pg-backend: terminalArchive game_x failed');
    expect(line).toContain('ArchiveUnavailable');
    expect(line).not.toContain(DATABASE_URL);
  });
});
