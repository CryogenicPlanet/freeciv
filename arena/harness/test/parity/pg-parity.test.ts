/**
 * **The acceptance oracle for the Postgres backend.**  Three gateways, every
 * upstream, every request, compared *pairwise*.
 *
 * `diff.test.ts` compares two implementations over one filesystem.  This file
 * adds a third process — the **same** TypeScript binary, `--backend postgres`
 * — and compares all three:
 *
 * ```
 * python3 -m agent_eval.replay_gateway   <flags>            (python)
 * bun     src/gateway/main.ts            <flags>            (tsFs)
 * bun     src/gateway/main.ts            <flags> --backend postgres --database-url …   (tsPg)
 * ```
 *
 * ## The three comparisons are not three of the same thing
 *
 * | pair | waivers | what a divergence means |
 * |---|---|---|
 * | `python` ↔ `tsFs` | {@link waiversIn}`('matrix')` | the port drifted — `diff.test.ts`'s claim, re-measured here |
 * | `python` ↔ `tsPg` | **the same list, inherited verbatim** | the pg backend answers CPython differently from the fs port |
 * | `tsFs` ↔ `tsPg` | **none, and none may ever be added** | the *database* laundered a byte |
 *
 * The third row is the whole point and it is the strictest of the three.  Two
 * of these processes are the same binary running the same route code over the
 * same projections; the only thing that differs is where the archive came from.
 * There is therefore no runtime-imposed divergence available to waive — every
 * entry in `waivers.ts` is a property of *Bun's HTTP layer versus CPython's*,
 * and both sides of this pair are Bun.  A byte that differs here is a storage
 * defect: a `jsonb` that sorted keys, a `text` column that re-encoded, an
 * `octet_length` used where the fstat size was the gate, a decoded
 * `VictoryRecord` fed to `relayedJson` instead of the stored bytes.  So this
 * pair is compared with the waiver machinery *switched off* rather than with an
 * empty list, and {@link noNewWaiversPossible} says so as an assertion.
 *
 * ## The falsification test, without which none of the above proves anything
 *
 * A `--backend postgres` gateway that silently fell back to `--runs-root` would
 * score **perfect** parity on every leg below, because it would be the `tsFs`
 * gateway.  And it is no longer an absurd hypothesis: both backends now read
 * their savegames, frames and `victory.json` out of the *same* `--runs-root`,
 * so the only thing left that could come from a database is the pair of JSON
 * documents.  Three-way agreement is therefore not evidence that anything was
 * read out of a database.  {@link PROVENANCE_SCENARIO} is: it ingests the
 * fixture tree, **takes the stored `report.json` away from one game** (its
 * `games.report_status` becomes `absent` and `extras -> 'report'` is dropped),
 * and asserts that the `tsPg` gateway's `/result` turns into CPython's
 * `404 game report not found` while `tsFs`, reading the same untouched disk,
 * still answers `200`.  A gateway reading the filesystem cannot pass that, and
 * a gateway reading the database cannot fail it.
 *
 * ## Postgres safety
 *
 * Absolute, and enforced in code rather than by care:
 *
 * - Every database this file creates is named `arena_wf_<random>`, and
 *   {@link EPHEMERAL_DB_RE} is checked at **both** the create and the drop.
 * - {@link databases}' `drop` refuses any name this process did not create,
 *   whatever it is called — the registry is the authority, the regex is the
 *   second lock.
 * - The pre-existing database list is read at module load
 *   ({@link DATABASES_BEFORE}) and asserted **unchanged** at the end, so a
 *   leaked or wrongly-dropped database is a failing test rather than a note.
 * - Nothing here runs `ALTER`, touches a role, or connects to anything but
 *   `postgres` (to create/drop) and the ephemeral databases themselves.
 *
 * ## When the server is not there
 *
 * The hermetic backend for `@arena/db`'s own tests is PGlite, in-process.  It
 * cannot serve *this* rig: the gateway is a **subprocess** and reaches its
 * database over the wire protocol, which PGlite only speaks through
 * `@electric-sql/pglite-socket` — not installed in this workspace (checked:
 * `node_modules/@electric-sql/` has `pglite` and nothing else).  So the fallback
 * is not a second backend, it is an **announced skip**: {@link POSTGRES_VERDICT}
 * names what did not run, and `ARENA_REQUIRE_PARITY=1` turns it into a failure.
 * A degradation that is stated is a result; a degradation that vanishes into a
 * green run is a lie, which is why the skip is announced and why
 * `ARENA_REQUIRE_PARITY=1` turns it into a failure.
 *
 * @module
 */

import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { existsSync, readFileSync, readdirSync } from 'node:fs';
import { join } from 'node:path';
import {
  aliveProcesses,
  bootGatewayTrio,
  type BySide,
  killAllBooted,
  PARITY_PLATFORM_SUPPORTED,
  PARITY_REQUIRED,
  paritySkipWarning,
  PG_ONLY_FLAGS,
  REPO_ROOT,
  SLOT_SCOPED_FLAGS,
  type TrioArgvParity,
  trioArgvParity,
  type TrioSide,
  TRIO_SIDES,
  type TrioStopReport,
  UNROUTABLE_UPSTREAM_TIMEOUT_S,
  UNROUTABLE_UPSTREAM_URL,
  unwrapTrio,
} from './boot.ts';
import { pipedStreamText } from './json-boundary.ts';
import { REQUEST_CASES, VALID_GAME_ID } from './fixtures/request-cases.ts';
import { PARITY_RUNS_ROOT, PARITY_SCENARIOS, type ParityScenario } from './fixtures/scenarios.ts';
import { compareBodies, describeBodyVerdict } from './normalizers.ts';
import { makeStub, STUB_MODES, type StubHandle, type StubMode } from './stub-supervisor.ts';
import {
  checkWaiver,
  isWaived,
  type ParityWaiver,
  type WaivedAspect,
  waiverExpectsNoResponse,
  waiversIn,
} from './waivers.ts';
import {
  bodyLatin1,
  isWireResponse,
  type WireOutcome,
  type WireRequest,
  wireRequest,
} from './wire-client.ts';

const DARWIN = PARITY_PLATFORM_SUPPORTED;

// ---------------------------------------------------------------------------
// The database, and the rules that bound it
// ---------------------------------------------------------------------------

/** The user's local server.  Never a remote one, never another port. */
const PG_HOST = '127.0.0.1' as const;
const PG_PORT = 5432;

/**
 * The **only** database name shape this file may create or drop.
 *
 * Checked at the create *and* at the drop, and paired with a registry of what
 * this process actually created (see {@link databases}).  Either check alone
 * would be enough to protect the user's `postgres` and `scalar` databases; both
 * are here because the cost is a regex and the failure mode is unrecoverable.
 */
const EPHEMERAL_DB_RE = /^arena_wf_[a-z0-9_]+$/;

/** The prefix the ground rule names, for the "none remain" assertion. */
const EPHEMERAL_DB_PREFIX = 'arena_wf_' as const;

/**
 * Twelve lowercase hex characters — `[a-z0-9]`, which is what
 * {@link EPHEMERAL_DB_RE} accepts and what an unquoted Postgres identifier
 * takes without folding.
 */
const randomSuffix = (): string =>
  Array.from(crypto.getRandomValues(new Uint8Array(6)), (byte) =>
    byte.toString(16).padStart(2, '0'),
  ).join('');

/** A `psql` invocation as a value: exit code, stdout, stderr, nothing thrown. */
interface PsqlResult {
  readonly ok: boolean;
  readonly exitCode: number | null;
  readonly stdout: string;
  readonly stderr: string;
}

const psql = (database: string, sql: string): PsqlResult => {
  const probe = Bun.spawnSync([
    'psql',
    '-h', PG_HOST,
    '-p', String(PG_PORT),
    '-d', database,
    '-v', 'ON_ERROR_STOP=1',
    '-Atc', sql,
  ]);
  return {
    ok: probe.exitCode === 0,
    exitCode: probe.exitCode,
    stdout: probe.stdout.toString(),
    stderr: probe.stderr.toString(),
  };
};

/** `postgres://127.0.0.1:5432/<name>` — credential-free, which `boot.ts` requires. */
const databaseUrlFor = (name: string): string =>
  `postgres://${PG_HOST}:${String(PG_PORT)}/${name}`;

/**
 * Every database on the server, sorted, or `null` when it cannot be asked.
 *
 * `null` rather than `[]`: "the server is down" and "the server has no
 * databases" are different answers and only one of them is a reason to skip.
 */
const listDatabases = (): ReadonlyArray<string> | null => {
  const result = psql('postgres', 'select datname from pg_database order by 1');
  return result.ok
    ? result.stdout.split('\n').filter((line) => line !== '')
    : null;
};

/**
 * The pre-existing databases, read **before** anything is created.
 *
 * The ground rule's "list databases first and record what pre-exists", as a
 * value the final assertion compares against.  Measured on this machine while
 * this file was written: `postgres`, `scalar`, `template0`, `template1`.
 */
const DATABASES_BEFORE: ReadonlyArray<string> | null = listDatabases();

/**
 * `arena_wf_*` databases that existed **before** this run — someone else's.
 *
 * The prefix is the shared convention for "an ephemeral workflow database", so
 * on a machine where another rig is running one of its own, the server is not
 * quiet.  Measured while this file was written: `arena_wf_pgrig_d64cc43f1757`,
 * with a live connection on it.
 *
 * They are recorded rather than refused, and *never* dropped: the drop path
 * consults this process's own registry, so a database it did not create cannot
 * be reached from here whatever it is called.  Recording them is what keeps the
 * end-of-run claim exact — "no `arena_wf_*` remains **that was not already
 * there**", which on a quiet server is the literal "none remain".
 */
const FOREIGN_EPHEMERAL: ReadonlyArray<string> = (DATABASES_BEFORE ?? []).filter((name) =>
  name.startsWith(EPHEMERAL_DB_PREFIX),
);

/**
 * Why this rig can or cannot run, decided once at module load.
 *
 * One reason only: no server.  A busy server is not a reason — see
 * {@link FOREIGN_EPHEMERAL} — and an earlier draft of this file that refused on
 * one was wrong in the ordinary case, blocking the whole oracle because a
 * sibling rig happened to be mid-sweep.
 */
type PostgresVerdict =
  | { readonly _tag: 'Usable'; readonly why: string }
  | { readonly _tag: 'Unreachable'; readonly why: string };

const postgresVerdict = (before: ReadonlyArray<string> | null): PostgresVerdict =>
  before === null
    ? {
        _tag: 'Unreachable',
        why:
          `no PostgreSQL answered at ${PG_HOST}:${String(PG_PORT)}. The hermetic backend ` +
          '(PGlite) cannot stand in here: the gateway is a subprocess and speaks the wire ' +
          'protocol, and @electric-sql/pglite-socket is not installed in this workspace.',
      }
    : {
        _tag: 'Usable',
        why:
          `PostgreSQL at ${PG_HOST}:${String(PG_PORT)}; databases before: ${before.join(', ')}` +
          (FOREIGN_EPHEMERAL.length === 0
            ? ''
            : ` (${String(FOREIGN_EPHEMERAL.length)} of them ${EPHEMERAL_DB_PREFIX}* and NOT this run's: ${FOREIGN_EPHEMERAL.join(', ')})`),
      };

const POSTGRES_VERDICT: PostgresVerdict = postgresVerdict(DATABASES_BEFORE);

/** Everything below runs only when both gates are open. */
const RUNNABLE = DARWIN && POSTGRES_VERDICT._tag === 'Usable';

/**
 * The ephemeral databases this process created, and the only ones it may drop.
 *
 * A closure rather than a module binding, for `boot.ts`'s reason: the set can
 * neither be reassigned nor reached except through its accessors, and a name is
 * registered **before** `createdb` runs, so a create that fails halfway still
 * leaves a droppable record.
 */
interface DatabaseRegistry {
  readonly create: (label: string) => string;
  readonly drop: (name: string) => PsqlResult;
  readonly dropAll: () => ReadonlyArray<string>;
  readonly created: () => ReadonlyArray<string>;
}

const databases = ((): DatabaseRegistry => {
  const created: string[] = [];
  const dropped = new Set<string>();
  return {
    create: (label) => {
      const name = `${EPHEMERAL_DB_PREFIX}${label}_${randomSuffix()}`;
      if (!EPHEMERAL_DB_RE.test(name)) {
        throw new Error(`refusing to create a database outside ${String(EPHEMERAL_DB_RE)}: ${name}`);
      }
      created.push(name);
      const result = psql('postgres', `create database ${name}`);
      if (!result.ok) throw new Error(`could not create ${name}: ${result.stderr.trim()}`);
      return name;
    },
    drop: (name) => {
      // Two locks, and the registry is the load-bearing one: a name this
      // process did not create is never dropped, whatever it is called.
      if (!EPHEMERAL_DB_RE.test(name) || !created.includes(name)) {
        return {
          ok: false,
          exitCode: null,
          stdout: '',
          stderr: `refusing to drop ${name}: not created by this run`,
        };
      }
      const result = psql('postgres', `drop database if exists ${name} with (force)`);
      if (result.ok) dropped.add(name);
      return result;
    },
    dropAll: () =>
      created.flatMap((name) => {
        if (dropped.has(name)) return [];
        const result = psql('postgres', `drop database if exists ${name} with (force)`);
        if (result.ok) dropped.add(name);
        return result.ok ? [name] : [];
      }),
    created: () => [...created],
  };
})();

// ---------------------------------------------------------------------------
// Ingest
// ---------------------------------------------------------------------------

/** `@arena/db`'s writer.  The gateway never writes; this is the only ingester. */
const INGEST_CLI: string = join(REPO_ROOT, 'arena/db/src/ingest-cli.ts');

/** What one sweep reported, as a value a test can assert on. */
interface IngestOutcome {
  readonly database: string;
  readonly exitCode: number | null;
  readonly stdout: string;
  readonly stderr: string;
}

/**
 * `--skip-boards`, and why the acceptance oracle is entitled to it.
 *
 * `board_state` is domain data — it exists so the database is self-sufficient
 * for scrubbing and frame regeneration later — and it is **not on any response
 * path**: `/v1/games/{id}/board.json` derives from the savegames on every
 * request, in every backend, through `python3 -m
 * agent_eval.replay_derive_cli --op board` reading `--runs-root`.  Ingesting
 * boards would spawn one python process per turn per scenario and change not
 * one byte this file compares.
 *
 * Skipping them is therefore not a shortcut around the oracle, it *is* an
 * assertion: the `board-turn-1` and `board-*` legs below are compared
 * three-way with an empty `board_state`, so a pg gateway that had quietly
 * started answering `board.json` out of the database would fail them.
 */
const SKIP_BOARDS_FLAG = '--skip-boards' as const;

/**
 * Migrate and ingest `runs_root` into a fresh database.
 *
 * The CLI applies the committed `arena/db/drizzle/` migrations by default, so
 * one call takes an empty database to a populated one — and it is the *same*
 * command an operator runs, which is the point of shelling out to it rather
 * than importing `Ingest.ingest` and building a layer stack here.
 */
const ingestFixtures = async (database: string): Promise<IngestOutcome> => {
  const child = Bun.spawn(
    [
      'bun',
      INGEST_CLI,
      '--runs-root',
      PARITY_RUNS_ROOT,
      '--database-url',
      databaseUrlFor(database),
      SKIP_BOARDS_FLAG,
    ],
    { cwd: REPO_ROOT, stdout: 'pipe', stderr: 'pipe' },
  );
  const [stdout, stderr] = await Promise.all([
    pipedStreamText(child.stdout),
    pipedStreamText(child.stderr),
  ]);
  await child.exited;
  return { database, exitCode: child.exitCode, stdout, stderr };
};

// ---------------------------------------------------------------------------
// Legs
// ---------------------------------------------------------------------------

/** One request, replayed against all three gateways in every scenario. */
interface MatrixLeg {
  readonly name: string;
  readonly method: string;
  readonly target: string;
  readonly why: string;
  readonly headers?: ReadonlyArray<readonly [string, string]>;
  readonly body?: string;
  readonly rawRequestLine?: string;
  readonly bodyRule: 'bytes' | 'health';
}

const get = (
  name: string,
  target: string,
  why: string,
  extra: Partial<MatrixLeg> = {},
): MatrixLeg => ({ name, method: 'GET', target, why, bodyRule: 'bytes', ...extra });

const fixtureId = (scenario: ParityScenario['scenario']): string => {
  const found = PARITY_SCENARIOS.find((candidate) => candidate.scenario === scenario);
  if (found === undefined) throw new Error(`no fixture for the ${scenario} class`);
  return found.gameId;
};

const TORN_TAIL_GAME_ID = fixtureId('torn-tail');

/**
 * `diff.test.ts`'s `ROUTE_LEGS`, transcribed.
 *
 * A copy, and copies drift — so it is **policed** rather than trusted:
 * {@link routeLegNamesInDiffTest} reads the other file's source text and
 * {@link ROUTE_LEG_DRIFT} fails this suite the moment the two tables stop
 * naming the same set.  Importing the table instead is not available: it is
 * module-private to a `.test.ts`, and importing that file would register its
 * 700-odd tests inside this one's run.
 */
const ROUTE_LEGS: ReadonlyArray<MatrixLeg> = [
  get('health', '/health', 'the only route that never proxies; the five volatile fields live here', {
    bodyRule: 'health',
  }),
  get('health-query-400', '/health?x=1', 'health takes no query at all, not even an unknown one'),
  get('bare-id', `/v1/games/${VALID_GAME_ID}`, 'the bare-id alias of /status, which proxies without the suffix'),
  get('result', `/v1/games/${VALID_GAME_ID}/result`, 'the report.json projection — on pg, `games.extras -> report`, reconstructed'),
  get('watch-json', `/v1/games/${VALID_GAME_ID}/watch.json`, 'the widest archive projection: frames, players and colors in one document'),
  get('status-trailing-slash', `/v1/games/${VALID_GAME_ID}/status/`, 'path.strip("/") routes it, and the archive family forwards the slash verbatim'),
  get('replay-json', `/v1/games/${VALID_GAME_ID}/replay.json`, 'the derivation route with no query: every default in one body'),
  get('replay-after-turn', `/v1/games/${VALID_GAME_ID}/replay.json?after_turn=1`, 'pagination from a turn, and the integer spelling of next_after_turn'),
  get('replay-limit-251', `/v1/games/${VALID_GAME_ID}/replay.json?limit=251`, 'one past the documented ceiling: the 400 is the bound, spelled exactly'),
  get(
    'replay-after-turn-2-53-plus-1',
    `/v1/games/${VALID_GAME_ID}/replay.json?after_turn=9007199254740993`,
    'a turn no double spells, echoed back as next_after_turn',
  ),
  get('replay-cold', `/v1/games/${VALID_GAME_ID}/replay.json?limit=5`, 'the first request after freshCaches: pays for the derivation subprocess, identically on all three'),
  get('replay-warm', `/v1/games/${VALID_GAME_ID}/replay.json?limit=5`, 'the same request again: reads what the cold leg wrote, and must be byte-identical to it'),
  get('board-turn-1', `/v1/games/${VALID_GAME_ID}/board.json?turn=1`, 'a real autosave parsed into a board — on pg too, straight off --runs-root: board_state is never on a response path'),
  get('board-missing-turn', `/v1/games/${VALID_GAME_ID}/board.json`, 'turn is required; absent is as wrong as duplicated'),
  get('events-json', `/v1/games/${VALID_GAME_ID}/events.json`, 'the third derivation, whose numeric fields are rebuilt as bigint'),
  get('events-query-400', `/v1/games/${VALID_GAME_ID}/events.json?turn=1`, 'events reject any query at all, from the handler rather than the router'),
  get('video-mp4', `/v1/games/${VALID_GAME_ID}/video.mp4`, 'no game.mp4 on disk and no run_videos row: the binary family 404 with its own message'),
  get('torn-tail-status', `/v1/games/${TORN_TAIL_GAME_ID}/status`, 'the torn-tail run on its own route, not as an index row'),
  get('torn-tail-watch', `/v1/games/${TORN_TAIL_GAME_ID}/watch.json`, 'the widest projection of a run whose replay tail is unparseable'),
  get('torn-tail-frames', `/v1/games/${TORN_TAIL_GAME_ID}/frames`, 'the frame listing of a non-terminal run, from the same torn archive'),
  get('root-path', '/', 'the shortest unroutable path there is'),
  get('unknown-path', '/nope', 'an unroutable path that is not a prefix of any route'),
  get('unknown-deep-path', '/v1/nope/x/y', 'unroutable but inside the versioned prefix'),
  get('private-route', `/v1/games/${VALID_GAME_ID}/join`, 'a real supervisor route the gateway must not expose'),
  get('nonsense-suffix-query-400', `/v1/games/${VALID_GAME_ID}/nonsense?x=1`, 'the query gate precedes the final 404'),
  get('absolute-form-target', 'http://evil.invalid/health', "urlsplit ignores the netloc, so this is /health", {
    bodyRule: 'health',
  }),
  { name: 'post-405', method: 'POST', target: `/v1/games/${VALID_GAME_ID}/status`, why: 'every non-GET verb is 405 with Allow: GET', bodyRule: 'bytes' },
  { name: 'put-405', method: 'PUT', target: '/health', why: '405 outranks even the routes served locally', bodyRule: 'bytes' },
  { name: 'delete-405', method: 'DELETE', target: `/v1/games/${VALID_GAME_ID}`, why: 'the 405 is built without _problem and must still be byte-identical', bodyRule: 'bytes' },
  { name: 'options-405', method: 'OPTIONS', target: '/health', why: 'no CORS preflight handling exists, and none may appear', bodyRule: 'bytes' },
  { name: 'head-health', method: 'HEAD', target: '/health', why: 'do_HEAD is _method_not_allowed; the headers, Content-Length included, must match', bodyRule: 'bytes' },
  {
    name: 'head-health-body-visible',
    method: 'GET',
    rawRequestLine: 'HEAD /health HTTP/1.1',
    target: '/health',
    why: 'the same HEAD, read with Content-Length framing, so the adapter-stripped body is observable',
    bodyRule: 'bytes',
  },
  { name: 'trace-501', method: 'TRACE', target: '/health', why: 'an unmapped-but-known verb: the stdlib 501 page, and the reason phrase waiver', bodyRule: 'bytes' },
  { name: 'invented-verb', method: 'FROB', target: '/health', why: 'a verb no parser knows: CPython answers 501, Bun closes the connection', bodyRule: 'bytes' },
  {
    name: 'get-with-body',
    method: 'GET',
    target: `/v1/games/${VALID_GAME_ID}/status`,
    why: 'a well-framed GET body does reach the handler, and is refused there',
    headers: [['Content-Length', '12']],
    body: 'private body',
    bodyRule: 'bytes',
  },
  {
    name: 'get-content-length-zero',
    method: 'GET',
    target: '/health',
    why: 'Content-Length: 0 is not a body: the route is served',
    headers: [['Content-Length', '0']],
    bodyRule: 'health',
  },
  {
    name: 'transfer-encoding-chunked',
    method: 'GET',
    target: '/health',
    why: 'a chunked GET does reach the handler on both sides — the control row for the identity waiver',
    headers: [['Transfer-Encoding', 'chunked']],
    body: '0\r\n\r\n',
    bodyRule: 'bytes',
  },
  {
    name: 'bad-content-length',
    method: 'GET',
    target: '/health',
    why: "WAIVED: Bun's parser answers an unreadable Content-Length itself",
    headers: [['Content-Length', 'abc']],
    bodyRule: 'bytes',
  },
  {
    name: 'transfer-encoding-identity',
    method: 'GET',
    target: '/health',
    why: 'WAIVED: a legal TE value CPython calls a body and Bun calls bad framing',
    headers: [['Transfer-Encoding', 'identity']],
    bodyRule: 'bytes',
  },
  {
    name: 'duplicate-content-length',
    method: 'GET',
    target: '/health',
    why: 'WAIVED: CPython reads the first occurrence and serves; Bun refuses the framing',
    headers: [
      ['Content-Length', '0'],
      ['Content-Length', '0'],
    ],
    bodyRule: 'health',
  },
  {
    name: 'space-in-request-target',
    method: 'GET',
    target: '/v1/games?a= b',
    rawRequestLine: 'GET /v1/games?a= b HTTP/1.1',
    why: 'WAIVED: four tokens on the request line, and the two parsers disagree about which one is the version',
    bodyRule: 'bytes',
  },
];

/** `request-cases.ts`, as legs.  Imported, so this half can never drift. */
const CASE_LEGS: ReadonlyArray<MatrixLeg> = REQUEST_CASES.map(
  (kase): MatrixLeg => ({
    name: kase.name,
    method: 'GET',
    target: kase.target,
    why: kase.why,
    bodyRule: 'bytes',
  }),
);

const MATRIX_LEGS: ReadonlyArray<MatrixLeg> = [...ROUTE_LEGS, ...CASE_LEGS];

/** The two legs whose *order* is their meaning. */
const SEQUENTIAL_LEG_NAMES: ReadonlyArray<string> = ['replay-cold', 'replay-warm'];

const legByName = (name: string): MatrixLeg => {
  const leg = MATRIX_LEGS.find((candidate) => candidate.name === name);
  if (leg === undefined) throw new Error(`no leg named ${name}`);
  return leg;
};

// ---------------------------------------------------------------------------
// The anti-drift guard on the transcribed table
// ---------------------------------------------------------------------------

/** `diff.test.ts`, as text.  Read, never imported — see {@link ROUTE_LEGS}. */
const DIFF_TEST_SOURCE: string = join(import.meta.dir, 'diff.test.ts');

/**
 * The leg names inside `diff.test.ts`'s `ROUTE_LEGS` array literal.
 *
 * Textual on purpose.  The question is not "what does that module export"
 * (nothing) but "does the other rig's route table still name exactly the legs
 * this one replays", and the array literal's own source is the only place that
 * answer lives.  A parse failure returns `[]`, which fails the comparison
 * loudly rather than passing vacuously.
 */
const routeLegNamesInDiffTest = (source: string): ReadonlyArray<string> => {
  const opened = source.indexOf('const ROUTE_LEGS: ReadonlyArray<MatrixLeg> = [');
  if (opened < 0) return [];
  const closed = source.indexOf('\n];', opened);
  if (closed < 0) return [];
  const block = source.slice(opened, closed);
  return [
    ...Array.from(block.matchAll(/\bget\(\s*'([^']+)'/g), (match) => match[1] ?? ''),
    ...Array.from(block.matchAll(/\bname:\s*'([^']+)'/g), (match) => match[1] ?? ''),
  ].toSorted();
};

/** The two tables' route-leg names, for the assertion that they agree. */
interface RouteLegDrift {
  readonly mine: ReadonlyArray<string>;
  readonly theirs: ReadonlyArray<string>;
}

const ROUTE_LEG_DRIFT = {
  mine: ROUTE_LEGS.map((leg) => leg.name).toSorted(),
  theirs: routeLegNamesInDiffTest(
    existsSync(DIFF_TEST_SOURCE) ? readFileSync(DIFF_TEST_SOURCE, 'utf8') : '',
  ),
} satisfies RouteLegDrift;

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

/** One upstream fixture, and the trio pointed at it. */
interface ScenarioSpec {
  readonly name: string;
  readonly serviceUrl: string;
  readonly why: string;
}

const stubScenarioName = (mode: StubMode): string => `stub-${mode}`;

const UPSTREAM_DOWN: ScenarioSpec = {
  name: 'upstream-down',
  serviceUrl: UNROUTABLE_UPSTREAM_URL,
  why: 'connect never completes: the whole disk-fallback matrix, which on the pg side is the whole *database* matrix',
};

/**
 * The scenario whose database is deliberately damaged — the falsification test.
 *
 * Not part of the comparison matrix: its `tsPg` gateway is *supposed* to
 * disagree, which is the one thing every other scenario forbids.
 */
const PROVENANCE_SCENARIO: ScenarioSpec = {
  name: 'pg-provenance',
  serviceUrl: UNROUTABLE_UPSTREAM_URL,
  why: 'one row deleted from the database: proof that the pg gateway is reading it and not --runs-root',
};

const MATRIX_VIEWER_PUBLIC_URL = 'http://viewer.parity.invalid';
const MATRIX_UPSTREAM_TIMEOUT_S = UNROUTABLE_UPSTREAM_TIMEOUT_S;
const LEG_TIMEOUT_MS = 25_000;
const LEG_CONCURRENCY = 10;
/** Each scenario is three processes, a stub and a database; two at a time. */
const SCENARIO_CONCURRENCY = 2;
const MATRIX_TIMEOUT_MS = 1_500_000;

/** The header subset parity is asserted on — `diff.test.ts`'s set, unchanged. */
const COMPARED_HEADERS = [
  'content-type',
  'cache-control',
  'allow',
  'x-content-type-options',
  'referrer-policy',
  'etag',
  'last-modified',
  'content-length',
] as const;

/** `content-length` is dropped for `/health`, whose body carries two pids. */
const comparedHeaders = (leg: MatrixLeg): ReadonlyArray<string> =>
  leg.bodyRule === 'health'
    ? COMPARED_HEADERS.filter((name) => name !== 'content-length')
    : COMPARED_HEADERS;

// ---------------------------------------------------------------------------
// Running one scenario
// ---------------------------------------------------------------------------

type Sided = BySide<WireOutcome>;

/** Everything one scenario produced. */
interface ScenarioReport {
  readonly name: string;
  readonly serviceUrl: string;
  readonly database: string;
  readonly ingest: IngestOutcome;
  readonly outcomes: ReadonlyMap<string, Sided>;
  readonly argv: TrioArgvParity;
  readonly stop: TrioStopReport;
  readonly pids: ReadonlyArray<number>;
  /**
   * The derivation caches the three gateways filled, by side.
   *
   * The replacement for the old `materialized` flag.  That flag asked "did the
   * pg gateway stage savegames into `<cache-root>-saves`", which was the only
   * observable proof that a *derived* answer on the pg leg had come from real
   * files rather than from an archive so empty every side said
   * `available: false`.  There is no staging directory any more, so the same
   * question is asked of the thing that still exists: the python loaders'
   * cache, which all three gateways fill, at their own `--cache-root`, only
   * when a derivation actually ran.
   */
  readonly derivationCaches: BySide<boolean>;
  readonly contentLengthSelfConsistent: boolean;
}

/** Bounded-concurrency `map`, output order = input order. */
const mapPool = async <A, B>(
  items: ReadonlyArray<A>,
  limit: number,
  run: (item: A) => Promise<B>,
): Promise<ReadonlyArray<B>> => {
  const cursor = { next: 0 };
  const worker = async (
    taken: ReadonlyArray<readonly [number, B]>,
  ): Promise<ReadonlyArray<readonly [number, B]>> => {
    const at = cursor.next;
    cursor.next = at + 1;
    const item = items[at];
    if (item === undefined) return taken;
    return worker([...taken, [at, await run(item)] as const]);
  };
  const batches = await Promise.all(
    Array.from({ length: Math.min(limit, items.length) }, () => worker([])),
  );
  return batches
    .flat()
    .toSorted(([left], [right]) => left - right)
    .map(([, value]) => value);
};

const runLeg = (origin: string, leg: MatrixLeg): Promise<WireOutcome> => {
  const base: WireRequest = {
    method: leg.method,
    target: leg.target,
    timeoutMs: LEG_TIMEOUT_MS,
  };
  const withHeaders: WireRequest =
    leg.headers === undefined ? base : { ...base, headers: leg.headers };
  const withBody: WireRequest =
    leg.body === undefined ? withHeaders : { ...withHeaders, body: leg.body };
  const request: WireRequest =
    leg.rawRequestLine === undefined
      ? withBody
      : { ...withBody, rawRequestLine: leg.rawRequestLine };
  return wireRequest(origin, request);
};

/** All three sides of one leg, concurrently — they are three processes. */
const runAll = async (
  origins: BySide<string>,
  leg: MatrixLeg,
): Promise<readonly [string, Sided]> => {
  const [python, tsFs, tsPg] = await Promise.all([
    runLeg(origins.python, leg),
    runLeg(origins.tsFs, leg),
    runLeg(origins.tsPg, leg),
  ]);
  return [leg.name, { python, tsFs, tsPg }] as const;
};

const contentLengthMatchesBody = (outcome: WireOutcome): boolean => {
  if (!isWireResponse(outcome)) return true;
  const declared = outcome.headers.get('content-length');
  if (declared === null || !/^[0-9]+$/.test(declared)) return true;
  return outcome.completedBy === 'head-request' || Number(declared) === outcome.bodyBytes.byteLength;
};

/**
 * Did a derivation actually run on this side?
 *
 * `--cache-root` is created by every gateway before it binds (`:2092`), so its
 * *existence* says nothing; the python loaders create `<cache-root>/<game-id>`
 * and write parsed savegames into it only when a derivation really happened.
 * Read as "non-empty" rather than by rebuilding that path, so the rig cannot
 * drift from `save_replay._cache_directory`'s layout.
 *
 * Asked before teardown removes the scratch.
 */
const derivationRan = (cacheRoot: string): boolean => {
  const entries = readdirSync(cacheRoot, { withFileTypes: true });
  return entries.length > 0;
};

/**
 * Create a database, ingest the fixture tree into it, boot the trio, replay
 * every leg, stop, and drop the database.
 *
 * The database is per scenario rather than shared, and that is not tidiness: a
 * scenario's database is state it mutates (it is created, migrated, ingested
 * and dropped inside `runScenario`), and the falsification scenario below
 * *edits rows*.  Sharing one would make the nine scenarios — which run
 * concurrently, `SCENARIO_CONCURRENCY` at a time — able to observe each other
 * through storage, which is a coupling the filesystem legs do not have and a
 * parity rig therefore must not have either.
 */
const runScenario = async (spec: ScenarioSpec): Promise<ScenarioReport> => {
  const database = databases.create(spec.name.replaceAll('-', '_'));
  const ingest = await ingestFixtures(database);
  if (ingest.exitCode !== 0) {
    databases.drop(database);
    throw new Error(
      `arena-ingest failed for ${spec.name} (exit ${String(ingest.exitCode)}):\n${ingest.stderr.trim()}`,
    );
  }
  const trio = unwrapTrio(
    await bootGatewayTrio({
      runsRoot: PARITY_RUNS_ROOT,
      serviceUrl: spec.serviceUrl,
      scenario: spec.name,
      upstreamTimeoutSeconds: MATRIX_UPSTREAM_TIMEOUT_S,
      viewerPublicUrl: MATRIX_VIEWER_PUBLIC_URL,
      databaseUrl: databaseUrlFor(database),
    }),
  );
  const origins: BySide<string> = {
    python: trio.python.origin,
    tsFs: trio.tsFs.origin,
    tsPg: trio.tsPg.origin,
  };
  const argv = trioArgvParity(trio);
  const pids = trio.all.map((gateway) => gateway.pid);

  // Cold first, and strictly in order.  `freshCaches()` removes all three
  // derivation caches, which is now the whole of what a side can be warm from:
  // the loaders' cache is the only thing any gateway carries between requests,
  // and it lives on disk on every side.  The cold leg is cold on every side by
  // construction rather than by hope.
  await trio.freshCaches();
  const sequential = await SEQUENTIAL_LEG_NAMES.reduce<Promise<ReadonlyArray<readonly [string, Sided]>>>(
    async (previous, name) => [...(await previous), await runAll(origins, legByName(name))],
    Promise.resolve([]),
  );

  const pooled = await mapPool(
    MATRIX_LEGS.filter((leg) => !SEQUENTIAL_LEG_NAMES.includes(leg.name)),
    LEG_CONCURRENCY,
    (leg) => runAll(origins, leg),
  );

  const outcomes = new Map([...sequential, ...pooled]);
  const derivationCaches: BySide<boolean> = {
    python: derivationRan(trio.python.cacheRoot),
    tsFs: derivationRan(trio.tsFs.cacheRoot),
    tsPg: derivationRan(trio.tsPg.cacheRoot),
  };
  const stop = await trio.stop();
  trio.cleanup();
  databases.drop(database);

  return {
    name: spec.name,
    serviceUrl: spec.serviceUrl,
    database,
    ingest,
    outcomes,
    argv,
    stop,
    pids,
    derivationCaches,
    contentLengthSelfConsistent: Array.from(outcomes.values()).every((sides) =>
      TRIO_SIDES.every((side) => contentLengthMatchesBody(sides[side])),
    ),
  };
};

// ---------------------------------------------------------------------------
// The three comparisons
// ---------------------------------------------------------------------------

/** One pairwise comparison of the trio, and whether waivers apply to it. */
interface TrioPair {
  readonly name: string;
  readonly left: TrioSide;
  readonly right: TrioSide;
  /**
   * Whether `waivers.ts` may exempt anything on this pair.
   *
   * True exactly when one side is CPython.  Every entry in that list is a
   * divergence between **CPython's** HTTP layer and **Bun's**; a pair whose two
   * sides are both Bun has no such divergence available to it, so switching the
   * machinery off is a stronger statement than an empty list — see
   * {@link noNewWaiversPossible}.
   */
  readonly waived: boolean;
  readonly why: string;
}

const TRIO_PAIRS: ReadonlyArray<TrioPair> = [
  {
    name: 'python-vs-tsFs',
    left: 'python',
    right: 'tsFs',
    waived: true,
    why: "diff.test.ts's claim, re-measured here so a pg run that agrees with a *drifted* fs port cannot look green",
  },
  {
    name: 'python-vs-tsPg',
    left: 'python',
    right: 'tsPg',
    waived: true,
    why: 'the acceptance claim: the pg backend answers CPython exactly as the fs backend does, under the same waivers and no others',
  },
  {
    name: 'tsFs-vs-tsPg',
    left: 'tsFs',
    right: 'tsPg',
    waived: false,
    why: 'the storage claim: same binary, same routes, different store — every byte must match, with no exemption of any kind',
  },
];

/** One aspect on which two sides disagreed. */
interface Divergence {
  readonly aspect: WaivedAspect | 'outcome';
  readonly detail: string;
  readonly left: string;
  readonly right: string;
}

const preview = (text: string): string =>
  text.length <= 160 ? text : `${text.slice(0, 160)}… (${String(text.length)}B)`;

const headerMap = (
  outcome: WireOutcome,
  names: ReadonlyArray<string>,
): Readonly<Record<string, string | null>> =>
  Object.fromEntries(
    names.map((name) => [name, isWireResponse(outcome) ? outcome.headers.get(name) : null]),
  );

/**
 * Every way two outcomes disagree, as values.
 *
 * `diff.test.ts#divergences` with the two sides made parameters rather than
 * named `python`/`typescript`.  `compareBodies` is reused unchanged: its
 * parameter names say python/typescript but its rule is left/right, and for the
 * `health` rule that is exactly right for all three pairs — `pid`, `port`,
 * `url`, `cache_root` and `identity` differ between *any* two of these
 * processes, so the dead-normalization alarm stays armed on every pair.
 */
const divergences = (leg: MatrixLeg, left: WireOutcome, right: WireOutcome): ReadonlyArray<Divergence> => {
  if (!isWireResponse(left) || !isWireResponse(right)) {
    return left._tag === right._tag
      ? []
      : [
          {
            aspect: 'outcome',
            detail: 'one side produced a response and the other did not',
            left: left._tag,
            right: right._tag,
          },
        ];
  }
  const names = comparedHeaders(leg);
  const leftHeaders = headerMap(left, names);
  const rightHeaders = headerMap(right, names);
  const bodyVerdict = compareBodies(leg.bodyRule, left, right);
  return [
    ...(left.status === right.status
      ? []
      : [{ aspect: 'status' as const, detail: 'status', left: String(left.status), right: String(right.status) }]),
    ...(left.reasonLine === right.reasonLine
      ? []
      : [{ aspect: 'reason' as const, detail: 'reason phrase', left: left.reasonLine, right: right.reasonLine }]),
    ...names.flatMap((name): ReadonlyArray<Divergence> =>
      leftHeaders[name] === rightHeaders[name]
        ? []
        : [
            {
              aspect: 'headers',
              detail: name,
              left: String(leftHeaders[name]),
              right: String(rightHeaders[name]),
            },
          ],
    ),
    ...(bodyVerdict._tag === 'Equal'
      ? []
      : [
          {
            aspect: 'body' as const,
            detail: describeBodyVerdict(bodyVerdict),
            left: bodyVerdict._tag === 'Differ' ? preview(bodyVerdict.python) : bodyVerdict._tag,
            right: bodyVerdict._tag === 'Differ' ? preview(bodyVerdict.typescript) : bodyVerdict._tag,
          },
        ]),
  ];
};

/**
 * The divergences a waiver does not excuse, for one pair.
 *
 * The `tsFs`↔`tsPg` pair never consults the list at all — not "consults an
 * empty list", *never consults it* — which is what makes "no new waivers
 * allowed" a property of the code rather than a promise in a comment.
 */
const unwaived = (
  pair: TrioPair,
  scenario: string,
  leg: MatrixLeg,
  found: ReadonlyArray<Divergence>,
): ReadonlyArray<Divergence> =>
  pair.waived
    ? found.filter(
        (one) => !isWaived(scenario, leg.name, one.aspect === 'outcome' ? 'status' : one.aspect, one.detail),
      )
    : found;

/** A status, or the failure tag when there was no response at all. */
const statusOf = (outcome: WireOutcome): number | string =>
  isWireResponse(outcome) ? outcome.status : outcome._tag;

/** The body as a short latin-1 preview, or the failure tag. */
const bodyOf = (outcome: WireOutcome): string =>
  isWireResponse(outcome) ? preview(bodyLatin1(outcome)) : outcome._tag;

/** Every unexcused divergence on every pair of one leg, as report lines. */
const legOffenders = (scenario: string, leg: MatrixLeg, sides: Sided): ReadonlyArray<string> =>
  TRIO_PAIRS.flatMap((pair) =>
    unwaived(pair, scenario, leg, divergences(leg, sides[pair.left], sides[pair.right])).map(
      (one) => `${pair.name} ${leg.name} [${one.aspect}/${one.detail}] ${pair.left}=${one.left} ${pair.right}=${one.right}`,
    ),
  );

// ---------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------

interface MatrixState {
  reports: ReadonlyMap<string, ScenarioReport>;
  stubs: ReadonlyArray<StubHandle>;
  provenance: ProvenanceReport | null;
  databasesAfter: ReadonlyArray<string> | null;
  dropFailures: ReadonlyArray<string>;
}

/** What the falsification scenario measured. */
interface ProvenanceReport {
  readonly database: string;
  readonly revoked: string;
  /** `/v1/games/<id>/result` on each side, after the stored report was revoked. */
  readonly resultStatus: BySide<number | string>;
  readonly resultBody: BySide<string>;
  readonly stop: TrioStopReport;
  readonly pids: ReadonlyArray<number>;
}

const state: MatrixState = {
  reports: new Map(),
  stubs: [],
  provenance: null,
  databasesAfter: null,
  dropFailures: [],
};

const scenarioReport = (name: string): ScenarioReport => {
  const report = state.reports.get(name);
  if (report === undefined) throw new Error(`scenario ${name} did not run`);
  return report;
};

const outcomesFor = (scenario: string, leg: string): Sided => {
  const sides = scenarioReport(scenario).outcomes.get(leg);
  if (sides === undefined) throw new Error(`leg ${leg} did not run in scenario ${scenario}`);
  return sides;
};

/** The evidence table: unexcused divergences only, plus per-scenario counts. */
const renderReport = (reports: ReadonlyMap<string, ScenarioReport>): ReadonlyArray<string> =>
  Array.from(reports.values()).flatMap((report): ReadonlyArray<string> => {
    const rows = MATRIX_LEGS.flatMap((leg): ReadonlyArray<string> => {
      const sides = report.outcomes.get(leg.name);
      return sides === undefined
        ? [`  ${leg.name}: DID NOT RUN`]
        : legOffenders(report.name, leg, sides).map((line) => `  DIFFER ${line}`);
    });
    return [
      `${report.name}  (${report.serviceUrl}, db=${report.database}, ${String(report.outcomes.size)} legs x 3 pairs)`,
      ...rows,
      `  unexcused divergences: ${String(rows.length)}   derivation caches filled: ${JSON.stringify(report.derivationCaches)}`,
    ];
  });

/**
 * Every `(leg, side)` that produced no response at all.
 *
 * Three-sided, for `diff.test.ts`'s reason: the comparison scores "neither side
 * answered, same tag" as parity, which is safe only because something else
 * insists the leg answered.
 */
const missingResponses = (report: ScenarioReport): ReadonlyArray<string> =>
  MATRIX_LEGS.flatMap((leg): ReadonlyArray<string> => {
    const sides = report.outcomes.get(leg.name);
    if (sides === undefined) return [`${leg.name}/did-not-run`];
    return TRIO_SIDES.flatMap((side) => (isWireResponse(sides[side]) ? [] : [`${leg.name}/${side}`]));
  }).toSorted();

const MATRIX_WAIVERS: ReadonlyArray<ParityWaiver> = waiversIn('matrix');

/**
 * The silences the rig accepts, derived from `waivers.ts` rather than restated.
 *
 * A waiver whose measured `typescript` signature is an outcome tag names a leg
 * Bun answers with silence — and `tsPg` **is** Bun, so it is allowed exactly the
 * same silences and no others.  That single line is the whole of "the ts-pg leg
 * inherits ts-fs's waivers", applied to the answered-at-all rule.
 */
const WAIVED_SILENCES: ReadonlyArray<string> = MATRIX_WAIVERS.flatMap((waiver) => [
  ...(waiverExpectsNoResponse(waiver, 'python') ? waiver.legs.map((leg) => `${leg}/python`) : []),
  ...(waiverExpectsNoResponse(waiver, 'typescript')
    ? waiver.legs.flatMap((leg) => [`${leg}/tsFs`, `${leg}/tsPg`])
    : []),
]).toSorted();

const SCENARIO_NAMES: ReadonlyArray<string> = [
  UPSTREAM_DOWN.name,
  ...STUB_MODES.map(stubScenarioName),
];

/** Stub personalities whose archive responses fall back to local storage. */
const DISK_FALLBACK_MODES = [
  'not-found-404',
  'method-405',
  'portless',
  'hang',
] as const satisfies ReadonlyArray<StubMode>;

/** Scenarios in which derivation must fill all three private caches. */
const DISK_FALLBACK_SCENARIOS: ReadonlyArray<string> = [
  UPSTREAM_DOWN.name,
  ...DISK_FALLBACK_MODES.map(stubScenarioName),
];

/**
 * The one edit that makes a pg gateway answer differently from an fs one.
 *
 * v2 stores no document bytes, so there is no row to `DELETE` any more: a
 * game's `report.json` lives as `games.report_status = 'ok'` plus the verbatim
 * document under `extras -> 'report'`.  Taking it away is therefore the two of
 * them together, and it is written as the state ingest would have written for a
 * run whose `report.json` could not be opened — `absent`, which the read path
 * turns into 404 `game report not found`.
 *
 * Both halves matter.  Flipping the status alone would leave the document
 * sitting in `extras` for a read path that happened to trust the wrong one of
 * the two; dropping `extras -> 'report'` alone would leave a `status = 'ok'`
 * row with nothing behind it, which is a state ingest cannot produce and so
 * proves nothing about the code paths that exist.
 */
const REVOKE_STORED_REPORT = (gameId: string): string =>
  `update games set report_status = 'absent', report_byte_size = null, ` +
  `extras = extras - 'report' where game_id = '${gameId}'`;

/**
 * The falsification scenario, run apart from the matrix.
 *
 * Ingest, then take the stored `report.json` away from the one terminal
 * fixture, then ask all three.  CPython and `tsFs` read the untouched fixture
 * tree and must still answer `200`; `tsPg` has nothing to read and must answer
 * the `404` CPython itself would give for an absent `report.json`.
 */
const runProvenance = async (): Promise<ProvenanceReport> => {
  const database = databases.create('provenance');
  const ingest = await ingestFixtures(database);
  if (ingest.exitCode !== 0) {
    databases.drop(database);
    throw new Error(`arena-ingest failed for the provenance scenario:\n${ingest.stderr.trim()}`);
  }
  const deleted = psql(database, REVOKE_STORED_REPORT(VALID_GAME_ID));
  if (!deleted.ok) {
    databases.drop(database);
    throw new Error(`could not revoke the stored report: ${deleted.stderr.trim()}`);
  }
  const trio = unwrapTrio(
    await bootGatewayTrio({
      runsRoot: PARITY_RUNS_ROOT,
      serviceUrl: PROVENANCE_SCENARIO.serviceUrl,
      scenario: PROVENANCE_SCENARIO.name,
      upstreamTimeoutSeconds: MATRIX_UPSTREAM_TIMEOUT_S,
      viewerPublicUrl: MATRIX_VIEWER_PUBLIC_URL,
      databaseUrl: databaseUrlFor(database),
    }),
  );
  const leg = legByName('result');
  const [, sides] = await runAll(
    { python: trio.python.origin, tsFs: trio.tsFs.origin, tsPg: trio.tsPg.origin },
    leg,
  );
  const pids = trio.all.map((gateway) => gateway.pid);
  const stop = await trio.stop();
  trio.cleanup();
  databases.drop(database);

  return {
    database,
    revoked: `games(${VALID_GAME_ID}).report_status -> absent: ${deleted.stdout.trim()}`,
    resultStatus: {
      python: statusOf(sides.python),
      tsFs: statusOf(sides.tsFs),
      tsPg: statusOf(sides.tsPg),
    },
    resultBody: {
      python: bodyOf(sides.python),
      tsFs: bodyOf(sides.tsFs),
      tsPg: bodyOf(sides.tsPg),
    },
    stop,
    pids,
  };
};

beforeAll(async () => {
  if (!RUNNABLE) return;
  const stubs = STUB_MODES.map((mode) => makeStub(mode));
  const scenarios: ReadonlyArray<ScenarioSpec> = [
    UPSTREAM_DOWN,
    ...STUB_MODES.map((mode, at): ScenarioSpec => ({
      name: stubScenarioName(mode),
      serviceUrl: stubs[at]?.origin ?? '',
      why: `the \`${mode}\` upstream personality, answering every target the same way`,
    })),
  ];

  const reports = await mapPool(scenarios, SCENARIO_CONCURRENCY, runScenario);
  const provenance = await runProvenance();
  await Promise.all(stubs.map((stub) => stub.close()));

  // Every scenario dropped its own database as it finished; this is the sweep
  // that proves it, and the only place a leftover can be caught while the
  // suite can still report it.
  const leftovers = databases.dropAll();
  state.reports = new Map(reports.map((report) => [report.name, report] as const));
  state.stubs = stubs;
  state.provenance = provenance;
  state.dropFailures = leftovers;
  state.databasesAfter = listDatabases();

  // The leg-by-leg table is this file's deliverable and has to reach a
  // terminal, not a Logger.
  // oxlint-disable-next-line effecttsgo/global-console
  console.log(
    [
      '',
      POSTGRES_VERDICT.why,
      `legs ${String(MATRIX_LEGS.length)} x scenarios ${String(SCENARIO_NAMES.length)} x pairs ${String(TRIO_PAIRS.length)} = ${String(MATRIX_LEGS.length * SCENARIO_NAMES.length * TRIO_PAIRS.length)} comparisons`,
      ...renderReport(state.reports),
      '',
      `PROVENANCE (${provenance.revoked}): ${JSON.stringify(provenance.resultStatus)}`,
      '',
      ...SCENARIO_NAMES.map(
        (scenario) =>
          `MEASURED LEGS WITH NO RESPONSE ${scenario}: ${JSON.stringify(missingResponses(scenarioReport(scenario)))}`,
      ),
      '',
      `DATABASES BEFORE: ${JSON.stringify(DATABASES_BEFORE)}`,
      `DATABASES AFTER:  ${JSON.stringify(state.databasesAfter)}`,
      `EPHEMERAL CREATED: ${JSON.stringify(databases.created())}`,
      '',
    ].join('\n'),
  );
}, MATRIX_TIMEOUT_MS);

/**
 * Unconditional, and it drops databases as well as killing processes.
 *
 * A `beforeAll` that throws halfway is exactly the case that leaves both
 * behind, and an orphaned `arena_wf_*` database is worse than an orphaned
 * process: nothing reaps it, and the next run's {@link postgresVerdict} would
 * refuse to start because of it.
 */
afterAll(async () => {
  await Promise.all(state.stubs.map((stub) => stub.close()));
  await killAllBooted();
  databases.dropAll();
});

// ---------------------------------------------------------------------------
// The gates, announced
// ---------------------------------------------------------------------------

/**
 * Unconditional, and deliberately the first test in the file.
 *
 * Two gates, one message: a non-darwin platform and an unreachable Postgres are
 * both reasons this oracle contributes zero assertions, and a gate that
 * vanishes is indistinguishable from a gate that passed.
 */
test('the 3-way pg matrix is not silently skipped', () => {
  if (!RUNNABLE) {
    const what =
      `${String(SCENARIO_NAMES.length)} upstream scenarios x ${String(MATRIX_LEGS.length)} legs x ` +
      '3 pairwise comparisons (python/ts-fs/ts-pg), plus the pg provenance falsification';
    // oxlint-disable-next-line effecttsgo/global-console -- skipped oracle must reach the terminal
    console.warn(
      DARWIN
        ? `\n!! THE 3-WAY PG MATRIX DID NOT RUN: ${POSTGRES_VERDICT.why}\n!! ${what} contributed ZERO assertions.\n!! Set ARENA_REQUIRE_PARITY=1 to make this a failure instead of a warning.\n`
        : paritySkipWarning('the 3-way pg parity matrix', what),
    );
  }
  expect({ ran: RUNNABLE || !PARITY_REQUIRED, why: POSTGRES_VERDICT.why }).toEqual({
    ran: true,
    why: POSTGRES_VERDICT.why,
  });
});

// ---------------------------------------------------------------------------
// The matrix
// ---------------------------------------------------------------------------

describe.if(RUNNABLE)('the 3-way pg matrix', () => {
  test('the transcribed route table still names exactly diff.test.ts\'s legs', () => {
    expect([...ROUTE_LEG_DRIFT.mine]).toEqual([...ROUTE_LEG_DRIFT.theirs]);
  });

  SCENARIO_NAMES.forEach((scenario) => {
    describe(scenario, () => {
      test('all three gateways were given the same flags, modulo the slot-scoped and pg-only ones', () => {
        const { argv } = scenarioReport(scenario);
        expect({
          pairSameOrder: argv.pair.sameFlagOrder,
          pairDivergent: argv.pair.divergent,
          pgOnly: argv.pgOnly,
          pgSharedDivergent: argv.pgSharedDivergent,
        }).toEqual({
          pairSameOrder: true,
          pairDivergent: [...SLOT_SCOPED_FLAGS],
          pgOnly: [...PG_ONLY_FLAGS],
          pgSharedDivergent: [...SLOT_SCOPED_FLAGS],
        });
      });

      test('the fixture tree was ingested before anything was asked of it', () => {
        const { ingest } = scenarioReport(scenario);
        expect({ exit: ingest.exitCode, stderr: ingest.stderr }).toEqual({ exit: 0, stderr: '' });
        // Seven of the eight fixture directories; the eighth is the symlink,
        // which the ingester refuses for the same reason `_disk_games_index`
        // does — a run directory may not point out of `runs_root`, so the fs
        // backend 404s it and the pg backend must have no row for it.
        expect(ingest.stdout).toContain('7 inserted');
        expect(ingest.stdout).toContain(`${fixtureId('symlinked-run')}: symlink`);
      });

      test('every leg ran on all three sides', () => {
        expect(scenarioReport(scenario).outcomes.size).toBe(MATRIX_LEGS.length);
      });

      test('every Content-Length frames the body it was sent with, on all three', () => {
        expect(scenarioReport(scenario).contentLengthSelfConsistent).toBe(true);
      });

      test('every leg answered on all three sides, except the measured silences', () => {
        expect(missingResponses(scenarioReport(scenario))).toEqual(WAIVED_SILENCES);
      });

      test('all three gateways derived — or all three did not — exactly when the archive was read from storage', () => {
        // `replay_derive_cli` is a subprocess and cannot be taught a database,
        // so a *derived* answer on any side requires the loaders to have parsed
        // real savegames and cached them.  If no cache filled in a disk-fallback
        // scenario, the derivation legs agreed for the worst possible reason —
        // an archive so empty that every side said `available: false`.  If one
        // filled in a *relaying* scenario, that gateway derived something it
        // should have proxied.
        //
        // Three-sided rather than pg-only, and that is the upgrade this rig got
        // when `--materialize-root` died: the old flag could only be asked of
        // the pg leg, because only the pg leg had a staging directory.  The
        // question is now identical on all three, so an asymmetry — a pg
        // gateway that skipped a derivation the fs one performed — is visible
        // instead of unaskable.
        const expected = DISK_FALLBACK_SCENARIOS.includes(scenario);
        expect({ scenario, caches: scenarioReport(scenario).derivationCaches }).toEqual({
          scenario,
          caches: { python: expected, tsFs: expected, tsPg: expected },
        });
      });

      MATRIX_LEGS.forEach((leg) => {
        test(`${leg.name} — ${leg.why}`, () => {
          expect(legOffenders(scenario, leg, outcomesFor(scenario, leg.name))).toEqual([]);
        });
      });

      test('SIGINT is a clean exit that removes all three ready files', () => {
        const { stop } = scenarioReport(scenario);
        expect({ exit: stop.exitCodes, readyRemoved: stop.readyFilesRemoved }).toEqual({
          exit: { python: 0, tsFs: 0, tsPg: 0 },
          readyRemoved: { python: true, tsFs: true, tsPg: true },
        });
      });

      test('no gateway wrote a byte to stderr', () => {
        expect(scenarioReport(scenario).stop.stderr).toEqual({ python: '', tsFs: '', tsPg: '' });
      });

      test('no process this scenario spawned is still alive', () => {
        const report = scenarioReport(scenario);
        expect({ orphans: report.stop.orphans, alive: aliveProcesses(report.pids) }).toEqual({
          orphans: [],
          alive: [],
        });
      });
    });
  });
});

// ---------------------------------------------------------------------------
// The claims the matrix makes as a whole
// ---------------------------------------------------------------------------

describe.if(RUNNABLE)('the pg backend is byte-identical to the fs backend', () => {
  /**
   * The strictest assertion in this file, and the one the brief names: the two
   * TypeScript gateways must agree **everywhere**, with no waiver of any kind.
   */
  test('no leg diverges between ts-fs and ts-pg, in any scenario', () => {
    const pair = TRIO_PAIRS.find((candidate) => candidate.name === 'tsFs-vs-tsPg');
    const offenders = SCENARIO_NAMES.flatMap((scenario) =>
      MATRIX_LEGS.flatMap((leg) => {
        const sides = outcomesFor(scenario, leg.name);
        return pair === undefined
          ? ['the tsFs-vs-tsPg pair is not in the table']
          : divergences(leg, sides[pair.left], sides[pair.right]).map(
              (one) => `${scenario}/${leg.name} [${one.aspect}/${one.detail}] tsFs=${one.left} tsPg=${one.right}`,
            );
      }),
    );
    expect(offenders).toEqual([]);
  });

  /**
   * The waiver machinery is *off* for the same-binary pair, not merely empty.
   *
   * Without this, someone could add a `tsFs`↔`tsPg` entry to `waivers.ts` and
   * the test above would quietly stop failing.  There is no code path from that
   * list to this pair, and this asserts it by construction.
   */
  const noNewWaiversPossible = (): ReadonlyArray<string> =>
    TRIO_PAIRS.filter((pair) => pair.left !== 'python' && pair.right !== 'python' && pair.waived).map(
      (pair) => pair.name,
    );

  test('no pair of two Bun gateways is allowed to consult the waiver list', () => {
    expect(noNewWaiversPossible()).toEqual([]);
  });

  /**
   * The other half: `tsPg` inherits `tsFs`'s waivers *exactly*.
   *
   * `checkWaiver` fails three ways and all three are findings here — the pg
   * gateway behaving differently from the measured Bun signature (`Drifted`),
   * and the pg gateway agreeing with CPython where the fs gateway does not
   * (`NoLongerNeeded`), which would mean the two backends diverge on a waived
   * leg and the entry no longer describes both.
   */
  MATRIX_WAIVERS.forEach((waiver) => {
    test(`${waiver.id} applies to ts-pg exactly as it applies to ts-fs`, () => {
      const claimed =
        waiver.scenarios === undefined
          ? SCENARIO_NAMES
          : SCENARIO_NAMES.filter((scenario) => waiver.scenarios?.includes(scenario) === true);
      const verdicts = claimed.flatMap((scenario) =>
        waiver.legs.map((leg) => {
          const sides = outcomesFor(scenario, leg);
          return { scenario, verdict: checkWaiver(waiver, sides.python, sides.tsPg, leg) };
        }),
      );
      expect(verdicts.filter(({ verdict }) => verdict._tag !== 'Honored')).toEqual([]);
    });
  });

  test('no leg outside the waiver list diverges on any pair, in any scenario', () => {
    const offenders = SCENARIO_NAMES.flatMap((scenario) =>
      MATRIX_LEGS.flatMap((leg) => legOffenders(scenario, leg, outcomesFor(scenario, leg.name)).map((line) => `${scenario}/${line}`)),
    );
    expect(offenders).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// The falsification: the pg gateway really is reading Postgres
// ---------------------------------------------------------------------------

describe.if(RUNNABLE)('the pg gateway reads the database, not --runs-root', () => {
  const provenance = (): ProvenanceReport => {
    if (state.provenance === null) throw new Error('the provenance scenario did not run');
    return state.provenance;
  };

  test('revoking the stored report changes only the pg gateway\'s answer', () => {
    const { resultStatus } = provenance();
    expect(resultStatus).toEqual({ python: 200, tsFs: 200, tsPg: 404 });
  });

  test('and the 404 it answers is the one CPython answers for an absent report', () => {
    expect(provenance().resultBody.tsPg).toBe('{"error":"game report not found"}');
  });

  test('the provenance trio exited cleanly and left nothing alive', () => {
    const report = provenance();
    expect({
      exit: report.stop.exitCodes,
      orphans: report.stop.orphans,
      alive: aliveProcesses(report.pids),
    }).toEqual({ exit: { python: 0, tsFs: 0, tsPg: 0 }, orphans: [], alive: [] });
  });
});

// ---------------------------------------------------------------------------
// Postgres safety, asserted rather than promised
// ---------------------------------------------------------------------------

describe.if(RUNNABLE)('the ephemeral databases are gone', () => {
  test('every database this run created matched the ephemeral name shape', () => {
    expect(databases.created().filter((name) => !EPHEMERAL_DB_RE.test(name))).toEqual([]);
  });

  test('this run created one database per scenario, plus the provenance one', () => {
    expect(databases.created().length).toBe(SCENARIO_NAMES.length + 1);
  });

  test('the end-of-run sweep found nothing left to drop', () => {
    expect(state.dropFailures).toEqual([]);
  });

  test('not one database this run created is still on the server', () => {
    const after = new Set(state.databasesAfter ?? []);
    expect(databases.created().filter((name) => after.has(name))).toEqual([]);
  });

  /**
   * "No `arena_wf_*` remains" — with the one honest qualification a shared
   * machine forces.
   *
   * A sibling rig's live database was present before this suite started
   * ({@link FOREIGN_EPHEMERAL}) and is none of this suite's business; every
   * *other* `arena_wf_*` name on the server afterwards is one this run left
   * behind.  On a quiet server `FOREIGN_EPHEMERAL` is empty and this is the
   * literal claim the ground rule asks for.
   */
  test('no arena_wf_* database remains that was not already there', () => {
    const foreign = new Set(FOREIGN_EPHEMERAL);
    expect(
      (state.databasesAfter ?? []).filter(
        (name) => name.startsWith(EPHEMERAL_DB_PREFIX) && !foreign.has(name),
      ),
    ).toEqual([]);
  });

  /**
   * The strongest form of the ground rule: not "our databases are gone" but
   * "everything that is not an ephemeral workflow database is exactly as we
   * found it".  A dropped `scalar` would fail here and nowhere else, and the
   * filter is what keeps a sibling rig creating or dropping its own database
   * mid-run from failing this suite for someone else's activity.
   */
  test('every database that is not an ephemeral one is exactly as the suite found it', () => {
    const settled = (names: ReadonlyArray<string> | null): ReadonlyArray<string> =>
      (names ?? []).filter((name) => !name.startsWith(EPHEMERAL_DB_PREFIX));
    expect(settled(state.databasesAfter)).toEqual(settled(DATABASES_BEFORE));
  });
});
