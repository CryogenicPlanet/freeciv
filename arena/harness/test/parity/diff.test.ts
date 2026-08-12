/**
 * **The diff matrix.**  Both gateways, every upstream, every request, compared.
 *
 * `boot.ts` proves the two processes were configured identically.  `stub-supervisor.ts`
 * makes the upstream a variable this rig owns completely.  `wire-client.ts` puts
 * bytes on a socket that no client library would let through.  This file is
 * what those three exist for: a cartesian product of **upstream fixture × request**,
 * replayed against CPython and the TypeScript port, compared on status, on a
 * header subset, and on **body bytes**.
 *
 * ## What is crossed with what, exactly
 *
 * The product is **upstream fixture x request**, and it is worth spelling out
 * because the obvious reading of "the whole cross product" is not what runs:
 *
 * | Axis | Size | Crossed |
 * |---|---|---|
 * | upstream scenario | 9 — `upstream-down` + one per {@link STUB_MODES} entry (+1 opt-in live) | with every leg |
 * | request leg | 79 — 41 {@link ROUTE_LEGS} + 38 {@link CASE_LEGS} | with every scenario |
 * | route family | 12/12 `RouteDecision` tags | with every scenario |
 * | fixture class | 8/8 reached; **one representative** (`terminal-valid`) gets the route sweep | *not* crossed with routes |
 *
 * So it is 9 x 79 = 711 comparison legs, not 8 x 12 x 9.  Seven of the eight
 * fixture classes carry a direct probe each — `/status`, or `/frames` for the
 * terminal ones, and three legs for the torn tail — and are otherwise seen
 * through the `/v1/games` index body; only `terminal-valid` is asked every
 * route.  That is a deliberate trade (the fixture axis is about *disk state*,
 * which the archive layer reads once per document) and not a full product, and
 * a claim of one would be overstated.
 *
 * ## What is compared
 *
 * | Aspect | Rule |
 * |---|---|
 * | outcome | a response, or the same failure tag on both sides — a closed connection is a comparison subject, not an error |
 * | answered at all | **both** sides must produce a response, in every scenario, except the two waived silences.  Without this the row above would score a mutual timeout as parity |
 * | status | equal |
 * | reason phrase | equal.  Included because it is the *only* observable of the discarded-`statusText` waiver; without it that waiver could never be policed |
 * | headers | {@link COMPARED_HEADERS}, case-insensitively, values exact |
 * | body | **byte for byte**, JSON and PNG and stdlib HTML alike |
 *
 * Excluded, per the behavior dossier §15 and for the same reasons
 * `smoke-live.test.ts` excludes them: `Server` and `Date` (stdlib versus Bun),
 * `Connection` (HTTP/1.0 has no keep-alive), and header order and case (both
 * insignificant in HTTP; CPython emits `Allow`, Bun emits `allow`).
 *
 * ## The two exemptions, and why neither is a hole
 *
 * 1. **`normalizers.ts`** rewrites five `/health` fields that are properties of
 *    *the answering process* rather than of the port.  Every other body,
 *    including every other JSON body, is compared with no normalizer at all —
 *    which is possible only because this rig pins `--viewer-public-url`
 *    ({@link MATRIX_VIEWER_PUBLIC_URL}) on both sides, so the gateway's own port
 *    never reaches an archive document.  That is a *configuration* choice
 *    rather than a comparison weakening: the flag is byte-identical in both
 *    argvs, and `argvParity` asserts it.
 * 2. **`waivers.ts`** lists the divergences either runtime's HTTP layer imposes
 *    below any code the port owns — eight of them reach this file, seven
 *    scenario-independent and one (`binary-disk-fallback-chunked`) scoped to
 *    the five scenarios that read the archive from disk, waiving one header
 *    name and nothing else.  Each waived leg still asserts the TypeScript
 *    side's *actual* behavior, in every scenario the entry claims; a waiver
 *    that stops being needed fails the suite, and one that exempts anything
 *    outside its own scenarios fails it too.
 *
 * Anything else that diverges is a bug, and this file reports it as one.
 *
 * ## There are no open findings
 *
 * This file used to pin real defects as expected-to-fail legs so the suite
 * stayed runnable while they were open.  All four closed — three in the gateway
 * and one into `waivers.ts` as a measured decision — so the table and its
 * machinery are gone rather than left empty, and every leg below is an ordinary
 * comparison.  The history is kept where the table was; see *The findings this
 * ratchet caught*.
 *
 * ## The user's stack is never touched
 *
 * Every process this file spawns binds `--port 0` under a private `mkdtemp`;
 * `bootGatewayPair` refuses a path inside `.agent-eval/` *and* a
 * `--service-url` whose port a running stack holds; the stubs bind
 * `127.0.0.1:0`.  The one leg that could reach a real service —
 * `PARITY_SERVICE_URL` — is opt-in, and is **refused** when it names a port a
 * live `local_stack` ready record claims.  "Live" is load-bearing in both
 * directions: `boot.ts#liveStackPorts` reads the directory `local_stack.py`
 * actually writes (`.agent-eval/local-stack/`, not `.agent-eval/`), takes the
 * port out of a supervisor record that has no `port` key, and drops records
 * whose pid is gone — the earlier guard did none of the three and reported two
 * dead ports while four live ones went unseen.  Teardown happens inside
 * `beforeAll`, which is what makes the orphan check a question with a right
 * answer.
 *
 * ## The platform gate is announced
 *
 * Everything below runs under `describe.if(DARWIN)`, and the *first* test in
 * the file is unconditional and says so when it does not run.
 *
 * @module
 */

import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import {
  type ArgvParity,
  aliveProcesses,
  argvParity,
  bootGatewayPair,
  killAllBooted,
  liveStackPorts,
  PARITY_PLATFORM_SUPPORTED,
  PARITY_REQUIRED,
  paritySkipWarning,
  type StopReport,
  UNROUTABLE_UPSTREAM_TIMEOUT_S,
  UNROUTABLE_UPSTREAM_URL,
  unwrapPair,
} from './boot.ts';
import {
  type ParityRequestCase,
  REQUEST_CASES,
  VALID_GAME_ID,
} from './fixtures/request-cases.ts';
import {
  PARITY_RUNS_ROOT,
  PARITY_SCENARIOS,
  type ParityScenario,
} from './fixtures/scenarios.ts';
import { compareBodies, describeBodyVerdict } from './normalizers.ts';
import { makeStub, STUB_MODES, type StubHandle, type StubMode } from './stub-supervisor.ts';
import {
  checkWaiver,
  isWaived,
  PARITY_WAIVERS,
  type ParityWaiver,
  type WaivedAspect,
  waiverExpectsNoResponse,
  waiversIn,
  waiverStillNeeded,
} from './waivers.ts';
import { bodyLatin1, isWireResponse, type WireOutcome, wireRequest } from './wire-client.ts';

/**
 * `flock` through `bun:ffi`, and the `python3` that owns `agent_eval`, are
 * darwin facts in this checkout.  The tag is on the whole file because a
 * partial matrix would report a parity result the platform cannot support.
 *
 * The skip is **announced** — see the unconditional test right below, which is
 * the only thing this file registers outside a gated `describe` and therefore
 * the only thing standing between "the matrix passed" and "the matrix was never
 * built".
 */
const DARWIN = PARITY_PLATFORM_SUPPORTED;

/**
 * Unconditional, and deliberately the first test in the file.
 *
 * `test/gateway/smoke-live.test.ts:842-871` establishes the pattern for exactly
 * this rig and states the reason: *a gate that vanishes is indistinguishable
 * from a gate that passed*.  Without this, `bun test` on any non-darwin box
 * prints `0 pass / 4 skip / 0 fail` for a file whose job is to compare 711 legs
 * against CPython, and exits 0.
 */
test('the diff matrix is not silently skipped', () => {
  if (!DARWIN) {
    // oxlint-disable-next-line effecttsgo/global-console -- a skipped oracle has
    // to reach a terminal; there is no Logger in a bun:test process.
    console.warn(
      paritySkipWarning(
        'the parity diff matrix',
        `${String(SCENARIO_NAMES.length)} upstream scenarios x ${String(MATRIX_LEGS.length)} legs of Python-vs-TypeScript byte parity`,
      ),
    );
  }
  expect({ platform: process.platform, ranTheMatrix: DARWIN || !PARITY_REQUIRED }).toEqual({
    platform: process.platform,
    ranTheMatrix: true,
  });
});

// ---------------------------------------------------------------------------
// Configuration shared by every scenario
// ---------------------------------------------------------------------------

/**
 * Pinned on both sides, which is what buys the byte-for-byte body rule.
 *
 * `_archive_base` (`:1881`) is `viewer_public_url or str(identity["url"])`, so
 * *without* this flag every status, result, watch and frames document would
 * carry the answering gateway's own ephemeral port and no body would be
 * comparable without a normalizer.  With it, the only process-scoped bytes left
 * in the whole matrix are `/health`'s five fields.
 */
const MATRIX_VIEWER_PUBLIC_URL = 'http://viewer.parity.invalid';

/**
 * One second, everywhere.  Long enough to be a timeout, short enough that the
 * two scenarios whose upstream never answers stay runnable.
 */
const MATRIX_UPSTREAM_TIMEOUT_S = UNROUTABLE_UPSTREAM_TIMEOUT_S;

/**
 * Generous: a cold `replay.json` shells out to
 * `python3 -m agent_eval.replay_derive_cli`.  It is a deadlock detector, not a
 * budget — the framing law in `wire-client.ts` is what keeps a healthy request
 * from approaching it, and the TypeScript gateway's own 12s idle close ends the
 * legs that never answer well before this fires.
 */
const LEG_TIMEOUT_MS = 25_000;

/** Legs in flight per scenario.  Both gateways are threaded/async; 10 is idle-cheap. */
const LEG_CONCURRENCY = 10;

/** Scenarios booted at once.  Each is two processes plus a stub. */
const SCENARIO_CONCURRENCY = 3;

/**
 * Nine scenarios (ten with the opt-in live one), 79 legs each, both sides — about
 * 1400 requests, eighteen spawned processes and eight stubs.  Measured at ~25s
 * since `--upstream-timeout-s` started working on both sides; it was ~300s when
 * a third of the legs waited out Bun's 12s idle close.
 */
const MATRIX_TIMEOUT_MS = 900_000;

/** The header subset parity is asserted on. */
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

/**
 * `content-length` is dropped for the `/health` legs, and only for those.
 *
 * The body carries `pid` and `port` as bare integers, so two correct processes
 * can legitimately differ by a digit — pid `9999` beside pid `10000`.  Dropping
 * it would be an unpoliced exemption, so each side's `Content-Length` is
 * instead asserted against *its own* body length, which is the property the
 * header actually makes.
 */
const comparedHeaders = (leg: MatrixLeg): ReadonlyArray<string> =>
  leg.bodyRule === 'health'
    ? COMPARED_HEADERS.filter((name) => name !== 'content-length')
    : COMPARED_HEADERS;

// ---------------------------------------------------------------------------
// Legs
// ---------------------------------------------------------------------------

/** One request, replayed against both gateways in every scenario. */
interface MatrixLeg {
  /** Unique across the whole table; the test name and the report key. */
  readonly name: string;
  readonly method: string;
  /** The literal request-target, byte for byte on the request line. */
  readonly target: string;
  /** What this leg proves. */
  readonly why: string;
  readonly headers?: ReadonlyArray<readonly [string, string]>;
  readonly body?: string;
  /** Replaces the whole request line — for a target containing a space. */
  readonly rawRequestLine?: string;
  /** `health` opts into `normalizers.ts`; everything else is bytes. */
  readonly bodyRule: 'bytes' | 'health';
  /** What CPython answered when `request-cases.ts` was measured, when known. */
  readonly expected?: ParityRequestCase['expected'];
}

const get = (
  name: string,
  target: string,
  why: string,
  extra: Partial<MatrixLeg> = {},
): MatrixLeg => ({ name, method: 'GET', target, why, bodyRule: 'bytes', ...extra });

/**
 * A fixture class's game id, by class rather than by literal.
 *
 * Reading it out of `scenarios.ts` is what keeps the *coverage* claim below
 * honest: a class renamed or removed there fails at module load here instead of
 * quietly leaving a route unprobed.
 */
const fixtureId = (scenario: ParityScenario['scenario']): string => {
  const found = PARITY_SCENARIOS.find((candidate) => candidate.scenario === scenario);
  if (found === undefined) throw new Error(`no fixture for the ${scenario} class`);
  return found.gameId;
};

/** `game_parity_torn_tail_08` — the class no other leg asks a direct question. */
const TORN_TAIL_GAME_ID = fixtureId('torn-tail');

/**
 * The routes the brief enumerates, plus the shapes `request-cases.ts` does not
 * cover.  Order is replay order; nothing here depends on it.
 */
const ROUTE_LEGS: ReadonlyArray<MatrixLeg> = [
  get('health', '/health', 'the only route that never proxies; the five volatile fields live here', {
    bodyRule: 'health',
  }),
  get('health-query-400', '/health?x=1', 'health takes no query at all, not even an unknown one'),
  get('bare-id', `/v1/games/${VALID_GAME_ID}`, 'the bare-id alias of /status, which proxies without the suffix'),
  get('result', `/v1/games/${VALID_GAME_ID}/result`, 'the report.json projection, a different archive file from status'),
  get('watch-json', `/v1/games/${VALID_GAME_ID}/watch.json`, 'the widest archive projection: frames, players and colors in one document'),
  get('status-trailing-slash', `/v1/games/${VALID_GAME_ID}/status/`, 'path.strip("/") routes it, and the archive family forwards the slash verbatim'),
  get('replay-json', `/v1/games/${VALID_GAME_ID}/replay.json`, 'the derivation route with no query: every default in one body'),
  get('replay-after-turn', `/v1/games/${VALID_GAME_ID}/replay.json?after_turn=1`, 'pagination from a turn, and the integer spelling of next_after_turn'),
  get('replay-limit-251', `/v1/games/${VALID_GAME_ID}/replay.json?limit=251`, 'one past the documented ceiling: the 400 is the bound, spelled exactly'),
  get(
    'replay-after-turn-2-53-plus-1',
    `/v1/games/${VALID_GAME_ID}/replay.json?after_turn=9007199254740993`,
    'a turn no double spells, echoed back as next_after_turn — the open finding the query fuzz measured',
  ),
  get('replay-cold', `/v1/games/${VALID_GAME_ID}/replay.json?limit=5`, 'the first request after freshCaches: pays for the derivation subprocess'),
  get('replay-warm', `/v1/games/${VALID_GAME_ID}/replay.json?limit=5`, 'the same request again: reads what the cold leg wrote, and must be byte-identical to it'),
  get('board-turn-1', `/v1/games/${VALID_GAME_ID}/board.json?turn=1`, 'a real autosave parsed into a board — the heaviest derivation in the tree'),
  get('board-missing-turn', `/v1/games/${VALID_GAME_ID}/board.json`, 'turn is required; absent is as wrong as duplicated'),
  get('events-json', `/v1/games/${VALID_GAME_ID}/events.json`, 'the third derivation, whose numeric fields are rebuilt as bigint'),
  get('events-query-400', `/v1/games/${VALID_GAME_ID}/events.json?turn=1`, 'events reject any query at all, from the handler rather than the router'),
  get('video-mp4', `/v1/games/${VALID_GAME_ID}/video.mp4`, 'no game.mp4 on disk: the binary family 404 with its own message'),
  // --- the eighth fixture class, probed directly ----------------------------
  //
  // `game_parity_torn_tail_08` is the only class `request-cases.ts` reaches
  // *exclusively* through the `/v1/games` index body, where a wrong
  // `current_turn` is one number among four rows.  These three ask it a
  // question of its own: the backwards tail scan that walks past a half-written
  // final line to the last line that parses (turn 3, from four lines of which
  // the fourth is torn) is what decides all three answers.
  get('torn-tail-status', `/v1/games/${TORN_TAIL_GAME_ID}/status`, 'the torn-tail run on its own route, not as an index row'),
  get('torn-tail-watch', `/v1/games/${TORN_TAIL_GAME_ID}/watch.json`, 'the widest projection of a run whose replay tail is unparseable'),
  get('torn-tail-frames', `/v1/games/${TORN_TAIL_GAME_ID}/frames`, 'the frame listing of a non-terminal run, from the same torn archive'),
  get('root-path', '/', 'the shortest unroutable path there is'),
  get('unknown-path', '/nope', 'an unroutable path that is not a prefix of any route'),
  get('unknown-deep-path', '/v1/nope/x/y', 'unroutable but inside the versioned prefix'),
  get('private-route', `/v1/games/${VALID_GAME_ID}/join`, 'a real supervisor route the gateway must not expose'),
  get('nonsense-suffix-query-400', `/v1/games/${VALID_GAME_ID}/nonsense?x=1`, 'the query gate precedes the final 404: a 400 about a route that does not exist'),
  get('absolute-form-target', 'http://evil.invalid/health', 'urlsplit ignores the netloc, so this is /health — and its body is /health\'s', {
    bodyRule: 'health',
  }),
  // --- methods ------------------------------------------------------------
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
  // --- request bodies -----------------------------------------------------
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
    why: 'a chunked GET *does* reach the handler on both sides — the control row for the identity waiver',
    headers: [['Transfer-Encoding', 'chunked']],
    body: '0\r\n\r\n',
    bodyRule: 'bytes',
  },
  {
    name: 'bad-content-length',
    method: 'GET',
    target: '/health',
    why: 'WAIVED: Bun\'s parser answers an unreadable Content-Length itself',
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

/** `request-cases.ts`, as legs.  Their `expected` is CPython's measured answer. */
const CASE_LEGS: ReadonlyArray<MatrixLeg> = REQUEST_CASES.map(
  (kase): MatrixLeg => ({
    name: kase.name,
    method: 'GET',
    target: kase.target,
    why: kase.why,
    bodyRule: 'bytes',
    expected: kase.expected,
  }),
);

/** Every leg, in replay order. */
const MATRIX_LEGS: ReadonlyArray<MatrixLeg> = [...ROUTE_LEGS, ...CASE_LEGS];

/**
 * The two legs whose *order* is their meaning, run sequentially per side after
 * `freshCaches()`.  Everything else is order-independent and runs pooled.
 */
const SEQUENTIAL_LEG_NAMES: ReadonlyArray<string> = ['replay-cold', 'replay-warm'];

const legByName = (name: string): MatrixLeg => {
  const leg = MATRIX_LEGS.find((candidate) => candidate.name === name);
  if (leg === undefined) throw new Error(`no leg named ${name}`);
  return leg;
};

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

/** One upstream fixture, and the pair of gateways pointed at it. */
interface ScenarioSpec {
  readonly name: string;
  readonly serviceUrl: string;
  readonly why: string;
}

/** `upstream-down` plus one per stub mode, plus the opt-in live leg. */
const stubScenarioName = (mode: StubMode): string => `stub-${mode}`;

/**
 * The unroutable-address fixture, per the parity-client law.
 *
 * A *released ephemeral port* is not an upstream-down fixture: the kernel is
 * free to hand it to the next binder, which in a rig whose gateways bind
 * ephemeral ports has been observed to be the gateway, proxying to itself.
 * RFC 5737 TEST-NET-1 cannot be routed and cannot be reused.
 */
const UPSTREAM_DOWN: ScenarioSpec = {
  name: 'upstream-down',
  serviceUrl: UNROUTABLE_UPSTREAM_URL,
  why: 'connect never completes: the whole disk-fallback matrix, and the connect-timeout branch',
};

/** `PARITY_SERVICE_URL` — a real supervisor, opt-in, never the user's live one. */
const LIVE_SERVICE_URL: string | undefined = process.env['PARITY_SERVICE_URL'];

// ---------------------------------------------------------------------------
// Live-stack safety
// ---------------------------------------------------------------------------

/**
 * The ports a running `local_stack` claims live in `boot.ts` — see
 * {@link liveStackPorts}, which reads `.agent-eval/local-stack/*.json`, takes a
 * `port` **or** the port of `url`/`internal_service_url`/`upstream_service_url`
 * (a supervisor record has no `port` key at all, and a supervisor is what
 * `--service-url` names), and drops records whose pid is not alive.
 *
 * It is used twice, and the second use is the one that matters: this file
 * refuses the *scenario*, and `bootGatewayPair` refuses the *boot*, so a caller
 * that never consults this verdict is still guarded.
 */

/**
 * Refuse a `PARITY_SERVICE_URL` that names a port the live stack published.
 *
 * The ground rule is absolute: the user's running stack and any live game are
 * off limits.  An opt-in environment variable is not consent to proxy a real
 * match's supervisor through a test rig.
 */
type LiveVerdict =
  | { readonly _tag: 'Absent'; readonly why: string }
  | { readonly _tag: 'Refused'; readonly why: string }
  | { readonly _tag: 'Usable'; readonly url: string; readonly why: string };

const liveServiceUrlVerdict = (
  url: string | undefined,
  claimed: ReadonlyArray<number>,
): LiveVerdict => {
  const stack =
    claimed.length > 0
      ? `a local_stack is RUNNING (ready records claim ports ${claimed.join(', ')})`
      : 'no local_stack ready record was found';
  if (url === undefined) {
    return {
      _tag: 'Absent',
      why: `PARITY_SERVICE_URL is unset, so the live-supervisor scenario is SKIPPED; ${stack}, and this rig never contacts it`,
    };
  }
  if (!URL.canParse(url)) {
    return { _tag: 'Refused', why: `PARITY_SERVICE_URL is not a URL: ${url}` };
  }
  const port = Number(new URL(url).port);
  return claimed.includes(port)
    ? {
        _tag: 'Refused',
        why: `PARITY_SERVICE_URL names port ${String(port)}, which a live local_stack ready record claims — REFUSED, the user's stack is never touched`,
      }
    : { _tag: 'Usable', url, why: `live-supervisor scenario RAN against ${url}` };
};

/**
 * Decided at module load, not in `beforeAll`, because `bun:test` registers
 * `describe` bodies before any hook runs and the live scenario needs its own
 * per-leg tests declared like every other scenario's.
 */
const LIVE_VERDICT: LiveVerdict = liveServiceUrlVerdict(LIVE_SERVICE_URL, liveStackPorts());

const LIVE_SCENARIO: ScenarioSpec | null =
  LIVE_VERDICT._tag === 'Usable'
    ? {
        name: 'live-supervisor',
        serviceUrl: LIVE_VERDICT.url,
        why: 'an opt-in real supervisor, via PARITY_SERVICE_URL',
      }
    : null;

// ---------------------------------------------------------------------------
// Running one scenario
// ---------------------------------------------------------------------------

/** A value keyed by implementation. */
interface Sided<A> {
  readonly python: A;
  readonly typescript: A;
}

/** Everything one scenario produced. */
interface ScenarioReport {
  readonly name: string;
  readonly serviceUrl: string;
  readonly outcomes: ReadonlyMap<string, Sided<WireOutcome>>;
  readonly argv: ArgvParity;
  readonly stop: StopReport;
  readonly pids: ReadonlyArray<number>;
  /** Each side's `Content-Length` against its own body length, for the health legs. */
  readonly contentLengthSelfConsistent: boolean;
}

/**
 * Bounded-concurrency `map`, without an index cell mutated in place.
 *
 * Each worker recurses on an accumulator of `[position, value]` pairs and the
 * results are reassembled by position at the end, so nothing writes into a
 * shared array and the output order is the input order regardless of who
 * finished first.
 */
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

const runLeg = (origin: string, leg: MatrixLeg): Promise<WireOutcome> =>
  wireRequest(origin, {
    method: leg.method,
    target: leg.target,
    ...(leg.headers === undefined ? {} : { headers: leg.headers }),
    ...(leg.body === undefined ? {} : { body: leg.body }),
    ...(leg.rawRequestLine === undefined ? {} : { rawRequestLine: leg.rawRequestLine }),
    timeoutMs: LEG_TIMEOUT_MS,
  });

/** Both sides of one leg, concurrently — they are two processes, not one queue. */
const runBoth = async (
  origins: Sided<string>,
  leg: MatrixLeg,
): Promise<readonly [string, Sided<WireOutcome>]> => {
  const [python, typescript] = await Promise.all([
    runLeg(origins.python, leg),
    runLeg(origins.typescript, leg),
  ]);
  return [leg.name, { python, typescript }] as const;
};

/** `Content-Length` against the body it framed — a per-side property, not a comparison. */
const contentLengthMatchesBody = (outcome: WireOutcome): boolean => {
  if (!isWireResponse(outcome)) return true;
  const declared = outcome.headers.get('content-length');
  if (declared === null || !/^[0-9]+$/.test(declared)) return true;
  // A `HEAD` response declares the length of the body it is not sending.
  return outcome.completedBy === 'head-request' || Number(declared) === outcome.bodyBytes.byteLength;
};

const runScenario = async (spec: ScenarioSpec): Promise<ScenarioReport> => {
  const pair = unwrapPair(
    await bootGatewayPair({
      runsRoot: PARITY_RUNS_ROOT,
      serviceUrl: spec.serviceUrl,
      scenario: spec.name,
      upstreamTimeoutSeconds: MATRIX_UPSTREAM_TIMEOUT_S,
      viewerPublicUrl: MATRIX_VIEWER_PUBLIC_URL,
    }),
  );
  const origins: Sided<string> = { python: pair.python.origin, typescript: pair.typescript.origin };
  const argv = argvParity(pair);
  const pids = [pair.python.pid, pair.typescript.pid];

  // The cold/warm pair first, and strictly in order: `freshCaches()` empties
  // both derivation caches, so the next `replay-cold` pays for
  // `replay_derive_cli` and `replay-warm` must read what it wrote.
  await pair.freshCaches();
  const sequential = await SEQUENTIAL_LEG_NAMES.reduce<
    Promise<ReadonlyArray<readonly [string, Sided<WireOutcome>]>>
  >(
    async (previous, name) => [...(await previous), await runBoth(origins, legByName(name))],
    Promise.resolve([]),
  );

  const pooled = await mapPool(
    MATRIX_LEGS.filter((leg) => !SEQUENTIAL_LEG_NAMES.includes(leg.name)),
    LEG_CONCURRENCY,
    (leg) => runBoth(origins, leg),
  );

  const outcomes = new Map([...sequential, ...pooled]);
  const stop = await pair.stop();
  pair.cleanup();

  return {
    name: spec.name,
    serviceUrl: spec.serviceUrl,
    outcomes,
    argv,
    stop,
    pids,
    contentLengthSelfConsistent: Array.from(outcomes.values()).every(
      (sides) => contentLengthMatchesBody(sides.python) && contentLengthMatchesBody(sides.typescript),
    ),
  };
};

// ---------------------------------------------------------------------------
// Comparison
// ---------------------------------------------------------------------------

/** One aspect on which the two implementations disagreed. */
interface Divergence {
  readonly aspect: WaivedAspect | 'outcome';
  readonly detail: string;
  readonly python: string;
  readonly typescript: string;
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
 * Every way this leg's two outcomes disagree, as values.
 *
 * Waivers are *not* applied here: the full list is computed first so the
 * evidence table can show what a waiver is actually hiding.
 */
const divergences = (leg: MatrixLeg, sides: Sided<WireOutcome>): ReadonlyArray<Divergence> => {
  const { python, typescript } = sides;
  if (!isWireResponse(python) || !isWireResponse(typescript)) {
    return python._tag === typescript._tag
      ? []
      : [
          {
            aspect: 'outcome',
            detail: 'one side produced a response and the other did not',
            python: python._tag,
            typescript: typescript._tag,
          },
        ];
  }
  const names = comparedHeaders(leg);
  const pythonHeaders = headerMap(python, names);
  const typescriptHeaders = headerMap(typescript, names);
  const bodyVerdict = compareBodies(leg.bodyRule, python, typescript);
  return [
    ...(python.status === typescript.status
      ? []
      : [
          {
            aspect: 'status' as const,
            detail: 'status',
            python: String(python.status),
            typescript: String(typescript.status),
          },
        ]),
    ...(python.reasonLine === typescript.reasonLine
      ? []
      : [
          {
            aspect: 'reason' as const,
            detail: 'reason phrase',
            python: python.reasonLine,
            typescript: typescript.reasonLine,
          },
        ]),
    ...names.flatMap((name): ReadonlyArray<Divergence> =>
      pythonHeaders[name] === typescriptHeaders[name]
        ? []
        : [
            {
              aspect: 'headers',
              detail: name,
              python: String(pythonHeaders[name]),
              typescript: String(typescriptHeaders[name]),
            },
          ],
    ),
    ...(bodyVerdict._tag === 'Equal'
      ? []
      : [
          {
            aspect: 'body' as const,
            detail: describeBodyVerdict(bodyVerdict),
            python: bodyVerdict._tag === 'Differ' ? preview(bodyVerdict.python) : bodyVerdict._tag,
            typescript:
              bodyVerdict._tag === 'Differ' ? preview(bodyVerdict.typescript) : bodyVerdict._tag,
          },
        ]),
  ];
};

/**
 * The divergences a waiver does not excuse — what a leg's test asserts is empty.
 *
 * An `outcome` divergence (one side never produced a response at all) is
 * excused only by a waiver that also waives `status`, which is true of exactly
 * the two legs where Bun answers with silence: the invented verb and the
 * body-visible `HEAD`.
 *
 * `scenario` and the divergence's `detail` are both load-bearing rather than
 * decorative: one waiver is scoped to the five scenarios that read the archive
 * from disk, and it waives a single *header name* out of the compared set, so a
 * leg that starts diverging on a second header — or in a sixth scenario — is an
 * ordinary failure.
 */
const unwaived = (
  scenario: string,
  leg: MatrixLeg,
  found: ReadonlyArray<Divergence>,
): ReadonlyArray<Divergence> =>
  found.filter(
    (one) =>
      !isWaived(
        scenario,
        leg.name,
        one.aspect === 'outcome' ? 'status' : one.aspect,
        one.detail,
      ),
  );

// ---------------------------------------------------------------------------
// The findings this ratchet caught, and what each one became
// ---------------------------------------------------------------------------

/**
 * **There are no open findings.**  Every leg below is an ordinary `test`.
 *
 * This file used to carry an `OPEN_FINDINGS` table: real defects, pinned as an
 * explicit `(scenario, leg)` set of expected-to-fail legs so the suite stayed
 * runnable while they were open.  The table ratcheted in both directions — a
 * fix turned its legs green, which failed the suite until the entry was
 * deleted, and a *new* leg falling into the same hole was an ordinary failing
 * test because it was not on the list.  It is empty now, and an empty table is
 * dead machinery, so it is gone: the next divergence is simply a failing test,
 * which is the strongest form of the same ratchet.
 *
 * What the four entries were is worth keeping, because what a fix *was* is the
 * most useful thing this file can tell the next reader:
 *
 * 1. **`upstream-timeout-ignored`** — every upstream-touching route in the two
 *    scenarios whose upstream never answers (`upstream-down`, `stub-hang`): 32
 *    legs on which CPython served the disk archive after exactly the configured
 *    second and the TypeScript side answered *nothing*, its connection closed at
 *    ~12s by Bun's idle timeout.  Cause: `HttpApp.toHandled` runs a handler
 *    inside `Effect.uninterruptible`, and a forked fiber inherits that, so
 *    `Effect.timeout` decided on time and then waited for the arm it had just
 *    cancelled.  Fix: `Effect.interruptible` at the app, which is what
 *    `@effect/platform`'s own routers do per route
 *    (`src/gateway/server.ts#gatewayApp`).  Re-measured: `200` in 1030ms
 *    against `192.0.2.1:9` at `-s 1`, and no stderr.
 * 2. **`frame-index-overflow-500`** — `frame-huge` and
 *    `frame-beyond-safe-integer` in *every* scenario answered a bare `500` with
 *    an empty body.  Cause: wire's `FrameIndexFromPngName` transform calls
 *    `FrameIndex.make(Number(digits))` and `Schema.int()` is
 *    `Number.isSafeInteger`, so the brand threw a `ParseError` *inside* the
 *    decode, escaping the `Either` the tolerant decoder promises.  Fix: a total
 *    parse in the dispatcher that routes an unrepresentable index to an index no
 *    six-digit archive can hold (`http/dispatch.ts#frameIndexFromPngName`), so
 *    the leg misses the lookup and answers CPython's `404 map frame does not
 *    exist` byte for byte.
 * 3. **`loader-integer-saturates-at-2-53`** — `replay-after-turn-2-53-plus-1`
 *    in the five scenarios that derive from disk: `?after_turn=9007199254740993`
 *    came back as `"next_after_turn":9007199254740991`, because
 *    `routes/replay.ts#toLoaderInteger` clamped the query's exact `bigint` to
 *    `Number.MAX_SAFE_INTEGER` on the way into a derivation service whose inputs
 *    were `number`.  Fix: `bigint` in `services/derivation.ts`'s two loader
 *    input types, and `toLoaderInteger` deleted rather than fixed — there is now
 *    no seam to saturate at.  The leg is an ordinary comparison from here on,
 *    and the derived body echoes the digits CPython echoes.
 * 4. **`binary-disk-fallback-chunked`** — the one that did *not* close in the
 *    gateway.  Its obvious fix was built, measured, and rejected: the body type
 *    that keeps `Content-Length` also makes Bun answer `Range` with a `206`
 *    where CPython ignores the header and sends a `200`, and trading a framing
 *    divergence for a status divergence on the video route is not a fix.  It
 *    moved to `waivers.ts` with the whole measurement table, and it is policed
 *    there exactly as tightly: four legs, five scenarios, the bytes still
 *    compared byte-for-byte, and the entry dies the moment the two sides agree.
 */

// ---------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------

interface MatrixState {
  reports: ReadonlyMap<string, ScenarioReport>;
  stubs: ReadonlyArray<StubHandle>;
  liveNote: string;
  scenarios: ReadonlyArray<ScenarioSpec>;
}

/**
 * One mutable cell, written once by `beforeAll` and read by every test — the
 * shape `bun:test` forces on a rig whose fixture is a fleet of processes.
 */
const state: MatrixState = { reports: new Map(), stubs: [], liveNote: '', scenarios: [] };

const scenarioReport = (name: string): ScenarioReport => {
  const report = state.reports.get(name);
  if (report === undefined) throw new Error(`scenario ${name} did not run`);
  return report;
};

const outcomesFor = (scenario: string, leg: string): Sided<WireOutcome> => {
  const sides = scenarioReport(scenario).outcomes.get(leg);
  if (sides === undefined) throw new Error(`leg ${leg} did not run in scenario ${scenario}`);
  return sides;
};

/** The evidence table: divergences only, plus a per-scenario count. */
const renderReport = (reports: ReadonlyMap<string, ScenarioReport>): ReadonlyArray<string> =>
  Array.from(reports.values()).flatMap((report): ReadonlyArray<string> => {
    const rows = MATRIX_LEGS.flatMap((leg): ReadonlyArray<string> => {
      const sides = report.outcomes.get(leg.name);
      if (sides === undefined) return [`  ${leg.name}: DID NOT RUN`];
      const found = divergences(leg, sides);
      return found.length === 0
        ? []
        : found.map(
            (one) =>
              `  ${unwaived(report.name, leg, [one]).length === 0 ? 'waived ' : 'DIFFER '}` +
              `${leg.name} [${one.aspect}/${one.detail}] py=${one.python} ts=${one.typescript}`,
          );
    });
    return [
      `${report.name}  (${report.serviceUrl}, ${String(report.outcomes.size)} legs)`,
      ...rows,
      `  divergent legs: ${String(
        MATRIX_LEGS.filter((leg) => {
          const sides = report.outcomes.get(leg.name);
          return sides !== undefined && divergences(leg, sides).length > 0;
        }).length,
      )}`,
    ];
  });

/**
 * Every `(leg, side)` that produced **no response at all**, as `leg/side`.
 *
 * Two-sided on purpose, and that is the point.  The comparison in
 * {@link divergences} treats "neither side answered, and the failure tags
 * match" as parity — which it must, because a closed connection *is* a
 * comparison subject — but that reading is only safe if something else insists
 * a leg answered in the first place.  Nothing did: a mutual timeout in any of
 * the seven healthy scenarios would have compared no status, no header and no
 * byte, and passed.  `hunt-header-method-fuzz.test.ts:456` already refuses a
 * non-response the strong way; this is the matrix doing the same.
 *
 * A leg the scenario never ran at all is reported here too, rather than being
 * silently absent from the list.
 */
const missingResponses = (report: ScenarioReport): ReadonlyArray<string> =>
  MATRIX_LEGS.flatMap((leg): ReadonlyArray<string> => {
    const sides = report.outcomes.get(leg.name);
    if (sides === undefined) return [`${leg.name}/did-not-run`];
    return SIDES.flatMap((side) => (isWireResponse(sides[side]) ? [] : [`${leg.name}/${side}`]));
  }).toSorted();

/** The two implementations, in a fixed order, for a rig that iterates. */
const SIDES = ['python', 'typescript'] as const;

/**
 * The waivers this file owns.  The rest belong to `hunt-query-fuzz.test.ts`,
 * whose legs are request targets rather than routes, and are policed there.
 */
const MATRIX_WAIVERS: ReadonlyArray<ParityWaiver> = waiversIn('matrix');

/**
 * The silences the rig accepts, derived rather than restated.
 *
 * A waiver whose measured signature for a side is an outcome *tag* rather than
 * a status line is a waiver that says "this side answers nothing": Bun's
 * `ClosedWithoutResponse` on an invented verb, and the `Truncated` read of an
 * adapter-stripped `HEAD` body.  Reading it off `waivers.ts`
 * ({@link waiverExpectsNoResponse}) means the exempt set cannot drift from the
 * waiver list, and that deleting a waiver deletes its exemption.
 */
const WAIVED_SILENCES: ReadonlyArray<string> = MATRIX_WAIVERS.flatMap((waiver) =>
  SIDES.flatMap((side) =>
    waiverExpectsNoResponse(waiver, side) ? waiver.legs.map((leg) => `${leg}/${side}`) : [],
  ),
).toSorted();

beforeAll(async () => {
  if (!DARWIN) return;
  const stubs = STUB_MODES.map((mode) => makeStub(mode));
  const scenarios: ReadonlyArray<ScenarioSpec> = [
    UPSTREAM_DOWN,
    ...STUB_MODES.map((mode, at): ScenarioSpec => ({
      name: stubScenarioName(mode),
      serviceUrl: stubs[at]?.origin ?? '',
      why: `the \`${mode}\` upstream personality, answering every target the same way`,
    })),
    ...(LIVE_SCENARIO === null ? [] : [LIVE_SCENARIO]),
  ];

  const reports = await mapPool(scenarios, SCENARIO_CONCURRENCY, runScenario);
  await Promise.all(stubs.map((stub) => stub.close()));

  const byName = new Map(reports.map((report) => [report.name, report] as const));
  state.reports = byName;
  state.stubs = stubs;
  state.liveNote = LIVE_VERDICT.why;
  state.scenarios = scenarios;

  // The leg-by-leg table is this file's deliverable and has to reach a
  // terminal, not a Logger.
  // oxlint-disable-next-line effecttsgo/global-console
  console.log(
    [
      '',
      LIVE_VERDICT.why,
      ...renderReport(byName),
      '',
      ...SCENARIO_NAMES.map(
        (scenario) =>
          `MEASURED LEGS WITH NO RESPONSE ${scenario}: ${JSON.stringify(
            missingResponses(scenarioReport(scenario)),
          )}`,
      ),
      '',
    ].join('\n'),
  );
}, MATRIX_TIMEOUT_MS);

/**
 * Unconditional: driven by `boot.ts`'s spawn registry rather than by a
 * successfully-built rig, because a `beforeAll` that throws halfway is exactly
 * the case that leaves processes behind.
 */
afterAll(async () => {
  await Promise.all(state.stubs.map((stub) => stub.close()));
  await killAllBooted();
});

// ---------------------------------------------------------------------------
// The matrix
// ---------------------------------------------------------------------------

const SCENARIO_NAMES: ReadonlyArray<string> = [
  UPSTREAM_DOWN.name,
  ...STUB_MODES.map(stubScenarioName),
  ...(LIVE_SCENARIO === null ? [] : [LIVE_SCENARIO.name]),
];

describe.if(DARWIN)('the diff matrix', () => {
  SCENARIO_NAMES.forEach((scenario) => {
    describe(scenario, () => {
      test('both gateways were given the same flags, modulo the two slot-scoped ones', () => {
        const { argv } = scenarioReport(scenario);
        expect({ sameOrder: argv.sameFlagOrder, divergent: argv.divergent }).toEqual({
          sameOrder: true,
          divergent: ['--cache-root', '--ready-file'],
        });
      });

      test('every leg ran on both sides', () => {
        expect(scenarioReport(scenario).outcomes.size).toBe(MATRIX_LEGS.length);
      });

      test('every Content-Length frames the body it was sent with', () => {
        expect(scenarioReport(scenario).contentLengthSelfConsistent).toBe(true);
      });

      // The comparison scores "neither side answered, same failure tag" as
      // parity, which is right — and safe only because this test exists.
      test('every leg answered on both sides, except the measured silences', () => {
        expect(missingResponses(scenarioReport(scenario))).toEqual(WAIVED_SILENCES);
      });

      MATRIX_LEGS.forEach((leg) => {
        test(`${leg.name} — ${leg.why}`, () => {
          const sides = outcomesFor(scenario, leg.name);
          expect(unwaived(scenario, leg, divergences(leg, sides))).toEqual([]);
        });
      });

      test('SIGINT is a clean exit that removes both ready files', () => {
        const { stop } = scenarioReport(scenario);
        expect({ exit: stop.exitCodes, readyRemoved: stop.readyFilesRemoved }).toEqual({
          exit: { python: 0, typescript: 0 },
          readyRemoved: { python: true, typescript: true },
        });
      });

      // Split from the exit-code assertion because it was the last thing the
      // two hanging-upstream scenarios failed: Bun announcing the idle close of
      // a socket whose handler was still blocked, which was a *symptom* of the
      // `upstream-timeout-ignored` finding rather than a second defect.  Both
      // are closed, so this is an ordinary test in every scenario.
      test('neither gateway wrote a byte to stderr', () => {
        expect(scenarioReport(scenario).stop.stderr).toEqual({ python: '', typescript: '' });
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
// The waiver list polices itself
// ---------------------------------------------------------------------------

/**
 * The scenarios a waiver claims to diverge in — its own list, or all of them.
 *
 * Naming a subset is a claim in both directions, and this function is only the
 * first half of it: the listed scenarios are checked below, and the unlisted
 * ones are checked by the ordinary per-leg comparison above, which applies no
 * exemption at all because {@link isWaived} consults the same list.
 */
const waivedScenarios = (waiver: ParityWaiver): ReadonlyArray<string> =>
  waiver.scenarios === undefined
    ? SCENARIO_NAMES
    : SCENARIO_NAMES.filter((scenario) => waiver.scenarios?.includes(scenario) === true);

describe.if(DARWIN)('waivers', () => {
  test('every matrix waiver names legs that exist', () => {
    const unknown = MATRIX_WAIVERS.flatMap((waiver) =>
      waiver.legs.filter((leg) => MATRIX_LEGS.every((candidate) => candidate.name !== leg)),
    );
    expect(unknown).toEqual([]);
  });

  test('every scenario a waiver names exists', () => {
    const unknown = MATRIX_WAIVERS.flatMap((waiver) =>
      (waiver.scenarios ?? []).filter((scenario) => !SCENARIO_NAMES.includes(scenario)),
    );
    expect(unknown).toEqual([]);
  });

  test('no waiver claims a divergence that is not one', () => {
    expect(
      PARITY_WAIVERS.filter((waiver) => !waiverStillNeeded(waiver)).map((waiver) => waiver.id),
    ).toEqual([]);
  });

  test('every waived header name is one this matrix actually compares', () => {
    const unknown = MATRIX_WAIVERS.flatMap((waiver) =>
      (waiver.headers ?? []).filter(
        (name) => !COMPARED_HEADERS.some((compared) => compared === name),
      ),
    );
    expect(unknown).toEqual([]);
  });

  MATRIX_WAIVERS.forEach((waiver) => {
    // A waived leg is never skipped: it asserts the TypeScript side's *actual*
    // behavior, in every scenario it claims, and fails if the divergence
    // disappears in any of them.
    test(`${waiver.id} still diverges exactly as measured, in every scenario it claims`, () => {
      const verdicts = waivedScenarios(waiver).flatMap((scenario) =>
        waiver.legs.map((leg) => {
          const sides = outcomesFor(scenario, leg);
          return {
            scenario,
            verdict: checkWaiver(waiver, sides.python, sides.typescript, leg),
          };
        }),
      );
      expect(verdicts.filter(({ verdict }) => verdict._tag !== 'Honored')).toEqual([]);
    });
  });

  /**
   * The other half of a scenario-scoped waiver: outside the scenarios it names,
   * the leg is compared in full and must **match**.
   *
   * Without this, narrowing a waiver's scenario list would be indistinguishable
   * from widening it — the per-leg tests would pass either way if the leg
   * happened to agree everywhere, and a leg that started diverging in a sixth
   * scenario would be caught only by the summary below.  This says it directly,
   * on the one entry that has a list.
   */
  test('a scenario-scoped waiver exempts nothing outside its own scenarios', () => {
    const leaks = MATRIX_WAIVERS.filter((waiver) => waiver.scenarios !== undefined).flatMap(
      (waiver) =>
        SCENARIO_NAMES.filter((scenario) => !waivedScenarios(waiver).includes(scenario)).flatMap(
          (scenario) =>
            waiver.legs.flatMap((leg) => {
              const found = divergences(legByName(leg), outcomesFor(scenario, leg));
              return found.map((one) => `${scenario}/${leg} [${one.aspect}/${one.detail}]`);
            }),
        ),
    );
    expect(leaks).toEqual([]);
  });

  /**
   * The summary claim the whole rig makes: outside the waiver list, the two
   * gateways are byte-identical everywhere, in every scenario.
   *
   * Redundant with the per-leg tests by construction, and deliberately so — it
   * is the one assertion whose failure message names *every* new divergence at
   * once instead of one per test.  There is no open-findings escape hatch in it
   * any more, because there are no open findings.
   */
  test('no leg outside the waiver list diverges anywhere', () => {
    const offenders = SCENARIO_NAMES.flatMap((scenario) =>
      MATRIX_LEGS.flatMap((leg) => {
        const found = unwaived(scenario, leg, divergences(leg, outcomesFor(scenario, leg.name)));
        return found.map((one) => `${scenario}/${leg.name} [${one.aspect}/${one.detail}]`);
      }),
    );
    expect(offenders).toEqual([]);
  });

  test('the live-supervisor leg reports what it did', () => {
    expect(state.liveNote).not.toBe('');
  });
});

// ---------------------------------------------------------------------------
// The fixture table, against the CPython oracle
// ---------------------------------------------------------------------------

/**
 * `request-cases.ts` records what CPython answered when the table was
 * measured.  Re-checking it against a *live* CPython in the upstream-down
 * scenario is what keeps the fixture honest — and it is the only assertion in
 * this file that is about one implementation rather than about the pair.
 */
describe.if(DARWIN)('the request-case table still describes CPython', () => {
  CASE_LEGS.forEach((leg) => {
    test(leg.name, () => {
      const expectation = leg.expected;
      if (expectation === undefined) return;
      const { python } = outcomesFor(UPSTREAM_DOWN.name, leg.name);
      expect(isWireResponse(python)).toBe(true);
      if (!isWireResponse(python)) return;
      expect({ leg: leg.name, status: python.status }).toEqual({
        leg: leg.name,
        status: expectation.status,
      });
      expect({ leg: leg.name, contentType: python.headers.get('content-type') }).toEqual({
        leg: leg.name,
        contentType:
          expectation._tag === 'Ok' ? expectation.contentType : 'application/json; charset=utf-8',
      });
      if (expectation._tag === 'Problem') {
        expect(bodyLatin1(python)).toBe(`{"error":${JSON.stringify(expectation.message)}}`);
      }
    });
  });
});
