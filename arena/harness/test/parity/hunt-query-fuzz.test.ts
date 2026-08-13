/**
 * **The query/parameter space, pinned where the two gateways agree.**
 *
 * `diff.test.ts` compares a route table; this file compares the *values inside
 * a route* — `after_turn`, `limit`, `turn`, and the shape of the query string
 * that carries them.  It exists because those three integers are the only
 * user-controlled numbers the gateway parses, they are parsed by
 * `int()`-compatible code rather than by `Number()`, and every boundary in that
 * parser is a place two implementations can drift apart while every status code
 * in the route table stays identical.
 *
 * ## What a leg proves
 *
 * Each leg is one request-target replayed against both gateways in two upstream
 * phases:
 *
 * - **`stub`** — an `ok-json` stub upstream.  A query that survives its parser
 *   is relayed as a 200, and the stub's request log shows the **normalized**
 *   query each gateway forwarded (`after_turn=N&limit=N`, `turn=N`).  That
 *   makes the *canonicalization* observable, not just the verdict: a leg where
 *   both sides answer 200 but forward `after_turn=5` and `after_turn=05` is a
 *   divergence this file catches and a status comparison would not.  It also
 *   pins the promise that a 400 opens no socket at all — the forwarded list is
 *   empty on both sides for every refusal below.
 * - **`disk`** — `http://127.0.0.1:1`, refused instantly, so both gateways take
 *   the disk fallback and the same leg is compared through a second code path.
 *   ({@link REFUSED_UPSTREAM_URL} and not the RFC 5737 unroutable fixture:
 *   `boot.ts` documents that the TypeScript gateway answers *nothing* for 12s
 *   on that path, which would drown a query finding in an unrelated one.)
 * - **`derive`** — the `disk` phase against the one fixture with real
 *   autosaves, so the derivation subprocess runs and the loaders' integer width
 *   is observable in the body.
 *
 * ## Agreements and runtime-boundary waivers are pinned here
 *
 * Every leg without a waiver agrees byte for byte on both sides. The four
 * runtime-boundary disagreements remain `waivers.ts` entries with measured
 * signatures, and each fails if either side moves or the implementations begin
 * to agree. A waiver nobody can observe is a waiver nobody can police.
 *
 * ## The traversal corpus
 *
 * {@link TRAVERSAL_LEGS} replays escape-shaped and normalization-sensitive
 * targets, including raw cross-game `A/../B`. Node now preserves the raw request
 * target at the server edge, so that cross-game case is ordinary byte parity:
 * both gateways refuse it. Backslash and self-dot cases retain their narrower
 * traversal-safety assertion because their refusal bodies differ.
 *
 * Every process this file spawns binds `--port 0` under a private `mkdtemp`,
 * and the stub binds `127.0.0.1:0`; nothing here can reach a running stack.
 *
 * @module
 */

import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import {
  aliveProcesses,
  bootGatewayPair,
  type GatewayPair,
  killAllBooted,
  REFUSED_UPSTREAM_URL,
  type StopReport,
  unwrapPair,
} from './boot.ts';
import { VALID_GAME_ID } from './fixtures/request-cases.ts';
import { PARITY_RUNS_ROOT } from './fixtures/scenarios.ts';
import { makeStub, type StubHandle } from './stub-supervisor.ts';
import { checkWaiver, waiverFor, waiversIn, waiverStillNeeded } from './waivers.ts';
import { isWireResponse, wireGet, type WireOutcome } from './wire-client.ts';

// ---------------------------------------------------------------------------
// Fixture ids
// ---------------------------------------------------------------------------

/** Well-formed under `GAME_ID_RE`, absent from the tree: a leg stays cheap. */
const ABSENT_GAME_ID = 'game_parity_absent_wellformed_id';

/** Under the 20-char floor, so the router refuses it before any query is read. */
const SHORT_GAME_ID = 'tooshort';

/**
 * Pinned so an archive body never carries the answering process's own port —
 * the same reason `diff.test.ts` pins it, and what lets this file compare every
 * body byte for byte with no normalizer.
 */
const VIEWER_PUBLIC_URL = 'http://viewer.parity.invalid';

// ---------------------------------------------------------------------------
// The table
// ---------------------------------------------------------------------------

type FuzzPhase = 'stub' | 'disk' | 'derive';

interface FuzzLeg {
  readonly name: string;
  readonly phase: FuzzPhase;
  /** The request-target, verbatim on the request line. */
  readonly target: string;
  readonly why: string;
  /**
   * What the leg's test asserts, when it is not byte equality.
   *
   * - `status` — the two bodies legitimately differ (the identity payload).
   * - `traversal-safety` — the two sides refuse a traversal-shaped target with
   *   *different* refusals; what is asserted is that both refuse, and that
   *   neither serves a 2xx. See {@link TRAVERSAL_LEGS}.
   *
   * A leg named by a `waivers.ts` entry needs none of these: the waiver itself
   * says what to assert.
   */
  readonly compare?: 'status' | 'traversal-safety';
}

interface QuerySeed {
  readonly suffix: string;
  readonly query: string;
  readonly why: string;
}

const replayTarget = (id: string, query: string): string => `/v1/games/${id}/replay.json${query}`;
const boardTarget = (id: string, query: string): string => `/v1/games/${id}/board.json${query}`;
const eventsTarget = (id: string, query: string): string => `/v1/games/${id}/events.json${query}`;

const nines = (count: number): string => '9'.repeat(count);

/**
 * `after_turn` and `limit` on `replay.json` (`_replay_query`, `:1555`).
 *
 * The block is dense on `int()` rather than on the range check on purpose: the
 * range check is two comparisons and the parser is where the two languages'
 * defaults disagree — `Number('1_0')` is `NaN`, `Number('')` is `0`,
 * `Number(' 5 ')` is `5`, and exactly one of those three matches Python.
 */
const REPLAY_SEEDS: ReadonlyArray<QuerySeed> = [
  { suffix: 'no-query', query: '', why: 'both parameters default (0, 250)' },
  { suffix: 'bare-question-mark', query: '?', why: 'a bare ? is NO query: urlsplit reports ""' },
  { suffix: 'after-0', query: '?after_turn=0', why: 'the floor' },
  { suffix: 'after-1', query: '?after_turn=1', why: 'the ordinary case' },
  { suffix: 'after-negative-1', query: '?after_turn=-1', why: 'after_turn < 0 is the range 400' },
  { suffix: 'after-negative-0', query: '?after_turn=-0', why: 'int("-0") is 0, which is in range' },
  { suffix: 'after-plus-1', query: '?after_turn=+1', why: '"+" is a SPACE before unquote: " 1"' },
  { suffix: 'after-encoded-plus-1', query: '?after_turn=%2B1', why: '%2B survives as "+1"' },
  { suffix: 'after-leading-space', query: '?after_turn=%201', why: 'int() strips an ASCII space' },
  { suffix: 'after-trailing-space', query: '?after_turn=1%20', why: 'and strips it on the right' },
  { suffix: 'after-leading-zero', query: '?after_turn=01', why: 'int("01") is 1, not an error' },
  { suffix: 'after-underscore', query: '?after_turn=1_0', why: 'PEP 515 separators parse: 10' },
  { suffix: 'after-underscore-leading', query: '?after_turn=_1', why: 'a leading _ does not' },
  { suffix: 'after-underscore-trailing', query: '?after_turn=1_', why: 'nor a trailing one' },
  { suffix: 'after-underscore-double', query: '?after_turn=1__0', why: 'nor a doubled one' },
  { suffix: 'after-float', query: '?after_turn=1.0', why: 'int() is not float()' },
  { suffix: 'after-exponent', query: '?after_turn=1e3', why: 'and not Number()' },
  { suffix: 'after-hex', query: '?after_turn=0x10', why: 'int(x) without a base is decimal only' },
  { suffix: 'after-arabic-indic', query: '?after_turn=%D9%A1%D9%A2', why: 'Unicode Nd parses: 12' },
  { suffix: 'after-fullwidth', query: '?after_turn=%EF%BC%90%EF%BC%91', why: 'fullwidth 01 is 1' },
  { suffix: 'after-thai-digit', query: '?after_turn=%E0%B9%95', why: 'Thai digit 5' },
  { suffix: 'after-fs-prefix', query: '?after_turn=%1C5', why: 'U+001C: neither parser strips it' },
  {
    suffix: 'after-nel-prefix',
    query: '?after_turn=%C2%855',
    why: 'U+0085: int() strips it and String.trim() does not — was a divergence, now pinned',
  },
  {
    suffix: 'after-bom-prefix',
    query: '?after_turn=%EF%BB%BF5',
    why: 'U+FEFF: String.trim() strips it and int() does not — the same divergence, mirrored',
  },
  { suffix: 'after-vtab-prefix', query: '?after_turn=%0B5', why: 'U+000B: both strip it' },
  { suffix: 'after-nbsp-prefix', query: '?after_turn=%C2%A05', why: 'U+00A0: both strip it' },
  { suffix: 'after-line-sep', query: '?after_turn=%E2%80%A85', why: 'U+2028: both strip it' },
  { suffix: 'after-mongolian-sep', query: '?after_turn=%E1%A0%8E5', why: 'U+180E: neither does' },
  { suffix: 'after-ideographic-space', query: '?after_turn=%E3%80%805', why: 'U+3000: both do' },
  { suffix: 'after-empty', query: '?after_turn=', why: 'keep_blank_values keeps ""; int("") raises' },
  { suffix: 'after-no-equals', query: '?after_turn', why: 'a key with no = is a blank value' },
  { suffix: 'after-repeated', query: '?after_turn=1&after_turn=1', why: 'len(items) != 1' },
  { suffix: 'after-and-limit', query: '?after_turn=1&limit=250', why: 'both keys, both legal' },
  { suffix: 'limit-0', query: '?limit=0', why: 'below the floor' },
  { suffix: 'limit-1', query: '?limit=1', why: 'the floor' },
  { suffix: 'limit-250', query: '?limit=250', why: 'the ceiling' },
  { suffix: 'limit-251', query: '?limit=251', why: 'one past it' },
  { suffix: 'limit-negative', query: '?limit=-1', why: 'negative is out of range, not an error' },
  { suffix: 'limit-exponent', query: '?limit=1e3', why: 'int() again' },
  { suffix: 'limit-spaces', query: '?limit=%20250%20', why: 'stripped on both sides' },
  { suffix: 'limit-then-after', query: '?limit=250&after_turn=0', why: 'urlencode order is fixed' },
  { suffix: 'limit-repeated', query: '?limit=250&limit=1', why: 'a duplicate limit' },
  { suffix: 'unknown-key-only', query: '?foo=1', why: 'an unknown key alone' },
  { suffix: 'unknown-key-mixed', query: '?after_turn=1&foo=2', why: 'mixed with a valid one' },
  { suffix: 'unknown-key-cased', query: '?AFTER_TURN=5', why: 'keys are case-sensitive' },
  { suffix: 'encoded-key', query: '?after%5Fturn=5', why: 'the KEY is percent-decoded too' },
  { suffix: 'plus-in-key', query: '?after+turn=1', why: '"+" in a key is a space: unknown key' },
  { suffix: 'empty-fields', query: '?&&', why: 'empty fields are skipped, not blank keys' },
  { suffix: 'empty-key', query: '?=5', why: 'an empty key is a key' },
  { suffix: 'semicolon-separator', query: '?after_turn=1;limit=2', why: '";" is not a separator' },
  { suffix: 'encoded-ampersand', query: '?after_turn=1%26limit=2', why: '%26 is not one either' },
  { suffix: 'double-equals', query: '?after_turn==1', why: 'split("=", 1): the value is "=1"' },
  { suffix: 'trailing-amp', query: '?after_turn=1&', why: 'a trailing & is an empty field' },
  { suffix: 'leading-amp', query: '?&after_turn=1', why: 'and so is a leading one' },
  { suffix: 'limit-blank-with-after', query: '?after_turn=1&limit=', why: 'one good, one blank' },
  { suffix: 'after-2-31', query: '?after_turn=2147483648', why: '2^31 is not a boundary in Python' },
  { suffix: 'after-2-31-minus-1', query: '?after_turn=2147483647', why: 'nor is 2^31-1' },
  { suffix: 'after-huge-32-digits', query: `?after_turn=${nines(32)}`, why: 'an unbounded int' },
  {
    suffix: 'after-2-53-plus-1',
    query: '?after_turn=9007199254740993',
    why: 'no double spells it — and with no derivation behind the route, nothing notices',
  },
  {
    suffix: 'after-4300-digits',
    query: `?after_turn=${nines(4300)}`,
    why: "CPython's int-from-string digit cap, exactly at it",
  },
  {
    suffix: 'after-4301-digits',
    query: `?after_turn=${nines(4301)}`,
    why: 'one digit past the cap: a ValueError there, and now a refusal here too',
  },
  {
    suffix: 'after-5001-zeros',
    query: `?after_turn=${'0'.repeat(5000)}1`,
    why: 'the cap counts digit characters, not magnitude: leading zeros are over it',
  },
  { suffix: 'after-percent-truncated', query: '?after_turn=%', why: 'a lone % stays a "%"' },
  { suffix: 'after-percent-one-hex', query: '?after_turn=%2', why: 'and so does "%2"' },
  { suffix: 'after-percent-non-hex', query: '?after_turn=%zz', why: 'and "%zz"' },
  { suffix: 'after-nul', query: '?after_turn=5%00', why: 'a NUL byte is not whitespace' },
  { suffix: 'after-invalid-utf8', query: '?after_turn=%FF%FE5', why: 'errors="replace", never a 500' },
];

/** `turn` on `board.json` (`_board_query`, `:1800`). */
const BOARD_SEEDS: ReadonlyArray<QuerySeed> = [
  { suffix: 'no-query', query: '', why: 'turn is REQUIRED: no query at all is the shape 400' },
  { suffix: 'bare-question-mark', query: '?', why: 'a bare ? is still no query' },
  { suffix: 'turn-1', query: '?turn=1', why: 'the floor' },
  { suffix: 'turn-0', query: '?turn=0', why: 'turn <= 0 is the range 400' },
  { suffix: 'turn-negative', query: '?turn=-1', why: 'and so is -1' },
  { suffix: 'turn-plus', query: '?turn=+1', why: '"+" is a space' },
  { suffix: 'turn-leading-zeros', query: '?turn=0001', why: 'int("0001") is 1' },
  { suffix: 'turn-underscore', query: '?turn=1_0', why: '10' },
  { suffix: 'turn-float', query: '?turn=1.0', why: 'the int() 400' },
  { suffix: 'turn-exponent', query: '?turn=1e3', why: 'the int() 400' },
  { suffix: 'turn-arabic-indic', query: '?turn=%D9%A1', why: 'a percent-encoded Unicode Nd is 1' },
  { suffix: 'turn-repeated', query: '?turn=1&turn=1', why: 'a repeated turn' },
  { suffix: 'turn-extra-key', query: '?turn=1&x=2', why: 'the key SET is tested, not the keys' },
  { suffix: 'turn-bad-plus-extra', query: '?turn=abc&x=2', why: 'shape 400 outranks the int() 400' },
  { suffix: 'turn-missing-other-key', query: '?x=2', why: 'a query without turn' },
  { suffix: 'turn-empty', query: '?turn=', why: 'a blank value is the int() 400' },
  { suffix: 'turn-no-equals', query: '?turn', why: 'no =, blank value' },
  { suffix: 'turn-2-53-plus-1', query: '?turn=9007199254740993', why: '2^53+1 misses the lookup' },
  { suffix: 'turn-huge-40-digits', query: `?turn=${nines(40)}`, why: 'unbounded, and still > 0' },
  { suffix: 'turn-spaces', query: '?turn=%20%201%20', why: 'stripped on both sides' },
  { suffix: 'turn-us-prefix', query: '?turn=%1F1', why: 'U+001F is not int() whitespace' },
  { suffix: 'turn-nel-prefix', query: '?turn=%C2%851', why: 'U+0085 is, on this route too' },
  { suffix: 'turn-bom-prefix', query: '?turn=%EF%BB%BF1', why: 'and U+FEFF is not, on this route too' },
  { suffix: 'turn-question-in-value', query: '?turn=1?turn=2', why: 'only the FIRST ? splits' },
];

/** Where the query gate sits relative to every other gate in `do_GET`. */
const SHAPE_LEGS: ReadonlyArray<Omit<FuzzLeg, 'phase'>> = [
  {
    name: 'events-no-query',
    target: eventsTarget(ABSENT_GAME_ID, ''),
    why: 'the control: events with no query reaches the fallback',
  },
  {
    name: 'events-bare-question-mark',
    target: eventsTarget(ABSENT_GAME_ID, '?'),
    why: 'a bare ? must NOT trip the "no query parameters" 400',
  },
  {
    name: 'events-unknown-query',
    target: eventsTarget(ABSENT_GAME_ID, '?x=1'),
    why: 'any query at all is a 400 from the handler',
  },
  {
    name: 'events-turn-query',
    target: eventsTarget(ABSENT_GAME_ID, '?turn=1'),
    why: 'even a query the sibling routes accept',
  },
  {
    name: 'events-empty-fields-query',
    target: eventsTarget(ABSENT_GAME_ID, '?&&'),
    why: 'parse_qs would yield {} but `if query:` tests the STRING',
  },
  { name: 'index-bare-question-mark', target: '/v1/games?', why: 'the index with a bare ?' },
  { name: 'index-unknown-query', target: '/v1/games?x=1', why: 'the index takes no query' },
  { name: 'index-empty-fields', target: '/v1/games?&&', why: 'a non-empty query that parses to {}' },
  { name: 'index-fragment-query', target: '/v1/games#?x=1', why: 'the fragment is split off first' },
  {
    name: 'health-bare-question-mark',
    target: '/health?',
    compare: 'status',
    why: 'health with a bare ? — the payload is per-process, so only the status is compared',
  },
  { name: 'health-unknown-query', target: '/health?x=1', why: 'health takes no query' },
  {
    name: 'status-bare-question-mark',
    target: `/v1/games/${ABSENT_GAME_ID}/status?`,
    why: 'a viewer route with a bare ? has no query',
  },
  {
    name: 'status-unknown-query',
    target: `/v1/games/${ABSENT_GAME_ID}/status?x=1`,
    why: 'the viewer-route query 400',
  },
  {
    name: 'nonexistent-suffix-query',
    target: `/v1/games/${ABSENT_GAME_ID}/nonsense?x=1`,
    why: 'the query gate precedes the final 404',
  },
  {
    name: 'short-id-with-query',
    target: `/v1/games/${SHORT_GAME_ID}/status?x=1`,
    why: 'the id gate precedes the query gate: 404, not 400',
  },
  {
    name: 'frame-latest-query',
    target: `/v1/games/${ABSENT_GAME_ID}/frames/latest.png?x=1`,
    why: 'the binary routes take no query either',
  },
  {
    name: 'replay-trailing-slash-query',
    target: `/v1/games/${ABSENT_GAME_ID}/replay.json/?after_turn=1`,
    why: 'strip("/") routes it, and the upstream path is rebuilt WITHOUT the slash',
  },
  {
    name: 'replay-leading-double-slash-query',
    target: `//v1/games/${ABSENT_GAME_ID}/replay.json?after_turn=1`,
    why: 'the gh-87389 collapse, with a query behind it',
  },
  {
    name: 'replay-fragment-after-query',
    target: `/v1/games/${ABSENT_GAME_ID}/replay.json?after_turn=1#zzz`,
    why: 'the fragment is dropped and the query survives',
  },
  {
    name: 'replay-fragment-before-query',
    target: `/v1/games/${ABSENT_GAME_ID}/replay.json#?after_turn=zz`,
    why: 'a fragment BEFORE the ? means there is no query at all',
  },
  {
    name: 'board-double-question-mark',
    target: `/v1/games/${ABSENT_GAME_ID}/board.json??turn=1`,
    why: 'the second ? belongs to the query string, so turn is "?turn=1"',
  },
];

/** The fixture with real autosaves: the loaders actually run. */
const DERIVE_LEGS: ReadonlyArray<Omit<FuzzLeg, 'phase'>> = [
  {
    name: 'derive-replay-after-0',
    target: replayTarget(VALID_GAME_ID, '?after_turn=0'),
    why: 'a real derived replay page, byte for byte',
  },
  {
    name: 'derive-replay-after-1',
    target: replayTarget(VALID_GAME_ID, '?after_turn=1'),
    why: 'one turn consumed',
  },
  {
    name: 'derive-replay-limit-1',
    target: replayTarget(VALID_GAME_ID, '?after_turn=0&limit=1'),
    why: 'limit truncates the page and moves next_after_turn',
  },
  {
    name: 'derive-replay-after-2-53-minus-1',
    target: replayTarget(VALID_GAME_ID, '?after_turn=9007199254740991'),
    why: 'the largest after_turn a double can spell',
  },
  {
    name: 'derive-replay-after-2-53-plus-1',
    target: replayTarget(VALID_GAME_ID, '?after_turn=9007199254740993'),
    // The regression pin for the saturation finding: the loader echoes
    // `after_turn` into `next_after_turn`, so a `number` anywhere on the path
    // from the query parser to the bridge's argv changes this body.
    why: 'one past it: next_after_turn echoes the digits the client sent, on both sides',
  },
  {
    name: 'derive-board-turn-1',
    target: boardTarget(VALID_GAME_ID, '?turn=1'),
    why: 'a real board snapshot',
  },
  {
    name: 'derive-board-turn-2-53-plus-1',
    target: boardTarget(VALID_GAME_ID, '?turn=9007199254740993'),
    why: 'the same width on the board route: exact on both sides, and both miss the lookup',
  },
  {
    name: 'derive-board-turn-huge',
    target: boardTarget(VALID_GAME_ID, `?turn=${nines(40)}`),
    why: 'and stays invisible at forty digits',
  },
  {
    name: 'derive-events-no-query',
    target: eventsTarget(VALID_GAME_ID, ''),
    why: 'events derives too, and accepts no query',
  },
];

const fromSeeds = (
  phase: FuzzPhase,
  prefix: string,
  seeds: ReadonlyArray<QuerySeed>,
  build: (query: string) => string,
): ReadonlyArray<FuzzLeg> =>
  seeds.map((seed) => ({
    name: `${phase}-${prefix}-${seed.suffix}`,
    phase,
    target: build(seed.query),
    why: seed.why,
  }));

const proxyPhaseLegs = (phase: 'stub' | 'disk'): ReadonlyArray<FuzzLeg> => [
  ...fromSeeds(phase, 'replay', REPLAY_SEEDS, (query) => replayTarget(ABSENT_GAME_ID, query)),
  ...fromSeeds(phase, 'board', BOARD_SEEDS, (query) => boardTarget(ABSENT_GAME_ID, query)),
  ...SHAPE_LEGS.map((leg): FuzzLeg => ({ ...leg, phase, name: `${phase}-${leg.name}` })),
];

// ---------------------------------------------------------------------------
// The runtime boundary: legs that exist to be waived
// ---------------------------------------------------------------------------

/**
 * The four divergences `waivers.ts` accepts at the two runtimes' HTTP layers.
 *
 * These are legs like any other — replayed on both sides, in the `disk` phase,
 * with the same client — and the only thing that differs is what their test
 * asserts: not equality, which is impossible, but the **measured signature
 * pair**, which fails if either side moves and fails if the two ever agree.
 * Without them each waiver would be a paragraph nobody re-measures.
 *
 * The `disk` phase, and not `stub`, because a relaying upstream would answer
 * the two legs whose query *parses* on one side (`turn=1`, `turn=5`) with its
 * own 200 on both sides and hide half the pair; against a refused upstream the
 * request reaches the archive and the absent game answers `404`, which is the
 * TypeScript behavior the waiver pins.
 *
 * Every byte here is deliberate.  `\u00d9\u00a1` is written as two latin-1
 * characters because `wire-client.ts` writes the request line with
 * `latin1`, so those are the raw bytes `D9 A1` on the socket — which is UTF-8
 * for `U+0661` and not valid latin-1 digits.  `\u0085` is one raw byte, `85`,
 * for the same reason.
 */
const BOUNDARY_LEGS: ReadonlyArray<Omit<FuzzLeg, 'phase'>> = [
  {
    name: 'disk-boundary-raw-arabic-indic-digit',
    target: `${boardTarget(ABSENT_GAME_ID, '?turn=')}\u00d9\u00a1`,
    why: 'raw bytes D9 A1: UTF-8 U+0661 to Bun, two latin-1 letters to CPython',
  },
  {
    name: 'disk-boundary-request-line-16k',
    target: replayTarget(ABSENT_GAME_ID, `?after_turn=0&limit=250&pad=${'p'.repeat(16 * 1024)}`),
    why: "a request line past Bun's ~16 KiB cap and inside CPython's 64 KiB one",
  },
  {
    name: 'disk-boundary-cpython-nel-split',
    target: `${boardTarget(ABSENT_GAME_ID, '?turn=')}\u00855`,
    why: 'a raw 85 byte, which CPython\'s request-line splitter treats as whitespace',
  },
  {
    name: 'disk-boundary-ucd-skew-11de0',
    target: boardTarget(ABSENT_GAME_ID, '?turn=%F0%91%B7%A05'),
    why: "U+11DE0: a digit in Bun's Unicode table, unassigned in CPython 3.14.6's",
  },
  {
    name: 'disk-boundary-ucd-skew-11de9',
    target: boardTarget(ABSENT_GAME_ID, '?turn=%F0%91%B7%A9'),
    why: 'and the other end of the same ten-code-point block',
  },
];

// ---------------------------------------------------------------------------
// The traversal corpus
// ---------------------------------------------------------------------------

/**
 * Escape-shaped targets, and the promise that none of them escapes.
 *
 * Bun resolves `..`, `%2e%2e` and `\\` in the request target *before* dispatch
 * and `urlsplit` does not, so this family is where a normalization difference
 * could turn into a route the client never named.  Every leg here is an
 * ordinary equality leg — both gateways answer the same `404` to all of them —
 * and the corpus-wide test additionally asserts the property that matters on
 * its own terms: neither side ever answers 2xx to one.
 *
 * The corpus is chosen to cover the escapes that would matter if one worked:
 * up and out of `/v1/games` into `/health`, an encoded separator inside the
 * game id, `..` inside the archive's frame path, and a `..` sequence that names
 * `/etc/passwd`.
 */
const TRAVERSAL_LEGS: ReadonlyArray<Omit<FuzzLeg, 'phase'>> = [
  {
    name: 'disk-traversal-up-two',
    target: `/v1/games/${ABSENT_GAME_ID}/../../health`,
    why: 'the plainest escape: two levels up lands on /health, or should not',
  },
  {
    name: 'disk-traversal-encoded-up',
    target: `/v1/games/${ABSENT_GAME_ID}/%2e%2e/%2e%2e/health`,
    why: 'the same, percent-encoded, which the WHATWG parser also resolves',
  },
  {
    name: 'disk-traversal-encoded-slash',
    target: `/v1/games/${ABSENT_GAME_ID}%2f..%2f..%2fhealth`,
    why: 'an encoded separator inside the game id: %2f must never become a path boundary',
  },
  {
    name: 'disk-traversal-backslash-up',
    target: `/v1/games/${ABSENT_GAME_ID}\\..\\..\\health`,
    why: 'backslashes as separators, which Bun rewrites and CPython does not',
  },
  {
    name: 'disk-traversal-archive-escape',
    target: `/v1/games/${ABSENT_GAME_ID}/frames/../../../etc/passwd`,
    why: 'the archive path is where a traversal would reach a file, so it gets its own leg',
  },
  {
    name: 'disk-traversal-frames-dotdot',
    target: `/v1/games/${ABSENT_GAME_ID}/frames/..%2f..%2fhealth`,
    why: 'the same, with the separator encoded so only a decoding router would resolve it',
  },
  {
    name: 'disk-traversal-dotdot-game-id',
    target: '/v1/games/..%2f..%2fetc%2fpasswd/board.json?turn=1',
    why: 'the traversal in the id itself: GAME_ID_RE is what refuses this',
  },
  {
    name: 'disk-traversal-double-slash',
    target: `//v1/games/${ABSENT_GAME_ID}/board.json?turn=1`,
    why: 'the gh-87389 leading-slash collapse, which both sides do identically',
  },
  {
    name: 'disk-traversal-backslash-separator',
    target: `/v1/games/${ABSENT_GAME_ID}/board.json\\?turn=1`,
    why: 'a backslash where the ? should be; both sides refuse it',
    compare: 'traversal-safety',
  },
  {
    name: 'disk-traversal-dot-segment',
    target: `/v1/games/${ABSENT_GAME_ID}/x/../board.json?turn=1`,
    why: 'a self-cancelling dot segment: 400 there, 404 here, and no other route reached',
    compare: 'traversal-safety',
  },
  {
    name: 'disk-traversal-percent-2e-segment',
    target: `/v1/games/${ABSENT_GAME_ID}/x/%2e%2e/board.json?turn=1`,
    why: 'the encoded spelling of the same segment',
    compare: 'traversal-safety',
  },
  {
    name: 'disk-traversal-percent-2e-upper',
    target: `/v1/games/${ABSENT_GAME_ID}/x/%2E%2E/board.json?turn=1`,
    why: 'and the upper-case spelling, since percent-decoding is case-insensitive',
    compare: 'traversal-safety',
  },
  {
    name: 'disk-traversal-trailing-dot',
    target: `/v1/games/${ABSENT_GAME_ID}/board.json/.?turn=1`,
    why: 'a single dot segment after the route, which normalizes to the route itself',
    compare: 'traversal-safety',
  },
  {
    name: 'disk-traversal-cross-game-dot-segment',
    target: `/v1/games/${ABSENT_GAME_ID}/../${VALID_GAME_ID}/board.json?turn=1`,
    why: 'the raw A/../B target stays raw and both gateways refuse it byte-for-byte',
  },
];

/** Every leg, in replay order. */
export const QUERY_FUZZ_LEGS: ReadonlyArray<FuzzLeg> = [
  ...proxyPhaseLegs('stub'),
  ...proxyPhaseLegs('disk'),
  ...BOUNDARY_LEGS.map((leg): FuzzLeg => ({ ...leg, phase: 'disk' })),
  ...TRAVERSAL_LEGS.map((leg): FuzzLeg => ({ ...leg, phase: 'disk' })),
  ...DERIVE_LEGS.map((leg): FuzzLeg => ({ ...leg, phase: 'derive' })),
];

// ---------------------------------------------------------------------------
// One side of one leg
// ---------------------------------------------------------------------------

/** What a leg compares.  Everything here is a byte off the wire. */
interface LegSide {
  readonly tag: string;
  readonly status: number | null;
  readonly contentType: string | null;
  readonly cacheControl: string | null;
  /** Latin-1, so a PNG or a torn UTF-8 sequence compares honestly. */
  readonly body: string;
  /** The targets this gateway forwarded upstream while the leg ran. */
  readonly forwarded: ReadonlyArray<string>;
  /**
   * The outcome this projection was built from, kept whole.
   *
   * {@link comparable} never reads it — a waived leg does, because
   * `waiverSignature` is the one comparison this file must not re-implement.
   */
  readonly outcome: WireOutcome;
}

const sideOf = (outcome: WireOutcome, forwarded: ReadonlyArray<string>): LegSide =>
  isWireResponse(outcome)
    ? {
        tag: 'Response',
        status: outcome.status,
        contentType: outcome.headers.get('content-type'),
        cacheControl: outcome.headers.get('cache-control'),
        body: Buffer.from(outcome.bodyBytes).toString('latin1'),
        forwarded,
        outcome,
      }
    : {
        tag: outcome._tag,
        status: null,
        contentType: null,
        cacheControl: null,
        body: '',
        forwarded,
        outcome,
      };

/** The comparable projection: `compare: 'status'` drops the body from it. */
const comparable = (leg: FuzzLeg, side: LegSide): Readonly<Record<string, unknown>> => ({
  tag: side.tag,
  status: side.status,
  contentType: side.contentType,
  cacheControl: side.cacheControl,
  forwarded: side.forwarded,
  ...(leg.compare === 'status' ? {} : { body: side.body }),
});

/**
 * `refused` for the two statuses a traversal-shaped target may answer with.
 *
 * Deliberately not "not 2xx": a `500`, a `301` or a closed connection would all
 * pass that test, and none of them is the answer a router that refused to
 * traverse gives.  Anything else renders as itself, so the failure message
 * names the status that broke the promise.
 */
const refusalKind = (status: number | null): string =>
  status === 400 || status === 404 ? 'refused' : `served ${String(status)}`;

interface LegResult {
  readonly python: LegSide;
  readonly typescript: LegSide;
}

interface PhaseReport {
  readonly results: ReadonlyMap<string, LegResult>;
  readonly pids: ReadonlyArray<number>;
  readonly stop: StopReport;
}

const LEG_TIMEOUT_MS = 60_000;

const runPhase = async (
  phase: FuzzPhase,
  legs: ReadonlyArray<FuzzLeg>,
): Promise<PhaseReport> => {
  const stub: StubHandle | null = phase === 'stub' ? makeStub('ok-json') : null;
  const pair: GatewayPair = unwrapPair(
    await bootGatewayPair({
      runsRoot: PARITY_RUNS_ROOT,
      serviceUrl: stub === null ? REFUSED_UPSTREAM_URL : stub.origin,
      scenario: `qfuzz-${phase}`,
      viewerPublicUrl: VIEWER_PUBLIC_URL,
    }),
  );
  const results = await legs.reduce<Promise<ReadonlyMap<string, LegResult>>>(
    async (previous, leg) => {
      const collected = await previous;
      const sides = await pair.both.reduce<Promise<ReadonlyArray<readonly [string, LegSide]>>>(
        async (pending, gateway) => {
          const done = await pending;
          const before = stub === null ? 0 : stub.requests().length;
          const outcome = await wireGet(gateway.origin, leg.target, { timeoutMs: LEG_TIMEOUT_MS });
          const forwarded =
            stub === null
              ? []
              : stub
                  .requests()
                  .slice(before)
                  .map((request) => request.target);
          return [...done, [gateway.impl, sideOf(outcome, forwarded)] as const];
        },
        Promise.resolve([]),
      );
      const python = sides.find(([impl]) => impl === 'python')?.[1];
      const typescript = sides.find(([impl]) => impl === 'typescript')?.[1];
      return python === undefined || typescript === undefined
        ? collected
        : new Map(collected).set(leg.name, { python, typescript });
    },
    Promise.resolve(new Map()),
  );
  const pids = pair.both.map((gateway) => gateway.pid);
  const stop = await pair.stop();
  pair.cleanup();
  if (stub !== null) await stub.close();
  return { results, pids, stop };
};

// ---------------------------------------------------------------------------
// The rig
// ---------------------------------------------------------------------------

/** One cell, written once by `beforeAll`; `bun:test` registers tests first. */
const state: { reports: ReadonlyMap<FuzzPhase, PhaseReport> } = { reports: new Map() };

const PHASES: ReadonlyArray<FuzzPhase> = ['stub', 'disk', 'derive'];

const reportFor = (phase: FuzzPhase): PhaseReport => {
  const report = state.reports.get(phase);
  if (report === undefined) throw new Error(`phase ${phase} never ran`);
  return report;
};

beforeAll(async () => {
  const reports = await PHASES.reduce<Promise<ReadonlyMap<FuzzPhase, PhaseReport>>>(
    async (previous, phase) => {
      const collected = await previous;
      const legs = QUERY_FUZZ_LEGS.filter((leg) => leg.phase === phase);
      return new Map(collected).set(phase, await runPhase(phase, legs));
    },
    Promise.resolve(new Map()),
  );
  state.reports = reports;
}, 300_000);

afterAll(async () => {
  // Unconditional: a `beforeAll` that threw halfway is exactly when a gateway
  // outlives the suite, and the registry is populated at spawn, not at health.
  await killAllBooted();
});

describe('query-fuzz parity', () => {
  PHASES.forEach((phase) => {
    describe(phase, () => {
      test('every leg ran on both sides', () => {
        expect(reportFor(phase).results.size).toBe(
          QUERY_FUZZ_LEGS.filter((leg) => leg.phase === phase).length,
        );
      });

      QUERY_FUZZ_LEGS.filter((leg) => leg.phase === phase).forEach((leg) => {
        test(`${leg.name} — ${leg.why}`, () => {
          const result = reportFor(phase).results.get(leg.name);
          expect(result).toBeDefined();
          if (result === undefined) return;
          const waiver = waiverFor('query-fuzz', leg.name);
          if (waiver !== undefined) {
            // Not equality — impossible here — but the measured pair, which
            // fails if either side moves and fails if the two ever agree.
            expect(
              checkWaiver(waiver, result.python.outcome, result.typescript.outcome, leg.name),
            ).toEqual({ _tag: 'Honored', leg: leg.name });
            return;
          }
          if (leg.compare === 'traversal-safety') {
            expect({
              leg: leg.name,
              refusals: [result.python.status, result.typescript.status].map(refusalKind),
            }).toEqual({ leg: leg.name, refusals: ['refused', 'refused'] });
            return;
          }
          expect(comparable(leg, result.typescript)).toEqual(comparable(leg, result.python));
        });
      });

      test('SIGINT is a clean exit that removes both ready files', () => {
        const stop = reportFor(phase).stop;
        expect({ exit: stop.exitCodes, readyRemoved: stop.readyFilesRemoved }).toEqual({
          exit: { python: 0, typescript: 0 },
          readyRemoved: { python: true, typescript: true },
        });
      });

      test('no process this phase spawned is still alive', () => {
        const report = reportFor(phase);
        expect({ orphans: report.stop.orphans, alive: aliveProcesses(report.pids) }).toEqual({
          orphans: [],
          alive: [],
        });
      });
    });
  });

  test('a refused query opens no upstream socket on either side', () => {
    const report = reportFor('stub');
    const refusalsThatProxied = QUERY_FUZZ_LEGS.filter((leg) => leg.phase === 'stub').flatMap(
      (leg) => {
        const result = report.results.get(leg.name);
        if (result === undefined) return [];
        const refused = result.python.status === 400;
        const proxied = result.python.forwarded.length + result.typescript.forwarded.length;
        return refused && proxied > 0 ? [leg.name] : [];
      },
    );
    expect(refusalsThatProxied).toEqual([]);
  });

  test('every waived divergence has a leg in this file that exercises it', () => {
    // The other half of the same rule: a waiver with no leg is a paragraph.
    const legs = new Set(QUERY_FUZZ_LEGS.map((leg) => leg.name));
    const orphaned = waiversIn('query-fuzz').flatMap((waiver) =>
      waiver.legs.filter((leg) => !legs.has(leg)).map((leg) => `${waiver.id}/${leg}`),
    );
    expect(orphaned).toEqual([]);
  });

  test('no query-fuzz waiver claims a divergence that is not one', () => {
    expect(
      waiversIn('query-fuzz')
        .filter((waiver) => !waiverStillNeeded(waiver))
        .map((waiver) => waiver.id),
    ).toEqual([]);
  });

  /** Every traversal-shaped target, including raw A/../B, is refused by both sides. */
  test('no traversal-shaped target is served by either gateway', () => {
    const report = reportFor('disk');
    const served = QUERY_FUZZ_LEGS.filter(
      (leg) => leg.phase === 'disk' && leg.name.startsWith('disk-traversal-'),
    ).flatMap((leg) => {
      const result = report.results.get(leg.name);
      if (result === undefined) return [`${leg.name}/did-not-run`];
      return [
        ...(refusalKind(result.python.status) === 'refused' ? [] : [`${leg.name}/python`]),
        ...(refusalKind(result.typescript.status) === 'refused' ? [] : [`${leg.name}/typescript`]),
      ];
    });
    expect(served).toEqual([]);
  });
});
