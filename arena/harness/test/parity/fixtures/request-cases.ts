/**
 * The bad/boundary request table the parity rig replays against **both**
 * gateways.
 *
 * Every row below was *measured*, not reasoned about: an ephemeral CPython
 * `agent_eval.replay_gateway` was started on port 0 over `./runs`, with
 * `--service-url http://192.0.2.1:1` (RFC 5737, unroutable) and
 * `--upstream-timeout-s 1` so every route falls through to the disk archive,
 * and each target was sent verbatim on a raw socket.  The `expected` field is
 * what CPython answered.  See `./README.md` for the transcript and for how to
 * re-measure.
 *
 * ## Why the targets are strings and not URLs
 *
 * Four of these cannot survive a `fetch()`:
 *
 * - `#frag` — WHATWG URL parsing strips the fragment *client-side*, so the
 *   server never sees it.  CPython strips it too (`urlsplit(...).fragment`),
 *   which is why the answer is the plain index — but a rig that used `fetch`
 *   would be asserting on its own client, not on the gateway.
 * - `//v1/games` — this is the sharpest trap in the table.  CPython's
 *   `BaseHTTPRequestHandler.parse_request` collapses a *leading* run of
 *   slashes to one (`gh-87389`, an open-redirect fix) **before** `do_GET`
 *   sees `self.path`, so `//v1/games` and `///v1/games` are the games index,
 *   byte-for-byte.  Nothing in `replay_gateway.py` says so.  A port built on
 *   `new URL(request.url).pathname` keeps both slashes and 404s, and that is
 *   a real divergence, not a waiver: it is on the same wire path a browser
 *   reaches by resolving a protocol-relative link.  Interior slashes are
 *   *not* collapsed — `/v1/games/{id}//frames/0.png` is a 404.
 * - `%2F` — the path is routed **un-decoded**.  `parts[2]` keeps the literal
 *   `%2F`, which is outside `GAME_ID_RE`'s alphabet, so the id is rejected at
 *   the router rather than after a decode that could have escaped
 *   `runs_root`.  A client that normalizes `%2F` to `/` would silently test a
 *   different route.
 * - a raw byte in the id (`å`) — must reach the socket as the bytes the case
 *   spells, not as whatever a URL constructor re-encodes them to.
 *
 * So the rig must write these targets onto the socket itself, and must
 * complete each read on `Content-Length` (waiting for EOF costs a phantom 12s
 * against a keep-alive server).  {@link ParityRequestCase.rawSocketOnly} marks
 * the rows where a client library is not allowed to be in the middle.
 *
 * @module
 */

import {
  GATEWAY_PROBLEM_MESSAGES,
  GATEWAY_PROBLEM_STATUS,
  type GatewayProblemMessage,
} from '@arena/wire/gateway';

// ---------------------------------------------------------------------------
// Shape
// ---------------------------------------------------------------------------

/**
 * What both gateways must answer.
 *
 * `Problem` pins the status *and* the `{"error": ...}` string, because the
 * viewer renders that string verbatim; `Ok` pins the status and media type
 * and leaves the body to the rig's byte comparison, which is stricter than
 * anything spelled here.
 */
export type ParityExpectation =
  | {
      readonly _tag: 'Problem';
      readonly status: number;
      readonly message: GatewayProblemMessage;
    }
  | {
      readonly _tag: 'Ok';
      readonly status: 200;
      readonly contentType: 'application/json; charset=utf-8' | 'image/png';
    };

/** A problem expectation; the status comes from wire's table, never a literal. */
export const problem = (message: GatewayProblemMessage): ParityExpectation => ({
  _tag: 'Problem',
  status: GATEWAY_PROBLEM_STATUS[message],
  message,
});

/** A 200 expectation, media type pinned. */
export const ok = (
  contentType: 'application/json; charset=utf-8' | 'image/png',
): ParityExpectation => ({ _tag: 'Ok', status: 200, contentType });

/** One request replayed against both gateways. */
export interface ParityRequestCase {
  /** Stable identifier; used as the test name and as the report key. */
  readonly name: string;
  /** The request-target, exactly as it must appear on the request line. */
  readonly target: string;
  /**
   * True when a client library would rewrite {@link target} before it reached
   * the socket.  These rows must be sent by writing the request line directly.
   */
  readonly rawSocketOnly: boolean;
  /** What this row proves. */
  readonly why: string;
  /** What CPython answered when this table was measured. */
  readonly expected: ParityExpectation;
}

// ---------------------------------------------------------------------------
// The fixture ids these cases address
// ---------------------------------------------------------------------------

/** The terminal, valid, winner-bearing fixture — the archive routes' subject. */
export const VALID_GAME_ID = 'game_parity_terminal_valid_01';

/** The terminal fixture with no valid winner and three *unpaired* frames. */
export const NO_WINNER_GAME_ID = 'game_parity_terminal_nowin_02';

/** Padding to build ids of an exact length without reusing a real one. */
const repeat = (count: number): string => 'a'.repeat(count);

// ---------------------------------------------------------------------------
// Game-id boundaries
// ---------------------------------------------------------------------------

/**
 * `GAME_ID_RE` is `^[A-Za-z0-9_-]{20,80}$`, applied with `fullmatch` to the
 * third path segment before any filesystem call.
 *
 * The two 404s are **different messages**, and that is the whole point of this
 * block: `not found` means the *router* refused the id, `game not found` means
 * the router accepted it and the disk lookup missed.  A port that collapses
 * them passes a status-only comparison and still lies to the viewer.
 */
export const GAME_ID_CASES: ReadonlyArray<ParityRequestCase> = [
  {
    name: 'id-19-chars',
    target: `/v1/games/${repeat(19)}/status`,
    rawSocketOnly: false,
    why: 'one below the 20-char floor: the router rejects it, so the message is the generic one',
    expected: problem(GATEWAY_PROBLEM_MESSAGES.notFound),
  },
  {
    name: 'id-20-chars',
    target: `/v1/games/${repeat(20)}/status`,
    rawSocketOnly: false,
    why: 'exactly at the floor: the router accepts it and the disk lookup misses',
    expected: problem(GATEWAY_PROBLEM_MESSAGES.gameNotFound),
  },
  {
    name: 'id-80-chars',
    target: `/v1/games/${repeat(80)}/status`,
    rawSocketOnly: false,
    why: 'exactly at the ceiling: still routable',
    expected: problem(GATEWAY_PROBLEM_MESSAGES.gameNotFound),
  },
  {
    name: 'id-81-chars',
    target: `/v1/games/${repeat(81)}/status`,
    rawSocketOnly: false,
    why: 'one past the ceiling: back to a router rejection',
    expected: problem(GATEWAY_PROBLEM_MESSAGES.notFound),
  },
  {
    name: 'id-alphabet-dot',
    target: '/v1/games/game.parity.bad.alphabet.01/status',
    rawSocketOnly: false,
    why: '"." is outside the alphabet even though the length is legal',
    expected: problem(GATEWAY_PROBLEM_MESSAGES.notFound),
  },
  {
    name: 'id-alphabet-plus',
    target: '/v1/games/game+parity+bad+alphabet+1/status',
    rawSocketOnly: true,
    why: '"+" must arrive as "+", not as a decoded space or as %2B',
    expected: problem(GATEWAY_PROBLEM_MESSAGES.notFound),
  },
  {
    name: 'id-alphabet-non-ascii',
    target: '/v1/games/game_parity_båd_alphabet_1/status',
    rawSocketOnly: true,
    why: 'a non-ASCII byte in the id: rejected at the router, never percent-decoded first',
    expected: problem(GATEWAY_PROBLEM_MESSAGES.notFound),
  },
  {
    name: 'id-percent-encoded-slash',
    target: '/v1/games/game_parity%2Fterminal_valid/status',
    rawSocketOnly: true,
    why: 'the path is routed un-decoded, so %2F is just a character the alphabet forbids',
    expected: problem(GATEWAY_PROBLEM_MESSAGES.notFound),
  },
  {
    name: 'id-percent-encoded-slash-splits-route',
    target: `/v1/games/${VALID_GAME_ID}%2Fstatus`,
    rawSocketOnly: true,
    why: 'a real id welded to its suffix by %2F must not decode into a valid two-segment route',
    expected: problem(GATEWAY_PROBLEM_MESSAGES.notFound),
  },
  {
    name: 'id-traversal',
    target: '/v1/games/..%2F..%2Fetc%2Fpasswd_padding/status',
    rawSocketOnly: true,
    why: 'traversal is stopped by the alphabet, before any path join',
    expected: problem(GATEWAY_PROBLEM_MESSAGES.notFound),
  },
] as const;

// ---------------------------------------------------------------------------
// Frame-index boundaries
// ---------------------------------------------------------------------------

/**
 * `FRAME_INDEX_RE` is `^(?:0|[1-9][0-9]*)\.png$` — **leading zeros are
 * forbidden**, so one frame has exactly one URL.  On-disk names are the padded
 * `^([0-9]{6})\.png$` form and never appear on the wire.
 *
 * `game_parity_terminal_valid_01` ships frames `000000.png` and `000752.png`,
 * so index 0 and index 752 exist, index 1 does not, and `latest.png` is 752.
 */
export const FRAME_INDEX_CASES: ReadonlyArray<ParityRequestCase> = [
  {
    name: 'frame-zero',
    target: `/v1/games/${VALID_GAME_ID}/frames/0.png`,
    rawSocketOnly: false,
    why: 'the canonical spelling of index 0 — the alternation admits bare "0"',
    expected: ok('image/png'),
  },
  {
    name: 'frame-leading-zeros',
    target: `/v1/games/${VALID_GAME_ID}/frames/007.png`,
    rawSocketOnly: false,
    why: 'the alias for an existing frame is a router 404, not a redirect and not a hit',
    expected: problem(GATEWAY_PROBLEM_MESSAGES.notFound),
  },
  {
    name: 'frame-double-zero',
    target: `/v1/games/${VALID_GAME_ID}/frames/00.png`,
    rawSocketOnly: false,
    why: '"00" is a leading-zero alias of the frame that does exist',
    expected: problem(GATEWAY_PROBLEM_MESSAGES.notFound),
  },
  {
    name: 'frame-sparse-miss',
    target: `/v1/games/${VALID_GAME_ID}/frames/1.png`,
    rawSocketOnly: false,
    why: 'a well-formed index the archive lacks: the router passes, the lookup misses',
    expected: problem(GATEWAY_PROBLEM_MESSAGES.mapFrameDoesNotExist),
  },
  {
    name: 'frame-last',
    target: `/v1/games/${VALID_GAME_ID}/frames/752.png`,
    rawSocketOnly: false,
    why: 'frames are sparse, not dense: 752 exists with only two PNGs on disk',
    expected: ok('image/png'),
  },
  {
    name: 'frame-latest',
    target: `/v1/games/${VALID_GAME_ID}/frames/latest.png`,
    rawSocketOnly: false,
    why: 'latest is max(index), so it must be the 752 bytes and not the 0 bytes',
    expected: ok('image/png'),
  },
  {
    name: 'frame-huge',
    target: `/v1/games/${VALID_GAME_ID}/frames/99999999999999999999999999.png`,
    rawSocketOnly: false,
    why: 'Python parses an unbounded int; a port must miss the lookup rather than overflow or throw',
    expected: problem(GATEWAY_PROBLEM_MESSAGES.mapFrameDoesNotExist),
  },
  {
    name: 'frame-beyond-safe-integer',
    target: `/v1/games/${VALID_GAME_ID}/frames/9007199254740993.png`,
    rawSocketOnly: false,
    why: '2^53+1 rounds to 2^53 as a double — still a miss, and it must stay a miss',
    expected: problem(GATEWAY_PROBLEM_MESSAGES.mapFrameDoesNotExist),
  },
  {
    name: 'frame-negative',
    target: `/v1/games/${VALID_GAME_ID}/frames/-1.png`,
    rawSocketOnly: false,
    why: 'the pattern has no sign, so this never reaches the lookup',
    expected: problem(GATEWAY_PROBLEM_MESSAGES.notFound),
  },
  {
    name: 'frame-encoded-dot',
    target: `/v1/games/${VALID_GAME_ID}/frames/0%2Epng`,
    rawSocketOnly: true,
    why: 'the suffix is matched un-decoded, so %2E is not a "."',
    expected: problem(GATEWAY_PROBLEM_MESSAGES.notFound),
  },
  {
    name: 'frame-interior-double-slash',
    target: `/v1/games/${VALID_GAME_ID}//frames/0.png`,
    rawSocketOnly: true,
    why: 'only a LEADING slash run is collapsed; an interior one yields an empty segment and a 404',
    expected: problem(GATEWAY_PROBLEM_MESSAGES.notFound),
  },
  {
    name: 'frame-query-string',
    target: `/v1/games/${VALID_GAME_ID}/frames/latest.png?x=1`,
    rawSocketOnly: false,
    why: 'viewer routes reject any query at all, before the archive is touched',
    expected: problem(GATEWAY_PROBLEM_MESSAGES.viewerRouteQuery),
  },
  {
    name: 'frame-unpaired-archive',
    target: `/v1/games/${NO_WINNER_GAME_ID}/frames/591.png`,
    rawSocketOnly: false,
    why: 'a frame whose index has no savegame to pair with is still served',
    expected: ok('image/png'),
  },
  {
    name: 'frame-zero-on-sparse-archive',
    target: `/v1/games/${NO_WINNER_GAME_ID}/frames/0.png`,
    rawSocketOnly: false,
    why: 'indices are the padded file names, not positions: this archive starts at 591',
    expected: problem(GATEWAY_PROBLEM_MESSAGES.mapFrameDoesNotExist),
  },
] as const;

// ---------------------------------------------------------------------------
// Request-target shapes
// ---------------------------------------------------------------------------

/** Targets whose *shape* — not whose ids — is the thing under test. */
export const PATH_SHAPE_CASES: ReadonlyArray<ParityRequestCase> = [
  {
    name: 'index',
    target: '/v1/games',
    rawSocketOnly: false,
    why: 'the control row: the index the next two must match byte-for-byte',
    expected: ok('application/json; charset=utf-8'),
  },
  {
    name: 'index-leading-double-slash',
    target: '//v1/games',
    rawSocketOnly: true,
    why: 'CPython collapses a leading slash run before routing (gh-87389): this IS the index',
    expected: ok('application/json; charset=utf-8'),
  },
  {
    name: 'index-leading-triple-slash',
    target: '///v1/games',
    rawSocketOnly: true,
    why: 'the whole leading run collapses, not just one slash',
    expected: ok('application/json; charset=utf-8'),
  },
  {
    name: 'index-fragment',
    target: '/v1/games#frag',
    rawSocketOnly: true,
    why: 'a fragment on the request line is split off server-side, so this is still the index',
    expected: ok('application/json; charset=utf-8'),
  },
  {
    name: 'index-query',
    target: '/v1/games?x=1',
    rawSocketOnly: false,
    why: 'the index takes no query at all, not even an unknown one',
    expected: problem(GATEWAY_PROBLEM_MESSAGES.gamesIndexQuery),
  },
] as const;

// ---------------------------------------------------------------------------
// Scenario-class routes
// ---------------------------------------------------------------------------

/**
 * One archive-route probe per fixture whose *absence from the index* is the
 * headline.  A dropped row is easy to get right by accident; the direct route
 * is where the three reasons for dropping it stay distinguishable.
 */
export const SCENARIO_ROUTE_CASES: ReadonlyArray<ParityRequestCase> = [
  {
    name: 'lobby-husk-status',
    target: '/v1/games/game_parity_lobby_husk_04/status',
    rawSocketOnly: false,
    why: 'the husk is hidden from the index but its manifest still reads: non-terminal, so no archive',
    expected: problem(GATEWAY_PROBLEM_MESSAGES.terminalArchiveNotFound),
  },
  {
    name: 'interrupted-status',
    target: '/v1/games/game_parity_interrupted_03/status',
    rawSocketOnly: false,
    why: 'an interrupted run is IN the index yet has no archive — the index row is not a promise',
    expected: problem(GATEWAY_PROBLEM_MESSAGES.terminalArchiveNotFound),
  },
  {
    name: 'malformed-manifest-status',
    target: '/v1/games/game_parity_malformed_05/status',
    rawSocketOnly: false,
    why: 'unparseable manifest is 503, not 404: the run exists, the gateway cannot speak for it',
    expected: problem(GATEWAY_PROBLEM_MESSAGES.manifestUnavailable),
  },
  {
    name: 'wrong-game-id-by-directory',
    target: '/v1/games/game_parity_wrong_id_06/status',
    rawSocketOnly: false,
    why: 'the manifest claims another id, so the directory name does not resolve',
    expected: problem(GATEWAY_PROBLEM_MESSAGES.gameNotFound),
  },
  {
    name: 'wrong-game-id-by-claim',
    target: '/v1/games/game_parity_wrong_id_06_other/status',
    rawSocketOnly: false,
    why: 'and the claimed id has no directory: the mismatch is unreachable from both sides',
    expected: problem(GATEWAY_PROBLEM_MESSAGES.gameNotFound),
  },
  {
    name: 'symlinked-run-status',
    target: '/v1/games/game_parity_symlink_07/status',
    rawSocketOnly: false,
    why: 'a symlinked run directory is refused by lstat before resolve, even pointing at a valid run',
    expected: problem(GATEWAY_PROBLEM_MESSAGES.gameNotFound),
  },
  {
    name: 'terminal-valid-status',
    target: `/v1/games/${VALID_GAME_ID}/status`,
    rawSocketOnly: false,
    why: 'the one archive that answers: winner, victory record and leaderboard',
    expected: ok('application/json; charset=utf-8'),
  },
  {
    name: 'terminal-valid-frames',
    target: `/v1/games/${VALID_GAME_ID}/frames`,
    rawSocketOnly: false,
    why: 'frame 0 pairs with a savegame (turn 1, map players parsed); frame 752 does not',
    expected: ok('application/json; charset=utf-8'),
  },
  {
    name: 'terminal-no-winner-frames',
    target: `/v1/games/${NO_WINNER_GAME_ID}/frames`,
    rawSocketOnly: false,
    why: 'every frame here is unpaired: turn null, map_players empty, source_name is the PNG',
    expected: ok('application/json; charset=utf-8'),
  },
] as const;

// ---------------------------------------------------------------------------
// The table
// ---------------------------------------------------------------------------

/** Every case, in the order the rig should replay them. */
export const REQUEST_CASES: ReadonlyArray<ParityRequestCase> = [
  ...PATH_SHAPE_CASES,
  ...GAME_ID_CASES,
  ...FRAME_INDEX_CASES,
  ...SCENARIO_ROUTE_CASES,
];

/** The subset a `fetch`-based client would silently rewrite. */
export const RAW_SOCKET_CASES: ReadonlyArray<ParityRequestCase> = REQUEST_CASES.filter(
  (kase) => kase.rawSocketOnly,
);
