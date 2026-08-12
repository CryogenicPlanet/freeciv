/**
 * The viewer family: `replay.json`, `board.json`, `events.json`.
 *
 * These three routes are where the port can go wrong *quietly*, so the suite is
 * organized around the six ways it can:
 *
 * 1. **A 400 that reached the network.**  Every query refusal is asserted with
 *    the verbatim `@arena/wire` message *and* with "the upstream stub was never
 *    called and no loader ran" — Python parses the query before it opens a
 *    socket (`:1656`, `:1728`, `:1820`), and a port that validates after
 *    fetching is observationally different for a running match.
 * 2. **A 2xx that was re-serialized.**  The relay is byte-for-byte
 *    (`:1666`/`:1742`/`:1830`), so the assertion is on bytes, with an upstream
 *    body whose key order and spacing no canonical writer would reproduce.
 * 3. **The wrong fallback trigger.**  Only 404, 405 and a *provably gone*
 *    upstream reach disk.  A 500, a redirect and an oversized body must not —
 *    and each of those has its own status and message.
 * 4. **The two 8 MiB errors merged.**  Upstream oversize is 502
 *    `the upstream JSON response is too large`; a locally built body over the
 *    cap is 503 `archive JSON response is too large`.  Both are exercised in
 *    one block so the difference cannot be edited away.
 * 5. **A loader failure that leaked.**  `DerivationUnavailable` carries a
 *    `detail` for the log; the body is the catalogue message and nothing else.
 * 6. **The mutex lost.**  `replay_lock` serializes the three loaders across
 *    routes; the Python suite has no concurrency test at all, so the gated
 *    fixture here is the only thing standing between a port and a corrupted
 *    savegame cache.
 *
 * Two rigs, both owned by this file: a `mkdtemp` runs root removed in
 * `afterAll`, and an injected `fetch`.  No socket is opened, no process is
 * spawned, and the user's stack is never contacted.
 *
 * The query parsers were additionally checked against CPython 3.14.6 by a
 * differential (46 inputs including `5_0`, `+3`, `%205`, Arabic-Indic digits,
 * `%zz`, `%4` and a 22-digit turn) — zero mismatches.  The table in
 * `describe('the two query parsers')` is that differential's interesting rows,
 * frozen here so the property is checked without a Python interpreter.
 */

import { afterAll, describe, expect, test } from 'bun:test';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  CANON_UTF8,
  canonicalBytes,
  type CanonRecord,
  Gateway,
  type JsonObject,
} from '@arena/wire';
import { HttpServerResponse } from '@effect/platform';
import {
  Deferred,
  Duration,
  Effect,
  Either,
  Fiber,
  Layer,
  Logger,
  LogLevel,
  Ref,
} from 'effect';
import { MAX_PROXY_JSON_BYTES } from 'src/gateway/constants';
import { gatewayErrorFromUpstream } from 'src/gateway/errors';
import { boundedJsonResponse } from 'src/gateway/http/json';
import { type GatewayError, gatewayProblem } from 'src/gateway/errors';
import { dispatch, type RouteDecision } from 'src/gateway/http/dispatch';
import { respondGateway } from 'src/gateway/http/respond';
import { boardQuery, boardRoute } from 'src/gateway/http/routes/board';
import { eventsRoute } from 'src/gateway/http/routes/events';
import {
  canonToJson,
  derivationPlaces,
  gatewayErrorFromDerivation,
  parseQuery,
  pythonUnquote,
  replayQuery,
  replayRoute,
  toLoaderInteger,
  type ViewerRouteEffect,
  type ViewerRouteServices,
} from 'src/gateway/http/routes/replay';
import {
  DerivationArtifactsMissing,
  type DerivationError,
  type DerivationOperation,
  type DerivationRequest,
  DerivationUnavailable,
  derivationFixture,
  derivationProblem,
  derivationRequestKey,
  layerFromRunner,
  ReplayDerivationFixture,
  ReplayDerivationUnavailable,
} from 'src/gateway/services/derivation';
import { untrustedField } from 'src/gateway/public';
import { layer as runsLayer } from 'src/gateway/services/runs';
import {
  type FetchLike,
  layerTest as upstreamLayerTest,
  UpstreamBodyError,
  UpstreamRedirect as UpstreamClientHttpError,
  type UpstreamFailure,
  upstreamFailureProblem,
  UpstreamJsonTooLarge,
  UpstreamOffline as UpstreamClientUnavailable,
} from 'src/gateway/services/upstream';

// ---------------------------------------------------------------------------
// Fixtures on disk
// ---------------------------------------------------------------------------

const TERMINAL_GAME = 'game_terminal_0123456789abcd';
const LIVE_GAME = 'game_live_0123456789abcdefgh';
const ABSENT_GAME = 'game_absent_0123456789abcdef';

const SERVICE_URL = 'http://127.0.0.1:9999';

/**
 * One place row as a manifest holds it — deliberately *not* the published
 * shape: `controller_metadata` and `controller_fingerprint` are what
 * `_public_places` exists to strip, so a loader that receives them is a leak.
 */
const RAW_PLACE = {
  place: 1,
  seat_id: 'seat-a',
  player_name: 'Alice',
  player_color: '#ff0000',
  controller: 'agent',
  joined: true,
  controller_metadata: { model: 'claude-x', fingerprint: 'secret' },
  controller_fingerprint: 'secret',
};

const manifest = (gameId: string, state: string): JsonObject => ({
  game_id: gameId,
  state,
  resolved_places: [RAW_PLACE],
});

/** A runs root owned by this file, removed in `afterAll`. */
const runsRoot = ((): string => {
  const root = mkdtempSync(join(tmpdir(), 'routes-replay-'));
  [
    [TERMINAL_GAME, 'completed'],
    [LIVE_GAME, 'running'],
  ].forEach(([gameId, state]) => {
    const directory = join(root, gameId ?? '');
    mkdirSync(directory, { recursive: true });
    writeFileSync(
      join(directory, 'manifest.json'),
      JSON.stringify(manifest(gameId ?? '', state ?? '')),
      { mode: 0o600 },
    );
  });
  return root;
})();

afterAll(() => {
  rmSync(runsRoot, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// The rig
// ---------------------------------------------------------------------------

interface Observed {
  readonly status: number;
  readonly headers: ReadonlyMap<string, string>;
  readonly bytes: Uint8Array;
  readonly text: string;
}

/** Everything that would reach the socket, read back off a real `Response`. */
const observe = async (
  response: HttpServerResponse.HttpServerResponse,
): Promise<Observed> => {
  const web = HttpServerResponse.toWeb(response);
  const bytes = new Uint8Array(await web.arrayBuffer());
  return {
    status: web.status,
    headers: new Map(web.headers.entries()),
    bytes,
    text: new TextDecoder().decode(bytes),
  };
};

/** `_canonical({"error": message})` — never hand-written. */
const problemBytes = (message: string): Uint8Array =>
  Either.getOrElse(Gateway.gatewayProblemBytes(message), () => new Uint8Array());

const canonical = (value: Parameters<typeof canonicalBytes>[0]): Uint8Array =>
  Either.getOrElse(canonicalBytes(value, CANON_UTF8), () => new Uint8Array());

const encoder = new TextEncoder();

interface Rig {
  readonly layer: Layer.Layer<ViewerRouteServices>;
  /** Every upstream URL the route asked for, in order. */
  readonly urls: () => readonly string[];
  /** Every derivation the route ran, in order. */
  readonly derivations: () => readonly DerivationRequest[];
}

/**
 * The three services, with the upstream URL and every derivation recorded.
 *
 * Recording sits in a closure rather than in module scope so two tests cannot
 * see each other's traffic.
 */
const makeRig = (options: {
  readonly fetch: FetchLike;
  readonly derivation?: Parameters<typeof layerFromRunner>[0] | undefined;
  readonly derivationLayer?: ReturnType<typeof ReplayDerivationFixture> | undefined;
}): Rig => {
  const urls: string[] = [];
  const derivations: DerivationRequest[] = [];
  const recordingFetch: FetchLike = (url, init) => {
    urls.push(url);
    return options.fetch(url, init);
  };
  const runner = options.derivation;
  const derivationLayer =
    options.derivationLayer ??
    layerFromRunner((request) => {
      derivations.push(request);
      return runner === undefined
        ? Effect.fail(
            new DerivationArtifactsMissing({
              operation: request.operation,
              gameId: request.gameId,
              detail: 'no fixture',
            }),
          )
        : runner(request);
    });
  return {
    layer: Layer.mergeAll(
      upstreamLayerTest({ serviceUrl: SERVICE_URL, fetch: recordingFetch }),
      runsLayer(runsRoot),
      derivationLayer,
    ),
    urls: () => urls,
    derivations: () => derivations,
  };
};

/** Run a route through the real response site and read the bytes back. */
const serve = (route: ViewerRouteEffect, rig: Rig): Promise<Observed> =>
  Effect.runPromise(
    Effect.provide(
      Logger.withMinimumLogLevel(respondGateway(route), LogLevel.None),
      rig.layer,
    ),
  ).then(observe);

// ---------------------------------------------------------------------------
// Routes, always through the real dispatcher
// ---------------------------------------------------------------------------

const decide = (path: string, query: string): RouteDecision =>
  Either.getOrThrowWith(
    dispatch('GET', path, query, 'absent'),
    (problem) => new Error(`dispatch refused: ${problem.message}`),
  );

const replayOf = (gameId: string, query: string): ViewerRouteEffect => {
  const decision = decide(`/v1/games/${gameId}/replay.json`, query);
  if (decision._tag !== 'ReplayJson') throw new Error(`expected ReplayJson, got ${decision._tag}`);
  return replayRoute(decision);
};

const boardOf = (gameId: string, query: string): ViewerRouteEffect => {
  const decision = decide(`/v1/games/${gameId}/board.json`, query);
  if (decision._tag !== 'BoardJson') throw new Error(`expected BoardJson, got ${decision._tag}`);
  return boardRoute(decision);
};

const eventsOf = (gameId: string, query: string): ViewerRouteEffect => {
  const decision = decide(`/v1/games/${gameId}/events.json`, query);
  if (decision._tag !== 'EventsJson') throw new Error(`expected EventsJson, got ${decision._tag}`);
  return eventsRoute(decision);
};

// ---------------------------------------------------------------------------
// Upstream stubs
// ---------------------------------------------------------------------------

const answering = (status: number, body: string, headers: Record<string, string> = {}): FetchLike =>
  () => Promise.resolve(new Response(body, { status, headers }));

const OFFLINE_PORTLESS: FetchLike = () =>
  Promise.resolve(
    new Response('<html>gone</html>', {
      status: 502,
      headers: { 'X-Portless': '1', 'Content-Type': 'text/html' },
    }),
  );

const OFFLINE_TRANSPORT: FetchLike = () => Promise.reject(new Error('ECONNREFUSED'));

const NEVER_CALLED: FetchLike = () => {
  throw new Error('upstream must not be contacted');
};

// ---------------------------------------------------------------------------
// 1. The two query parsers
// ---------------------------------------------------------------------------

describe('the two query parsers', () => {
  interface ReplayCase {
    readonly query: string;
    readonly outcome: string;
  }

  /**
   * Frozen from the CPython 3.14.6 differential.  `outcome` is either the
   * normalized upstream query or the symbolic problem name.
   */
  const replayCases: readonly ReplayCase[] = [
    { query: '', outcome: 'after_turn=0&limit=250' },
    { query: 'after_turn=1', outcome: 'after_turn=1&limit=250' },
    { query: 'limit=5&after_turn=2', outcome: 'after_turn=2&limit=5' },
    // Python `int()`, not `Number()`: underscores group digits.
    { query: 'limit=5_0', outcome: 'after_turn=0&limit=50' },
    { query: 'after_turn=%2B7', outcome: 'after_turn=7&limit=250' },
    // `parse_qs` turns `+` into a space, and `int()` strips it.
    { query: 'after_turn=+7', outcome: 'after_turn=7&limit=250' },
    { query: 'after_turn=%205%20', outcome: 'after_turn=5&limit=250' },
    // Unicode decimal digits parse: ٣ is three.
    { query: 'after_turn=%D9%A3', outcome: 'after_turn=3&limit=250' },
    { query: 'after_turn=1_0_0', outcome: 'after_turn=100&limit=250' },
    { query: 'after_turn=1__0', outcome: 'replayQueryNotIntegers' },
    { query: 'after_turn=0x10', outcome: 'replayQueryNotIntegers' },
    { query: 'after_turn=abc', outcome: 'replayQueryNotIntegers' },
    { query: 'after_turn=1e3', outcome: 'replayQueryNotIntegers' },
    { query: 'after_turn=', outcome: 'replayQueryNotIntegers' },
    { query: 'limit', outcome: 'replayQueryNotIntegers' },
    { query: 'after_turn=-1', outcome: 'replayQueryOutOfRange' },
    { query: 'limit=0', outcome: 'replayQueryOutOfRange' },
    { query: 'limit=251', outcome: 'replayQueryOutOfRange' },
    { query: 'limit=1', outcome: 'after_turn=0&limit=1' },
    { query: 'limit=250', outcome: 'after_turn=0&limit=250' },
    { query: 'after_turn=1&after_turn=2', outcome: 'replayQueryDuplicates' },
    { query: 'after_turn=1&x=2', outcome: 'replayQueryDuplicates' },
    { query: 'x=1', outcome: 'replayQueryDuplicates' },
    // An empty field is skipped, not a blank key.
    { query: '&', outcome: 'after_turn=0&limit=250' },
    { query: 'a=1&&b=2', outcome: 'replayQueryDuplicates' },
  ];

  replayCases.forEach(({ query, outcome }) => {
    test(`_replay_query(${JSON.stringify(query)}) -> ${outcome}`, () => {
      expect(
        Either.match(replayQuery(query), {
          onLeft: (problem) => problem.problem,
          onRight: (value) => value.normalizedQuery,
        }),
      ).toBe(outcome);
    });
  });

  const boardCases: readonly ReplayCase[] = [
    { query: 'turn=1', outcome: 'turn=1' },
    { query: 'turn=%205', outcome: 'turn=5' },
    { query: 'turn=9999999999999999999999', outcome: 'turn=9999999999999999999999' },
    { query: '', outcome: 'boardQueryTurn' },
    { query: 'turn=1&turn=1', outcome: 'boardQueryTurn' },
    { query: 'turn=1&x=2', outcome: 'boardQueryTurn' },
    { query: 'x=1', outcome: 'boardQueryTurn' },
    { query: 'turn=', outcome: 'boardTurnNotInteger' },
    { query: 'turn', outcome: 'boardTurnNotInteger' },
    { query: 'turn=%zz', outcome: 'boardTurnNotInteger' },
    { query: 'turn=%4', outcome: 'boardTurnNotInteger' },
    { query: 'turn=1e3', outcome: 'boardTurnNotInteger' },
    { query: 'turn=0', outcome: 'boardTurnNotPositive' },
    { query: 'turn=-1', outcome: 'boardTurnNotPositive' },
  ];

  boardCases.forEach(({ query, outcome }) => {
    test(`_board_query(${JSON.stringify(query)}) -> ${outcome}`, () => {
      expect(
        Either.match(boardQuery(query), {
          onLeft: (problem) => problem.problem,
          onRight: (value) => value.normalizedQuery,
        }),
      ).toBe(outcome);
    });
  });

  test('an integer past 2**53 survives the normalized query', () => {
    // `bigint`, not `number`: a `number` would forward `1e+22`.
    expect(
      Either.getOrThrowWith(replayQuery('after_turn=9999999999999999999999'), () => new Error()),
    ).toMatchObject({ normalizedQuery: 'after_turn=9999999999999999999999&limit=250' });
  });

  test('parse_qs keeps first-appearance order and groups repeats', () => {
    expect([...parseQuery('b=1&a=2&b=3')]).toEqual([
      ['b', ['1', '3']],
      ['a', ['2']],
    ]);
  });

  test('unquote leaves an invalid escape literal and replaces bad UTF-8', () => {
    expect(pythonUnquote('%zz')).toBe('%zz');
    expect(pythonUnquote('%4')).toBe('%4');
    expect(pythonUnquote('%C3%A9')).toBe('é');
    // Split across an ASCII-run boundary: two invalid sequences, not one `é`.
    expect(pythonUnquote('%C3é%A9')).toBe('�é�');
    expect(pythonUnquote('nothing')).toBe('nothing');
  });

  test('a Python int past 2**53 saturates at the loader seam, and only there', () => {
    expect(toLoaderInteger(7n)).toBe(7);
    expect(toLoaderInteger(BigInt(Number.MAX_SAFE_INTEGER) + 10n)).toBe(Number.MAX_SAFE_INTEGER);
  });
});

// ---------------------------------------------------------------------------
// 2. Every 400, verbatim, with nothing proxied
// ---------------------------------------------------------------------------

describe('a query refusal never reaches the network', () => {
  interface RefusalCase {
    readonly label: string;
    readonly route: () => ViewerRouteEffect;
    readonly problem: Gateway.GatewayProblemName;
  }

  const refusals: readonly RefusalCase[] = [
    {
      label: 'replay.json?x=1',
      route: () => replayOf(TERMINAL_GAME, 'x=1'),
      problem: 'replayQueryDuplicates',
    },
    {
      label: 'replay.json?after_turn=abc',
      route: () => replayOf(TERMINAL_GAME, 'after_turn=abc'),
      problem: 'replayQueryNotIntegers',
    },
    {
      label: 'replay.json?limit=251',
      route: () => replayOf(TERMINAL_GAME, 'limit=251'),
      problem: 'replayQueryOutOfRange',
    },
    {
      label: 'board.json (no query)',
      route: () => boardOf(TERMINAL_GAME, ''),
      problem: 'boardQueryTurn',
    },
    {
      label: 'board.json?turn=abc',
      route: () => boardOf(TERMINAL_GAME, 'turn=abc'),
      problem: 'boardTurnNotInteger',
    },
    {
      label: 'board.json?turn=0',
      route: () => boardOf(TERMINAL_GAME, 'turn=0'),
      problem: 'boardTurnNotPositive',
    },
    {
      label: 'events.json?anything=1',
      route: () => eventsOf(TERMINAL_GAME, 'anything=1'),
      problem: 'eventsQuery',
    },
  ];

  refusals.forEach(({ label, route, problem }) => {
    test(`${label} -> 400 ${Gateway.GATEWAY_PROBLEM_MESSAGES[problem]}`, async () => {
      const rig = makeRig({ fetch: NEVER_CALLED });
      const observed = await serve(route(), rig);
      expect(observed.status).toBe(400);
      expect(observed.bytes).toEqual(problemBytes(Gateway.GATEWAY_PROBLEM_MESSAGES[problem]));
      expect(observed.headers.get('content-type')).toBe(Gateway.GATEWAY_PROBLEM_CONTENT_TYPE);
      expect(observed.headers.get('cache-control')).toBe('no-store');
      expect(observed.headers.get('x-content-type-options')).toBe('nosniff');
      // The whole point: nothing was proxied, nothing was derived.
      expect(rig.urls()).toEqual([]);
      expect(rig.derivations()).toEqual([]);
    });
  });

  test('an empty query — which is what a bare `?` produces — is served', async () => {
    const rig = makeRig({
      fetch: answering(200, '{"ok":true}'),
    });
    const observed = await serve(eventsOf(TERMINAL_GAME, ''), rig);
    expect(observed.status).toBe(200);
    expect(rig.urls()).toEqual([`${SERVICE_URL}/v1/games/${TERMINAL_GAME}/events.json`]);
  });

  test('a trailing slash routes, and the viewer family forwards it canonically', async () => {
    // Trap B1's other half: `path.strip("/")` lets `…/replay.json/` route, and
    // `_replay` rebuilds the upstream path from an f-string (`:1657`) — so
    // unlike the archive routes, the slash does **not** reach upstream.
    const decision = decide(`/v1/games/${TERMINAL_GAME}/replay.json/`, '');
    if (decision._tag !== 'ReplayJson') throw new Error(`got ${decision._tag}`);
    const rig = makeRig({ fetch: answering(200, '{}') });
    await serve(replayRoute(decision), rig);
    expect(rig.urls()).toEqual([
      `${SERVICE_URL}/v1/games/${TERMINAL_GAME}/replay.json?after_turn=0&limit=250`,
    ]);
  });
});

// ---------------------------------------------------------------------------
// 3. The 2xx relay
// ---------------------------------------------------------------------------

describe('an upstream 2xx is relayed byte for byte', () => {
  /** Key order, spacing and float spelling no canonical writer would produce. */
  const UPSTREAM_BODY = '{"z":1,  "a":[1.50,"é"],\n"m":{"b":2,"a":1}}';

  test('replay.json relays the bytes, the status and nothing else', async () => {
    const rig = makeRig({ fetch: answering(200, UPSTREAM_BODY) });
    const observed = await serve(replayOf(TERMINAL_GAME, 'after_turn=2&limit=5'), rig);
    expect(observed.status).toBe(200);
    expect(observed.bytes).toEqual(encoder.encode(UPSTREAM_BODY));
    expect(observed.headers.get('content-type')).toBe(Gateway.GATEWAY_PROBLEM_CONTENT_TYPE);
    expect(observed.headers.get('cache-control')).toBe('no-store');
    expect(observed.headers.get('referrer-policy')).toBe('no-referrer');
    // The query is re-encoded canonically whatever the client sent.
    expect(rig.urls()).toEqual([
      `${SERVICE_URL}/v1/games/${TERMINAL_GAME}/replay.json?after_turn=2&limit=5`,
    ]);
    expect(rig.derivations()).toEqual([]);
  });

  test('replay.json with no query still forwards both parameters', async () => {
    const rig = makeRig({ fetch: answering(200, '{}') });
    await serve(replayOf(LIVE_GAME, ''), rig);
    expect(rig.urls()).toEqual([
      `${SERVICE_URL}/v1/games/${LIVE_GAME}/replay.json?after_turn=0&limit=250`,
    ]);
  });

  test('board.json forwards the canonical turn', async () => {
    const rig = makeRig({ fetch: answering(200, '{}') });
    await serve(boardOf(LIVE_GAME, 'turn=%2007%20'), rig);
    expect(rig.urls()).toEqual([`${SERVICE_URL}/v1/games/${LIVE_GAME}/board.json?turn=7`]);
  });

  test('events.json forwards no query at all', async () => {
    const rig = makeRig({ fetch: answering(200, '{}') });
    await serve(eventsOf(LIVE_GAME, ''), rig);
    expect(rig.urls()).toEqual([`${SERVICE_URL}/v1/games/${LIVE_GAME}/events.json`]);
  });

  test('a 2xx that is not 200 keeps its status', async () => {
    const rig = makeRig({ fetch: answering(203, '{"a":1}') });
    const observed = await serve(eventsOf(TERMINAL_GAME, ''), rig);
    expect(observed.status).toBe(203);
    expect(observed.text).toBe('{"a":1}');
  });

  test('events.json does NOT re-project an upstream 2xx', async () => {
    // `_public_events` runs only on the disk path (`:1797`); re-shaping a relay
    // would destroy byte parity with the supervisor.
    const rig = makeRig({ fetch: answering(200, '{"events":[],"game_id":"someone-else"}') });
    const observed = await serve(eventsOf(TERMINAL_GAME, ''), rig);
    expect(observed.text).toBe('{"events":[],"game_id":"someone-else"}');
  });
});

// ---------------------------------------------------------------------------
// 4. The fallback matrix
// ---------------------------------------------------------------------------

describe('the disk fallback, and only for the three triggers', () => {
  /**
   * The loader's document as the bridge really delivers it: Python `int`s, so
   * `bigint`s.  A `1` here would be a Python *float* and `replay.json` would
   * come back `"schema_version":1.0` — which is exactly the defect the live
   * smoke rig caught, and which a fixture written with `number`s would have
   * hidden from every unit test in this file.
   */
  const REPLAY_BODY: CanonRecord = { schema_version: 1n, next_after_turn: 4n, snapshots: [] };

  const replayFixture = (afterTurn: number, limit: number, complete: boolean, gameId: string) =>
    ReplayDerivationFixture(
      derivationFixture({
        [derivationRequestKey({
          operation: 'replay',
          gameId,
          places: [],
          afterTurn,
          limit,
          complete,
        })]: REPLAY_BODY,
      }),
    );

  const triggers: readonly (readonly [string, FetchLike])[] = [
    ['upstream 404', answering(404, 'not found')],
    ['upstream 405', answering(405, 'nope')],
    ['the Portless offline probe', OFFLINE_PORTLESS],
    ['a transport failure', OFFLINE_TRANSPORT],
  ];

  triggers.forEach(([label, fetch]) => {
    test(`${label} serves the derivation`, async () => {
      const rig = makeRig({
        fetch,
        derivationLayer: replayFixture(0, 250, true, TERMINAL_GAME),
      });
      const observed = await serve(replayOf(TERMINAL_GAME, ''), rig);
      expect(observed.status).toBe(200);
      // `_bounded_json`: canonical, sorted keys, no spaces.
      expect(observed.bytes).toEqual(canonical(REPLAY_BODY));
      expect(observed.headers.get('cache-control')).toBe('no-store');
    });
  });

  test('an interrupted, non-terminal run still serves its disk replay when upstream is gone', async () => {
    // The property Python's `except GatewayProblem: manifest = _read_manifest`
    // protects (`:1687-1693`): `_terminal_archive` would 404 on this run.
    const rig = makeRig({
      fetch: OFFLINE_PORTLESS,
      derivationLayer: replayFixture(0, 250, false, LIVE_GAME),
    });
    const observed = await serve(replayOf(LIVE_GAME, ''), rig);
    expect(observed.status).toBe(200);
    expect(observed.bytes).toEqual(canonical(REPLAY_BODY));
  });

  test('`complete` comes from the manifest, never from upstream', async () => {
    const terminal = makeRig({ fetch: answering(404, '') });
    await serve(replayOf(TERMINAL_GAME, 'after_turn=3&limit=7'), terminal);
    expect(terminal.derivations()).toMatchObject([
      { operation: 'replay', gameId: TERMINAL_GAME, afterTurn: 3, limit: 7, complete: true },
    ]);

    const live = makeRig({ fetch: answering(404, '') });
    await serve(eventsOf(LIVE_GAME, ''), live);
    expect(live.derivations()).toMatchObject([
      { operation: 'events', gameId: LIVE_GAME, complete: false },
    ]);
  });

  test('the board loader takes a turn and no completeness', async () => {
    const rig = makeRig({ fetch: answering(405, '') });
    await serve(boardOf(TERMINAL_GAME, 'turn=12'), rig);
    expect(rig.derivations()).toMatchObject([
      { operation: 'board', gameId: TERMINAL_GAME, turn: 12 },
    ]);
  });

  test('the loader receives sanitized places, JSON-safe', async () => {
    const rig = makeRig({ fetch: answering(404, '') });
    await serve(replayOf(TERMINAL_GAME, ''), rig);
    const places = rig.derivations()[0]?.places ?? [];
    expect(places).toEqual([
      {
        place: 1,
        seat_id: 'seat-a',
        player_name: 'Alice',
        player_color: '#ff0000',
        controller: 'agent',
        joined: true,
        controller_type: 'external',
        model: 'claude-x',
      },
    ]);
    // The sanitizer's whole point: neither field survives.
    expect(JSON.stringify(places)).not.toContain('secret');
    // And it is JSON, not canon: `JSON.stringify` would throw on a bigint.
    expect(() => JSON.stringify(places)).not.toThrow();
  });

  test('events.json projects the loader document through _public_events', async () => {
    // Python `int`s, i.e. `bigint`s: `_public_event`'s rejections are
    // `isinstance(..., int)` tests, so a fixture spelled with `number`s would
    // be a document of floats and every row would be dropped.
    const raw: CanonRecord = {
      available: true,
      events: [
        { turn: 3n, kind: 'city_founded', summary: 'Alice founds Rome', weight: 3n, actors: ['a'] },
        { turn: 4n, kind: '', summary: 'dropped', weight: 3n },
      ],
      total_events: 99n,
      game_id: 'someone-else',
      schema_version: 77n,
    };
    const rig = makeRig({
      fetch: answering(404, ''),
      derivationLayer: ReplayDerivationFixture(
        derivationFixture({
          [derivationRequestKey({
            operation: 'events',
            gameId: TERMINAL_GAME,
            places: [],
            complete: true,
          })]: raw,
        }),
      ),
    });
    const observed = await serve(eventsOf(TERMINAL_GAME, ''), rig);
    const payload: unknown = JSON.parse(observed.text);
    expect(observed.status).toBe(200);
    expect(payload).toMatchObject({
      // The *requested* id and the port's schema version, not the loader's.
      game_id: TERMINAL_GAME,
      schema_version: Gateway.ARCHIVE_SCHEMA_VERSION,
      // The malformed row is dropped, and `total_events` still reports the
      // loader's own count (`:420` only defaults when the key is absent).
      total_events: 99,
    });
    const events = untrustedField(payload, 'events');
    expect(Array.isArray(events)).toBe(true);
    expect(events).toHaveLength(1);
  });
});

describe('an upstream failure is never masked by disk data', () => {
  const cases: readonly (readonly [number, string, number, string])[] = [
    [500, 'internal', 500, Gateway.upstreamReturnedHttp(500)],
    [418, 'teapot', 418, Gateway.upstreamReturnedHttp(418)],
    [400, 'bad', 400, Gateway.upstreamReturnedHttp(400)],
  ];

  cases.forEach(([upstreamStatus, body, status, message]) => {
    test(`upstream ${String(upstreamStatus)} -> ${String(status)} ${message}`, async () => {
      const rig = makeRig({ fetch: answering(upstreamStatus, body) });
      const observed = await serve(replayOf(TERMINAL_GAME, ''), rig);
      expect(observed.status).toBe(status);
      expect(observed.bytes).toEqual(problemBytes(message));
      expect(rig.derivations()).toEqual([]);
    });
  });

  test('a redirect is refused, not followed', async () => {
    const rig = makeRig({
      fetch: answering(302, '', { Location: 'http://evil.example/' }),
    });
    const observed = await serve(boardOf(TERMINAL_GAME, 'turn=1'), rig);
    expect(observed.status).toBe(502);
    expect(observed.bytes).toEqual(
      problemBytes(Gateway.GATEWAY_PROBLEM_MESSAGES.upstreamRedirect),
    );
    // One request, and no second one to the redirect target.
    expect(rig.urls()).toHaveLength(1);
    expect(rig.derivations()).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// 5. The two 8 MiB errors
// ---------------------------------------------------------------------------

describe('the two size limits are different errors', () => {
  test('an oversized UPSTREAM body is a 502, and never falls back', async () => {
    const rig = makeRig({
      // Rejected on the header, before a byte is read (`:1527`).
      fetch: () =>
        Promise.resolve(
          new Response('{}', {
            status: 200,
            headers: { 'Content-Length': String(MAX_PROXY_JSON_BYTES + 1) },
          }),
        ),
    });
    const observed = await serve(replayOf(TERMINAL_GAME, ''), rig);
    expect(observed.status).toBe(502);
    expect(observed.bytes).toEqual(
      problemBytes(Gateway.GATEWAY_PROBLEM_MESSAGES.upstreamJsonTooLarge),
    );
    expect(rig.derivations()).toEqual([]);
  });

  test('an oversized LOCAL body is a 503 with the other message', async () => {
    const huge: CanonRecord = { a: 'x'.repeat(MAX_PROXY_JSON_BYTES) };
    const rig = makeRig({
      fetch: answering(404, ''),
      derivation: () => Effect.succeed(huge),
    });
    const observed = await serve(replayOf(TERMINAL_GAME, ''), rig);
    expect(observed.status).toBe(503);
    expect(observed.bytes).toEqual(
      problemBytes(Gateway.GATEWAY_PROBLEM_MESSAGES.archiveJsonTooLarge),
    );
    // The body was built in full and then declined: the cap is on the
    // response, not on the work.
    expect(rig.derivations()).toHaveLength(1);
  });

  test('a body one byte under the cap is served', async () => {
    const body: CanonRecord = { a: 'x'.repeat(MAX_PROXY_JSON_BYTES - 10) };
    const rig = makeRig({ fetch: answering(404, ''), derivation: () => Effect.succeed(body) });
    const observed = await serve(replayOf(TERMINAL_GAME, ''), rig);
    expect(observed.status).toBe(200);
    expect(observed.bytes.byteLength).toBeLessThanOrEqual(MAX_PROXY_JSON_BYTES);
  });

  test('boundedJson refuses a value the canonical writer cannot spell, as a 500', async () => {
    // A lone surrogate: `json.dumps(...).encode("utf-8")` raises, after the
    // loader's `except` clauses, so Python answers `:2040`'s 500.
    const rig = makeRig({
      fetch: answering(404, ''),
      derivation: () => Effect.succeed({ a: '\ud800' }),
    });
    const observed = await serve(replayOf(TERMINAL_GAME, ''), rig);
    expect(observed.status).toBe(500);
    expect(observed.bytes).toEqual(problemBytes(Gateway.GATEWAY_PROBLEM_MESSAGES.internalError));
  });

  test('boundedJson is the only size decision, and it is on encoded bytes', () => {
    // Two code points, four UTF-8 bytes: a length check on the string would
    // pass a body the byte check refuses.
    expect(Either.isRight(boundedJsonResponse({ a: 'éé' }))).toBe(true);
    expect(
      Either.match(boundedJsonResponse({ a: 'x'.repeat(MAX_PROXY_JSON_BYTES) }), {
        onLeft: (error) => error._tag,
        onRight: () => 'served',
      }),
    ).toBe('ArchiveUnavailable');
  });
});

// ---------------------------------------------------------------------------
// 6. Loader failures
// ---------------------------------------------------------------------------

describe('a loader failure is classified, and never quoted', () => {
  const operations: readonly (readonly [
    DerivationOperation,
    () => ViewerRouteEffect,
  ])[] = [
    ['replay', () => replayOf(TERMINAL_GAME, '')],
    ['board', () => boardOf(TERMINAL_GAME, 'turn=1')],
    ['events', () => eventsOf(TERMINAL_GAME, '')],
  ];

  operations.forEach(([operation, route]) => {
    test(`${operation}: FileNotFoundError -> 404 with its own message`, async () => {
      const rig = makeRig({
        fetch: answering(404, ''),
        derivation: (request) =>
          Effect.fail(
            new DerivationArtifactsMissing({
              operation: request.operation,
              gameId: request.gameId,
              detail: '/private/path/should/not/leak',
            }),
          ),
      });
      const observed = await serve(route(), rig);
      const expected = derivationProblem(
        new DerivationArtifactsMissing({ operation, gameId: TERMINAL_GAME, detail: '' }),
      );
      expect(observed.status).toBe(404);
      expect(observed.status).toBe(expected.status);
      expect(observed.bytes).toEqual(problemBytes(expected.message));
      expect(observed.text).not.toContain('private');
    });

    test(`${operation}: any other loader error -> 503 with its own message`, async () => {
      const rig = makeRig({
        fetch: answering(405, ''),
        derivationLayer: ReplayDerivationUnavailable,
      });
      const observed = await serve(route(), rig);
      const expected = derivationProblem(
        new DerivationUnavailable({ operation, gameId: TERMINAL_GAME, detail: '' }),
      );
      expect(observed.status).toBe(503);
      expect(observed.status).toBe(expected.status);
      expect(observed.bytes).toEqual(problemBytes(expected.message));
    });
  });

  test('a game with no manifest is a 404 before any loader runs', async () => {
    const rig = makeRig({ fetch: answering(404, '') });
    const observed = await serve(replayOf(ABSENT_GAME, ''), rig);
    expect(observed.status).toBe(404);
    expect(observed.bytes).toEqual(problemBytes(Gateway.GATEWAY_PROBLEM_MESSAGES.gameNotFound));
    expect(rig.derivations()).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// 7. The two translations, pinned to the modules that own them
// ---------------------------------------------------------------------------

describe('the failure translations cannot drift', () => {
  const upstreamFailures: readonly UpstreamFailure[] = [
    new UpstreamClientUnavailable({ reason: 'portless', url: 'u', cause: null }),
    new UpstreamClientUnavailable({ reason: 'transport', url: 'u', cause: null }),
    new UpstreamClientUnavailable({ reason: 'timeout', url: 'u', cause: null }),
    new UpstreamJsonTooLarge({ source: 'body', capBytes: 1, bytesRead: 2, bytesRetained: 1, url: 'u' }),
    new UpstreamJsonTooLarge({ source: 'content-length', capBytes: 1, bytesRead: 0, bytesRetained: 0, url: 'u' }),
    new UpstreamClientHttpError({ status: 302, url: 'u' }),
    new UpstreamClientHttpError({ status: 307, url: 'u' }),
    new UpstreamBodyError({ reason: 'read', url: 'u', cause: null }),
    new UpstreamBodyError({ reason: 'timeout', url: 'u', cause: null }),
  ];

  upstreamFailures.forEach((failure) => {
    test(`${failure._tag} renders what UpstreamClient says it renders`, () => {
      expect(gatewayProblem(gatewayErrorFromUpstream(failure))).toEqual(
        upstreamFailureProblem(failure),
      );
    });
  });

  const derivationFailures: readonly DerivationError[] = (
    ['replay', 'board', 'events'] as const
  ).flatMap((operation) => [
    new DerivationArtifactsMissing({ operation, gameId: TERMINAL_GAME, detail: 'x' }),
    new DerivationUnavailable({ operation, gameId: TERMINAL_GAME, detail: 'x' }),
  ]);

  derivationFailures.forEach((failure) => {
    test(`${failure._tag}/${failure.operation} renders what ReplayDerivation says`, () => {
      expect(gatewayProblem(gatewayErrorFromDerivation(failure))).toEqual(
        derivationProblem(failure),
      );
    });
  });

  test('canonToJson lowers a bigint and leaves everything else alone', () => {
    expect(canonToJson({ a: 1n, b: [2n, 'x', null, true, 1.5] })).toEqual({
      a: 1,
      b: [2, 'x', null, true, 1.5],
    });
  });

  test('derivationPlaces drops a manifest whose places are not rows', () => {
    expect(derivationPlaces({ game_id: TERMINAL_GAME, resolved_places: 'nonsense' })).toEqual([]);
    expect(derivationPlaces({ game_id: TERMINAL_GAME })).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// 8. The mutex, and the absence of a response cache
// ---------------------------------------------------------------------------

describe('replay_lock serializes the loaders across routes', () => {
  test('a second derivation cannot start while the first is in flight', async () => {
    const gate = {
      started: Effect.runSync(Deferred.make<void>()),
      release: Effect.runSync(Deferred.make<void>()),
      inFlight: Effect.runSync(Ref.make(0)),
      peak: Effect.runSync(Ref.make(0)),
    };

    const rig = makeRig({
      fetch: answering(404, ''),
      derivation: (request) =>
        Effect.gen(function* () {
          const now = yield* Ref.updateAndGet(gate.inFlight, (count) => count + 1);
          yield* Ref.update(gate.peak, (seen) => (seen < now ? now : seen));
          yield* Deferred.succeed(gate.started, undefined);
          yield* Deferred.await(gate.release);
          yield* Ref.update(gate.inFlight, (count) => count - 1);
          const answer: CanonRecord = { operation: request.operation };
          return answer;
        }),
    });

    /**
     * The layer is provided **once**, to the whole program.
     *
     * This is not a test detail: `Effect.provide` builds a layer per call, so
     * a server that provided `ReplayDerivation` per request would hand every
     * request its own semaphore and lose `replay_lock` entirely, with no type
     * error and no failing unit test.  Providing once is what makes the mutex
     * real, and the assertion below is what proves it.
     */
    const program = Effect.gen(function* () {
      const first = yield* Effect.fork(respondGateway(replayOf(TERMINAL_GAME, '')));
      yield* Deferred.await(gate.started);
      // A *different* route on a *different* game: the lock is one per
      // process, not one per game and not one per loader.
      const second = yield* Effect.fork(respondGateway(boardOf(LIVE_GAME, 'turn=1')));
      // Long enough for an unsynchronized second derivation to have started.
      yield* Effect.sleep(Duration.millis(50));
      const midway = yield* Ref.get(gate.peak);

      yield* Deferred.succeed(gate.release, undefined);
      const firstResponse = yield* Fiber.join(first);
      const secondResponse = yield* Fiber.join(second);

      return {
        midway,
        peak: yield* Ref.get(gate.peak),
        statuses: [firstResponse.status, secondResponse.status],
        derivations: rig.derivations().length,
      };
    });

    const outcome = await Effect.runPromise(
      Effect.provide(Logger.withMinimumLogLevel(program, LogLevel.None), rig.layer),
    );
    expect(outcome.midway).toBe(1);
    expect(outcome.peak).toBe(1);
    expect(outcome.statuses).toEqual([200, 200]);
    expect(outcome.derivations).toBe(2);
  });

  test('the gateway holds no response cache: the cold and warm legs both derive', async () => {
    const answers = ['cold', 'warm'];
    const calls: DerivationRequest[] = [];
    const rig = makeRig({
      fetch: answering(404, ''),
      derivation: (request) => {
        calls.push(request);
        const answer: CanonRecord = { leg: answers[calls.length - 1] ?? 'extra' };
        return Effect.succeed(answer);
      },
    });

    const cold = await serve(replayOf(TERMINAL_GAME, 'after_turn=1&limit=2'), rig);
    const warm = await serve(replayOf(TERMINAL_GAME, 'after_turn=1&limit=2'), rig);

    // Identical requests, and the loader ran twice: the only cache in the
    // system is the loaders' own, under `cache_root`.
    expect(calls).toHaveLength(2);
    expect(cold.text).toBe('{"leg":"cold"}');
    expect(warm.text).toBe('{"leg":"warm"}');
  });
});

// ---------------------------------------------------------------------------
// 9. Shape guards on the module surface
// ---------------------------------------------------------------------------

describe('the module surface', () => {
  test('the derivation fixture key is what the routes actually request', () => {
    // If this drifts, every fixture-based test above silently starts asserting
    // "no artifacts" instead of the payload it wrote.
    expect(
      derivationRequestKey({
        operation: 'replay',
        gameId: TERMINAL_GAME,
        places: [],
        afterTurn: 0,
        limit: 250,
        complete: true,
      }),
    ).toBe(`replay:${TERMINAL_GAME}:0:250:true`);
  });

  test('a route effect needs exactly the three services, and no scope', async () => {
    // The three layers leave `R = never`: the annotation is the assertion, and
    // the run proves the annotation is not vacuous.  A route that quietly
    // acquired a `Scope` (an upstream reader left open, say) would not fit.
    const rig = makeRig({ fetch: answering(200, '{"ok":true}') });
    const provided: Effect.Effect<HttpServerResponse.HttpServerResponse, GatewayError> =
      Effect.provide(replayOf(TERMINAL_GAME, ''), rig.layer);
    const observed = await observe(await Effect.runPromise(provided));
    expect(observed.status).toBe(200);
  });
});
