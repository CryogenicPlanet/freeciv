/** Ordered route behavior matrix, including refusal precedence and forwarded paths. */
import { describe, expect, test } from 'bun:test';
import { Either, Option } from 'effect';
import { FrameIndex, GameId, Gateway } from '@arena/wire';
import {
  GATEWAY_METHODS,
  GATEWAY_REJECTED_METHODS,
  isGatewayMethod,
  type GatewayMethod,
} from 'src/gateway/constants';
import {
  ARCHIVE_JSON_VIEWS,
  collapseLeadingSlashes,
  dispatch,
  type DispatchProblem,
  type RequestBodySignal,
  type RouteDecision,
} from 'src/gateway/http/dispatch';

const MESSAGES = Gateway.GATEWAY_PROBLEM_MESSAGES;

/** The dossier's probe id: 29 chars, comfortably inside `GAME_ID_RE`. */
const GAME = GameId.make('game_gggggggggggggggggggggggg');
const ID_20 = GameId.make('a'.repeat(20));
const ID_80 = GameId.make('b'.repeat(80));

const decide = (
  path: string,
  query = '',
  body: RequestBodySignal = 'absent',
  method: GatewayMethod = 'GET',
): Either.Either<RouteDecision, DispatchProblem> => dispatch(method, path, query, body);

/** The decision, or `null` when the request was refused (which fails the diff). */
const routeOf = (path: string, query = ''): RouteDecision | null =>
  Either.getOrNull(decide(path, query));

const problemOf = (
  path: string,
  query = '',
  body: RequestBodySignal = 'absent',
  method: GatewayMethod = 'GET',
): DispatchProblem | null => Option.getOrNull(Either.getLeft(decide(path, query, body, method)));

/** What the one response site would render: the tag, the wire text, the status. */
const refusalOf = (
  path: string,
  query = '',
  body: RequestBodySignal = 'absent',
  method: GatewayMethod = 'GET',
): { tag: string; message: string; status: number } | null => {
  const problem = problemOf(path, query, body, method);
  return problem === null
    ? null
    : { tag: problem._tag, message: problem.message, status: problem.status };
};

interface BadRequestView {
  readonly tag: string;
  readonly message: string;
  readonly status: number;
}

const badRequest = (message: string): BadRequestView => ({
  tag: 'BadRequest',
  message,
  status: 400,
});

const NOT_FOUND = { tag: 'NotFound', message: MESSAGES.notFound, status: 404 };
const METHOD_NOT_ALLOWED = {
  tag: 'MethodNotAllowed',
  message: MESSAGES.methodNotAllowed,
  status: 405,
};

// ---------------------------------------------------------------------------
// Every route, and the upstream path it forwards
// ---------------------------------------------------------------------------

interface RouteCase {
  readonly why: string;
  readonly path: string;
  readonly query?: string;
  readonly expect: RouteDecision;
}

const ROUTE_CASES: ReadonlyArray<RouteCase> = [
  // 1-2 — the two exact matches (`:1970`, `:1978`).
  { why: '/health is served locally', path: '/health', expect: { _tag: 'Health' } },
  {
    why: '//health collapses (stdlib gh-87389)',
    path: '//health',
    expect: { _tag: 'Health' },
  },
  { why: '/v1/games is the index', path: '/v1/games', expect: { _tag: 'GamesIndex' } },
  {
    why: '//v1/games collapses to the index',
    path: '//v1/games',
    expect: { _tag: 'GamesIndex' },
  },
  {
    why: 'a bare trailing ? is no query at all',
    path: '/v1/games',
    query: '',
    expect: { _tag: 'GamesIndex' },
  },

  // 3 — the viewer routes, matched before the query gate (`:1996-2004`).
  {
    why: 'replay.json carries its raw query and forwards a rebuilt path',
    path: `/v1/games/${GAME}/replay.json`,
    query: 'limit=3&after_turn=2',
    expect: {
      _tag: 'ReplayJson',
      gameId: GAME,
      upstreamPath: `/v1/games/${GAME}/replay.json`,
      query: 'limit=3&after_turn=2',
    },
  },
  {
    why: 'replay.json with a trailing slash rebuilds WITHOUT it (B1, the other half)',
    path: `/v1/games/${GAME}/replay.json/`,
    expect: {
      _tag: 'ReplayJson',
      gameId: GAME,
      upstreamPath: `/v1/games/${GAME}/replay.json`,
      query: '',
    },
  },
  {
    why: 'board.json carries its raw query',
    path: `/v1/games/${GAME}/board.json`,
    query: 'turn=1',
    expect: {
      _tag: 'BoardJson',
      gameId: GAME,
      upstreamPath: `/v1/games/${GAME}/board.json`,
      query: 'turn=1',
    },
  },
  {
    why: 'events.json routes even with a query — the handler owns that 400 (:1729)',
    path: `/v1/games/${GAME}/events.json`,
    query: 'turn=1',
    expect: {
      _tag: 'EventsJson',
      gameId: GAME,
      upstreamPath: `/v1/games/${GAME}/events.json`,
      query: 'turn=1',
    },
  },

  // 4 — the archive JSON routes (`:2010-2019`).
  {
    why: 'the bare id aliases status and proxies WITHOUT /status',
    path: `/v1/games/${GAME}`,
    expect: {
      _tag: 'ArchiveStatus',
      gameId: GAME,
      upstreamPath: `/v1/games/${GAME}`,
      bareId: true,
    },
  },
  {
    why: '/status proxies the dispatched path verbatim',
    path: `/v1/games/${GAME}/status`,
    expect: {
      _tag: 'ArchiveStatus',
      gameId: GAME,
      upstreamPath: `/v1/games/${GAME}/status`,
      bareId: false,
    },
  },
  {
    why: 'a trailing slash routes AND is forwarded (B1)',
    path: `/v1/games/${GAME}/status/`,
    expect: {
      _tag: 'ArchiveStatus',
      gameId: GAME,
      upstreamPath: `/v1/games/${GAME}/status/`,
      bareId: false,
    },
  },
  {
    why: 'a collapsed // is what gets forwarded',
    path: `//v1/games/${GAME}/status`,
    expect: {
      _tag: 'ArchiveStatus',
      gameId: GAME,
      upstreamPath: `/v1/games/${GAME}/status`,
      bareId: false,
    },
  },
  {
    why: 'result',
    path: `/v1/games/${GAME}/result`,
    expect: {
      _tag: 'ArchiveResult',
      gameId: GAME,
      upstreamPath: `/v1/games/${GAME}/result`,
    },
  },
  {
    why: 'watch.json',
    path: `/v1/games/${GAME}/watch.json`,
    expect: {
      _tag: 'ArchiveWatch',
      gameId: GAME,
      upstreamPath: `/v1/games/${GAME}/watch.json`,
    },
  },
  {
    why: 'the frame listing',
    path: `/v1/games/${GAME}/frames`,
    expect: {
      _tag: 'ArchiveFrames',
      gameId: GAME,
      upstreamPath: `/v1/games/${GAME}/frames`,
    },
  },

  // 5 — the binary routes (`:2022-2035`).
  {
    why: 'latest.png',
    path: `/v1/games/${GAME}/frames/latest.png`,
    expect: {
      _tag: 'LatestFramePng',
      gameId: GAME,
      upstreamPath: `/v1/games/${GAME}/frames/latest.png`,
    },
  },
  {
    why: 'video.mp4',
    path: `/v1/games/${GAME}/video.mp4`,
    expect: {
      _tag: 'VideoMp4',
      gameId: GAME,
      upstreamPath: `/v1/games/${GAME}/video.mp4`,
    },
  },
  {
    why: 'frame 0 — the one index a leading-zero grammar would swallow',
    path: `/v1/games/${GAME}/frames/0.png`,
    expect: {
      _tag: 'FramePng',
      gameId: GAME,
      upstreamPath: `/v1/games/${GAME}/frames/0.png`,
      index: FrameIndex.make(0),
    },
  },
  {
    why: 'frame 7',
    path: `/v1/games/${GAME}/frames/7.png`,
    expect: {
      _tag: 'FramePng',
      gameId: GAME,
      upstreamPath: `/v1/games/${GAME}/frames/7.png`,
      index: FrameIndex.make(7),
    },
  },
  {
    why: 'frame 999999 — the widest index the six-digit on-disk name can hold',
    path: `/v1/games/${GAME}/frames/999999.png`,
    expect: {
      _tag: 'FramePng',
      gameId: GAME,
      upstreamPath: `/v1/games/${GAME}/frames/999999.png`,
      index: FrameIndex.make(999999),
    },
  },
  {
    why: 'an index beyond the archive still routes — the 404 comes from the file map',
    path: `/v1/games/${GAME}/frames/1000000.png`,
    expect: {
      _tag: 'FramePng',
      gameId: GAME,
      upstreamPath: `/v1/games/${GAME}/frames/1000000.png`,
      index: FrameIndex.make(1000000),
    },
  },
  {
    why: 'the shortest legal id routes',
    path: `/v1/games/${ID_20}/status`,
    expect: {
      _tag: 'ArchiveStatus',
      gameId: ID_20,
      upstreamPath: `/v1/games/${ID_20}/status`,
      bareId: false,
    },
  },
  {
    why: 'the longest legal id routes',
    path: `/v1/games/${ID_80}/status`,
    expect: {
      _tag: 'ArchiveStatus',
      gameId: ID_80,
      upstreamPath: `/v1/games/${ID_80}/status`,
      bareId: false,
    },
  },
];

describe('every route', () => {
  test.each(ROUTE_CASES.map((route) => [route.why, route] as const))('%s', (_why, route) => {
    expect(routeOf(route.path, route.query ?? '')).toEqual(route.expect);
  });

  test('the cases cover all twelve routes', () => {
    const seen = new Set<RouteDecision['_tag']>(ROUTE_CASES.map((route) => route.expect._tag));
    expect([...seen].toSorted()).toEqual([
      'ArchiveFrames',
      'ArchiveResult',
      'ArchiveStatus',
      'ArchiveWatch',
      'BoardJson',
      'EventsJson',
      'FramePng',
      'GamesIndex',
      'Health',
      'LatestFramePng',
      'ReplayJson',
      'VideoMp4',
    ]);
  });

  test('the four archive-JSON tags carry _archive_json_route’s `kind` argument', () => {
    expect(ARCHIVE_JSON_VIEWS).toEqual({
      ArchiveStatus: 'status',
      ArchiveResult: 'result',
      ArchiveWatch: 'watch',
      ArchiveFrames: 'frames',
    });
  });
});

// ---------------------------------------------------------------------------
// 404 — before any I/O, and before the query gate
// ---------------------------------------------------------------------------

const NOT_FOUND_CASES: ReadonlyArray<readonly [string, string, string]> = [
  ['/v1/games/ needs the exact match, and strip() leaves it two segments', '/v1/games/', ''],
  ['/health/ is not /health', '/health/', ''],
  ['the prefix is case-sensitive', '/V1/games', ''],
  ['an empty interior segment breaks the prefix', `/v1//games/${GAME}`, ''],
  ['a 19-character id is one short', `/v1/games/${'a'.repeat(19)}/status`, ''],
  ['an 81-character id is one long', `/v1/games/${'b'.repeat(81)}/status`, ''],
  ['a dot is outside the id alphabet', `/v1/games/${'a'.repeat(19)}./status`, ''],
  ['a space is outside the id alphabet', `/v1/games/${'a'.repeat(19)}%20/status`, ''],
  ['unicode is outside the id alphabet', `/v1/games/${'é'.repeat(25)}/status`, ''],
  ['%2F is never decoded into a separator', `/v1/games/${GAME}%2Fstatus`, ''],
  ['a short id 404s even with a query (:1988 precedes :2005)', '/v1/games/short/status', 'x=1'],
  ['00.png has a leading zero', `/v1/games/${GAME}/frames/00.png`, ''],
  ['007.png has leading zeros', `/v1/games/${GAME}/frames/007.png`, ''],
  ['-1.png is not the grammar', `/v1/games/${GAME}/frames/-1.png`, ''],
  ['1e3.png is not the grammar', `/v1/games/${GAME}/frames/1e3.png`, ''],
  ['+1.png is not the grammar', `/v1/games/${GAME}/frames/+1.png`, ''],
  ['.png alone is not the grammar', `/v1/games/${GAME}/frames/.png`, ''],
  ['the extension is case-sensitive', `/v1/games/${GAME}/frames/0.PNG`, ''],
  ['a third segment is one too many', `/v1/games/${GAME}/frames/0.png/extra`, ''],
  ['latest.png only under frames/', `/v1/games/${GAME}/latest.png`, ''],
  ['an unknown suffix', `/v1/games/${GAME}/nonsense`, ''],
  ['a private control route is never proxied', `/v1/games/${GAME}/join`, ''],
  ['the agent route is not the gateway’s', '/me/next', ''],
  ['the native viewer is not the gateway’s', '/native-viewer', ''],
  ['internal routes are not the gateway’s', `/internal/v1/games/${GAME}/turns`, ''],
  ['the watch page is the viewer’s, not the gateway’s', `/watch/${GAME}`, ''],
  ['the root', '/', ''],
  ['the empty path', '', ''],
];

describe('404 — not found', () => {
  test.each(NOT_FOUND_CASES)('%s', (_why, path, query) => {
    expect(refusalOf(path, query)).toEqual(NOT_FOUND);
  });
});

// ---------------------------------------------------------------------------
// 400 — the query gates and the body gate
// ---------------------------------------------------------------------------

describe('400 — query gates', () => {
  const QUERY_CASES: ReadonlyArray<readonly [string, string, string, string]> = [
    ['/health takes no query', '/health', 'x=1', MESSAGES.healthQuery],
    ['/health takes no query, however harmless', '/health', 'pretty', MESSAGES.healthQuery],
    ['/v1/games takes no query', '/v1/games', 'limit=1', MESSAGES.gamesIndexQuery],
    [
      'status rejects a query, and never proxies it',
      `/v1/games/${GAME}/status`,
      'token=private',
      MESSAGES.viewerRouteQuery,
    ],
    [
      'the bare-id alias rejects a query too',
      `/v1/games/${GAME}`,
      'x=1',
      MESSAGES.viewerRouteQuery,
    ],
    ['result rejects a query', `/v1/games/${GAME}/result`, 'x=1', MESSAGES.viewerRouteQuery],
    ['watch.json rejects a query', `/v1/games/${GAME}/watch.json`, 'x=1', MESSAGES.viewerRouteQuery],
    ['the frame listing rejects a query', `/v1/games/${GAME}/frames`, 'x=1', MESSAGES.viewerRouteQuery],
    [
      'a frame png rejects a query',
      `/v1/games/${GAME}/frames/0.png`,
      'x=1',
      MESSAGES.viewerRouteQuery,
    ],
    [
      'latest.png rejects a query',
      `/v1/games/${GAME}/frames/latest.png`,
      'x=1',
      MESSAGES.viewerRouteQuery,
    ],
    ['video.mp4 rejects a query', `/v1/games/${GAME}/video.mp4`, 'x=1', MESSAGES.viewerRouteQuery],
    [
      'B2: an unroutable suffix with a query is the QUERY 400, not the 404',
      `/v1/games/${GAME}/nonsense`,
      'x=1',
      MESSAGES.viewerRouteQuery,
    ],
    [
      'B2 again: even a leading-zero frame is the query 400 once the id is valid',
      `/v1/games/${GAME}/frames/007.png`,
      'x=1',
      MESSAGES.viewerRouteQuery,
    ],
  ];

  test.each(QUERY_CASES)('%s', (_why, path, query, message) => {
    expect(refusalOf(path, query)).toEqual(badRequest(message));
  });
});

describe('400 — the body gate runs first (:1967)', () => {
  const BODY_CASES: ReadonlyArray<readonly [string, string, RequestBodySignal, string]> = [
    [
      'a GET body on a real route is refused without forwarding',
      `/v1/games/${GAME}/status`,
      'present',
      MESSAGES.getRequestBody,
    ],
    [
      'a GET body on an unroutable path is a 400, not a 404',
      '/nowhere',
      'present',
      MESSAGES.getRequestBody,
    ],
    [
      'a GET body on a bad id is a 400, not a 404',
      '/v1/games/short/status',
      'present',
      MESSAGES.getRequestBody,
    ],
    ['a GET body beats the health query gate', '/health', 'present', MESSAGES.getRequestBody],
    [
      'an unparseable Content-Length has its own message',
      `/v1/games/${GAME}/status`,
      'invalid-content-length',
      MESSAGES.invalidContentLength,
    ],
    [
      'and it beats the query gate as well',
      '/v1/games',
      'invalid-content-length',
      MESSAGES.invalidContentLength,
    ],
  ];

  test.each(BODY_CASES)('%s', (_why, path, body, message) => {
    expect(refusalOf(path, 'x=1', body)).toEqual(badRequest(message));
  });

  test('Content-Length: 0 is accepted — "absent" is the verdict, and it routes', () => {
    expect(routeOf('/health')).toEqual({ _tag: 'Health' });
  });
});

// ---------------------------------------------------------------------------
// 405 — the mapped verbs, and only them
// ---------------------------------------------------------------------------

describe('405 — mapped non-GET verbs', () => {
  const PATHS = [
    '/health',
    '/v1/games',
    `/v1/games/${GAME}/status`,
    `/v1/games/${GAME}/replay.json`,
    `/v1/games/${GAME}/join`,
    '/nowhere',
  ];

  test.each(
    GATEWAY_REJECTED_METHODS.flatMap((method) => PATHS.map((path) => [method, path] as const)),
  )('%s %s is 405 wherever it points', (method, path) => {
    expect(refusalOf(path, '', 'absent', method)).toEqual(METHOD_NOT_ALLOWED);
  });

  test('405 outranks the body 400 — do_HEAD never calls _reject_body', () => {
    expect(refusalOf('/health', '', 'present', 'HEAD')).toEqual(METHOD_NOT_ALLOWED);
    expect(refusalOf('/health', 'x=1', 'invalid-content-length', 'OPTIONS')).toEqual(
      METHOD_NOT_ALLOWED,
    );
  });

  test('GET is the only verb that reaches a route', () => {
    expect(GATEWAY_METHODS.filter((method) => Either.isRight(decide('/health', '', 'absent', method))))
      .toEqual(['GET']);
  });

  test('an unmapped verb is not dispatch’s business — the stdlib 501 is HTML', () => {
    expect(['TRACE', 'CONNECT', 'FOO', 'get'].some(isGatewayMethod)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Path normalization
// ---------------------------------------------------------------------------

describe('leading-slash collapse', () => {
  test.each([
    ['/v1/games', '/v1/games'],
    ['//v1/games', '/v1/games'],
    ['///v1/games', '/v1/games'],
    ['/', '/'],
    ['//', '/'],
    ['/v1//games', '/v1//games'],
    ['', ''],
  ])('collapse(%s) === %s', (path, expected) => {
    expect(collapseLeadingSlashes(path)).toBe(expected);
  });

  test('it is idempotent, so the socket edge may collapse the raw target first', () => {
    const once = collapseLeadingSlashes(`//v1/games/${GAME}/status`);
    expect(collapseLeadingSlashes(once)).toBe(once);
  });
});
