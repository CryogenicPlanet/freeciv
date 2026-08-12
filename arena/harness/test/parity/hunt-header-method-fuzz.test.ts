/**
 * The header/method fuzz **agreement** pins.
 *
 * An adversarial sweep of the header and method space — `Accept` and
 * `Accept-Encoding` negotiation, `Range` on binaries and on JSON, conditional
 * requests against routes that publish no validator, `Connection` and protocol
 * version, absolute-form and `Host` shapes, `HEAD` and `OPTIONS` on every route
 * class, and benign header bulk — turned up a short list of real divergences.
 * Those are **not** here: a divergence belongs in a finding, not in a test that
 * would then have to encode the wrong answer as expected.
 *
 * What is here is the other side of that sweep: the legs where the two
 * gateways answered **identically**, pinned so they cannot quietly start
 * disagreeing.  Every one was measured against the committed fixture tree, and
 * the interesting thing about most of them is that nothing happens:
 *
 * - **No content negotiation exists, and none may appear.**  `Accept:
 *   image/png` on `/v1/games`, an unsatisfiable `Accept`, a garbage `Accept`,
 *   two `Accept` fields — all of them get the same JSON. A `406` on either side
 *   would be a new behavior, not a fix.
 * - **No compression exists.**  `Accept-Encoding: gzip, deflate, br` gets the
 *   identical uncompressed bytes and no `Content-Encoding`, which is what keeps
 *   the byte comparison in `diff.test.ts` meaningful in the first place.
 * - **No range support exists.**  A `Range` on a JSON route is ignored by both:
 *   `200`, whole body, no `Content-Range`, no `Accept-Ranges`. (`Range` on the
 *   *disk-served PNG* is deliberately absent from this file — that route
 *   diverges on framing, which `waivers.ts#binary-disk-fallback-chunked` owns
 *   and the matrix polices.  It is also the leg that killed the obvious fix:
 *   with a `Bun.file` body the length survives and Bun answers this request
 *   `206 Partial Content`, where CPython ignores `Range` entirely — which is
 *   why the framing divergence was accepted and the `206` was not.)
 * - **No conditional-request support exists.**  `If-None-Match: *` on a route
 *   that publishes no `ETag` must be a `200`, never a `304`.
 * - **`HEAD` and every non-`GET` verb are `405 Allow: GET`, on every route
 *   class** — index, archive JSON, frames projection, PNG, mp4, derivation
 *   route, unroutable path — because `do_HEAD = _method_not_allowed` never
 *   looks at the path. A CORS preflight is that same `405`, with no
 *   `Access-Control-*` header anywhere in it.
 * - **`X-HTTP-Method-Override` is not honoured**, in either direction.
 * - **Credentials and forwarding headers change nothing**: a `Cookie`, an
 *   `Authorization` and four `X-Forwarded-*` fields leave the response
 *   byte-identical to the same request without them.
 *
 * The four `/health` legs take `normalizers.ts`'s volatile-field substitution
 * and drop `Content-Length` from the header comparison, for the same reason
 * `diff.test.ts` does: `pid` and `port` are bare integers two correct processes
 * differ on, and a digit changes the length.
 *
 * One scenario, a *refused* upstream (`127.0.0.1:1`), so every archive route is
 * answered from the disk fixture and no stub is needed. Both gateways bind
 * `--port 0` under a private `mkdtemp`; `boot.ts` refuses to run inside the
 * live stack's state directory.
 *
 * @module
 */

import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import {
  REFUSED_UPSTREAM_URL,
  aliveProcesses,
  bootGatewayPair,
  bootedPids,
  killAllBooted,
  unwrapPair,
  type GatewayPair,
} from './boot.ts';
import { normalizeHealthBody } from './normalizers.ts';
import {
  bodyLatin1,
  isWireResponse,
  wireRequest,
  type WireOutcome,
  type WireRequest,
} from './wire-client.ts';
import { PARITY_RUNS_ROOT } from './fixtures/scenarios.ts';
import { VALID_GAME_ID } from './fixtures/request-cases.ts';

// ---------------------------------------------------------------------------
// What is compared
// ---------------------------------------------------------------------------

/** `diff.test.ts`'s compared subset, plus the four this lens adds. */
const COMPARED_HEADERS: ReadonlyArray<string> = [
  'content-type',
  'cache-control',
  'allow',
  'x-content-type-options',
  'referrer-policy',
  'etag',
  'last-modified',
  'content-length',
  'content-range',
  'accept-ranges',
  'content-encoding',
  'vary',
];

/** Headers no response in this file may carry, whatever was asked for. */
const FORBIDDEN_HEADERS: ReadonlyArray<string> = [
  'content-range',
  'accept-ranges',
  'content-encoding',
  'access-control-allow-origin',
  'access-control-allow-methods',
  'access-control-allow-headers',
  'set-cookie',
];

/** Statuses that would mean a negotiation feature appeared. */
const FORBIDDEN_STATUSES: ReadonlyArray<number> = [206, 304, 406, 412, 416];

type BodyRule = 'bytes' | 'health';

/** One replayed request and what it proves. */
interface FuzzLeg {
  readonly name: string;
  readonly why: string;
  readonly request: WireRequest;
  readonly rule: BodyRule;
  /** CPython's measured status, transcribed from a run of `hunt.ts`. */
  readonly status: number;
  /** CPython's measured `Allow`, when the leg has one. */
  readonly allow?: string;
}

const STATUS_ROUTE = `/v1/games/${VALID_GAME_ID}/status`;
const FRAMES_ROUTE = `/v1/games/${VALID_GAME_ID}/frames`;
const PNG_ROUTE = `/v1/games/${VALID_GAME_ID}/frames/0.png`;
const MP4_ROUTE = `/v1/games/${VALID_GAME_ID}/video.mp4`;
const REPLAY_ROUTE = `/v1/games/${VALID_GAME_ID}/replay.json?limit=2`;

const leg = (
  name: string,
  why: string,
  status: number,
  request: WireRequest,
  extra: Partial<Pick<FuzzLeg, 'rule' | 'allow'>> = {},
): FuzzLeg => ({ name, why, status, request, rule: extra.rule ?? 'bytes', ...extra });

const g = (
  name: string,
  why: string,
  status: number,
  target: string,
  headers: ReadonlyArray<readonly [string, string]> = [],
  rule: BodyRule = 'bytes',
): FuzzLeg => leg(name, why, status, { method: 'GET', target, headers }, { rule });

/** Every non-`GET` verb is the same `405`, whatever the path is. */
const notAllowed = (name: string, why: string, request: WireRequest): FuzzLeg =>
  leg(name, why, 405, request, { allow: 'GET' });

// ---------------------------------------------------------------------------
// The legs
// ---------------------------------------------------------------------------

const NEGOTIATION_LEGS: ReadonlyArray<FuzzLeg> = [
  g('accept-none', 'no Accept at all: the baseline the rest are measured against', 200, '/v1/games'),
  g('accept-star', 'Accept: */* — the browser default', 200, '/v1/games', [['Accept', '*/*']]),
  g('accept-json-q', 'a full q-value Accept list changes nothing', 200, '/v1/games', [
    ['Accept', 'application/json;q=0.9, text/html;q=0.8, */*;q=0.1'],
  ]),
  g('accept-png-only-on-json', 'Accept: image/png on a JSON route is still JSON, not a 406', 200, '/v1/games', [
    ['Accept', 'image/png'],
  ]),
  g('accept-impossible', 'an Accept nothing can satisfy is still served', 200, STATUS_ROUTE, [
    ['Accept', 'application/x-nope'],
  ]),
  g('accept-garbage', 'an Accept no parser can grade', 200, '/v1/games', [['Accept', ')(*&^%$#@!']]),
  g('accept-duplicated', 'two Accept fields — joined by one runtime, first-wins by the other, read by neither', 200, '/v1/games', [
    ['Accept', 'application/json'],
    ['Accept', 'image/png'],
  ]),
  g('accept-empty', 'an empty Accept', 200, '/v1/games', [['Accept', '']]),
  g('accept-encoding-gzip', 'gzip offered: neither gateway compresses, which is what keeps the byte rule honest', 200, '/v1/games', [
    ['Accept-Encoding', 'gzip, deflate, br'],
  ]),
  g('accept-encoding-identity', 'identity only', 200, '/v1/games', [['Accept-Encoding', 'identity']]),
  g('accept-charset-language', 'two more negotiation headers nothing reads', 200, STATUS_ROUTE, [
    ['Accept-Charset', 'utf-8'],
    ['Accept-Language', 'en-GB, fr;q=0.5'],
  ]),
];

const RANGE_LEGS: ReadonlyArray<FuzzLeg> = [
  g('range-json-index', 'a Range on the JSON index is ignored: 200, whole body, no Content-Range', 200, '/v1/games', [
    ['Range', 'bytes=0-9'],
  ]),
  g('range-json-status', 'the same on an archive JSON route', 200, STATUS_ROUTE, [['Range', 'bytes=0-9']]),
  g('range-404-route', 'a Range on a 404: the error body is not ranged either', 404, '/nope', [
    ['Range', 'bytes=0-3'],
  ]),
];

const CONDITIONAL_LEGS: ReadonlyArray<FuzzLeg> = [
  g('inm-star-json', 'If-None-Match: * on a route with no ETag must be 200, never 304', 200, '/v1/games', [
    ['If-None-Match', '*'],
  ]),
  g('inm-star-status', 'the same on the archive JSON route', 200, STATUS_ROUTE, [['If-None-Match', '*']]),
  g('inm-star-mp4', 'and on a route that 404s: the condition never short-circuits the miss', 404, MP4_ROUTE, [
    ['If-None-Match', '*'],
  ]),
];

const CONNECTION_LEGS: ReadonlyArray<FuzzLeg> = [
  leg('connection-close-explicit', 'the client asks for a close', 200, {
    method: 'GET',
    target: '/health',
    headers: [['Connection', 'close']],
  }, { rule: 'health' }),
  leg('connection-keep-alive', 'the client asks to keep the connection: the payload is unchanged', 200, {
    method: 'GET',
    target: '/health',
    autoConnectionClose: false,
    headers: [['Connection', 'keep-alive']],
  }, { rule: 'health' }),
  leg('connection-none-http11', 'HTTP/1.1 with no Connection field', 200, {
    method: 'GET',
    target: '/health',
    autoConnectionClose: false,
  }, { rule: 'health' }),
  leg('connection-none-http10', 'HTTP/1.0 with no Connection field', 200, {
    method: 'GET',
    target: '/health',
    httpVersion: 'HTTP/1.0',
    autoConnectionClose: false,
  }, { rule: 'health' }),
  leg('connection-keep-alive-http10', 'the HTTP/1.0 keep-alive handshake', 200, {
    method: 'GET',
    target: '/health',
    httpVersion: 'HTTP/1.0',
    autoConnectionClose: false,
    headers: [['Connection', 'keep-alive']],
  }, { rule: 'health' }),
  leg('connection-garbage-token', 'a Connection token neither side defines', 200, {
    method: 'GET',
    target: '/health',
    autoConnectionClose: false,
    headers: [['Connection', 'frobnicate']],
  }, { rule: 'health' }),
  leg('connection-lists-a-header', 'Connection naming a hop-by-hop field that is also sent', 200, {
    method: 'GET',
    target: '/health',
    autoConnectionClose: false,
    headers: [
      ['Connection', 'close, x-hop'],
      ['X-Hop', '1'],
    ],
  }, { rule: 'health' }),
  g('expect-unknown', 'an Expect nobody implements is ignored, not answered with 417', 200, '/health', [
    ['Expect', 'the-unexpected'],
  ], 'health'),
];

const TARGET_FORM_LEGS: ReadonlyArray<FuzzLeg> = [
  leg('absolute-form-foreign-authority', 'absolute-form: the authority is ignored and the path routes', 200, {
    method: 'GET',
    target: 'http://evil.invalid:9999/v1/games',
  }),
  leg('absolute-form-https', 'an https scheme on a plaintext socket is still just a path', 200, {
    method: 'GET',
    target: 'https://evil.invalid/v1/games',
  }),
  leg('absolute-form-no-path', 'absolute-form with an empty path is the unroutable root', 404, {
    method: 'GET',
    target: 'http://evil.invalid',
  }),
  leg('absolute-form-userinfo', 'userinfo in the target does not change which route answers', 200, {
    method: 'GET',
    target: 'http://user:pass@evil.invalid/health',
  }, { rule: 'health' }),
  leg('duplicate-host-header', 'two Host fields — the request-smuggling shape — change nothing', 200, {
    method: 'GET',
    target: '/v1/games',
    autoHost: false,
    headers: [
      ['Host', '127.0.0.1'],
      ['Host', 'evil.invalid'],
    ],
  }),
  leg('foreign-host-header', 'a Host naming somebody else: no virtual hosting exists', 200, {
    method: 'GET',
    target: '/v1/games',
    autoHost: false,
    headers: [['Host', 'evil.invalid']],
  }),
  leg('empty-host-header', 'an empty Host', 200, {
    method: 'GET',
    target: '/v1/games',
    autoHost: false,
    headers: [['Host', '']],
  }),
];

const METHOD_LEGS: ReadonlyArray<FuzzLeg> = [
  notAllowed('head-index', 'HEAD on the JSON index', { method: 'HEAD', target: '/v1/games' }),
  notAllowed('head-status', 'HEAD on an archive route', { method: 'HEAD', target: STATUS_ROUTE }),
  notAllowed('head-frames', 'HEAD on the frames projection', { method: 'HEAD', target: FRAMES_ROUTE }),
  notAllowed('head-png', 'HEAD on the local binary route', { method: 'HEAD', target: PNG_ROUTE }),
  notAllowed('head-mp4', 'HEAD on the mp4 route', { method: 'HEAD', target: MP4_ROUTE }),
  notAllowed('head-replay', 'HEAD on a derivation route: the 405 precedes the subprocess', {
    method: 'HEAD',
    target: REPLAY_ROUTE,
  }),
  notAllowed('head-unknown-path', 'HEAD on an unroutable path: 405 outranks 404', {
    method: 'HEAD',
    target: '/nope',
  }),
  notAllowed('head-health-query', 'HEAD where the same query would be a 400 for GET', {
    method: 'HEAD',
    target: '/health?x=1',
  }),
  notAllowed('head-with-range', 'HEAD plus a Range', {
    method: 'HEAD',
    target: PNG_ROUTE,
    headers: [['Range', 'bytes=0-3']],
  }),
  notAllowed('options-index', 'OPTIONS on the index', { method: 'OPTIONS', target: '/v1/games' }),
  notAllowed('options-png', 'OPTIONS on the binary route', { method: 'OPTIONS', target: PNG_ROUTE }),
  notAllowed('options-cors-preflight', 'a real preflight is the plain 405, with no Access-Control-* anywhere', {
    method: 'OPTIONS',
    target: STATUS_ROUTE,
    headers: [
      ['Origin', 'https://evil.invalid'],
      ['Access-Control-Request-Method', 'GET'],
      ['Access-Control-Request-Headers', 'authorization'],
    ],
  }),
  notAllowed('patch-png', 'PATCH on the binary route', { method: 'PATCH', target: PNG_ROUTE }),
  notAllowed('post-index-with-body', 'POST with a body the 405 must not read', {
    method: 'POST',
    target: '/v1/games',
    headers: [['Content-Type', 'application/json']],
    body: '{"x":1}',
  }),
  notAllowed('method-override-header', 'X-HTTP-Method-Override must not turn a POST into a GET', {
    method: 'POST',
    target: '/health',
    headers: [['X-HTTP-Method-Override', 'GET']],
  }),
  g('get-with-override-to-delete', 'nor a GET into a DELETE', 200, '/health', [
    ['X-HTTP-Method-Override', 'DELETE'],
  ], 'health'),
];

const BULK_HEADER_LEGS: ReadonlyArray<FuzzLeg> = [
  g(
    'many-headers-20',
    'twenty distinct benign fields',
    200,
    '/health',
    Array.from({ length: 20 }, (_, index) => [`X-Pad-${String(index)}`, String(index)] as const),
    'health',
  ),
  g(
    'many-headers-90',
    'ninety of them — inside both parsers\' limits, which are otherwise very different',
    200,
    '/health',
    Array.from({ length: 90 }, (_, index) => [`X-Pad-${String(index)}`, String(index)] as const),
    'health',
  ),
  g(
    'duplicated-benign-header-x12',
    'the same benign field twelve times',
    200,
    '/health',
    Array.from({ length: 12 }, (_, index) => ['X-Trace', String(index)] as const),
    'health',
  ),
  g('long-header-value-8k', 'an 8 KiB field value, inside both caps', 200, '/health', [
    ['X-Pad', 'a'.repeat(8192)],
  ], 'health'),
  g('long-header-name', 'a 4 KiB field name', 200, '/health', [['X-'.padEnd(4096, 'n'), '1']], 'health'),
  g('empty-header-value', 'a field with no value at all', 200, '/health', [['X-Empty', '']], 'health'),
  g('header-name-odd-case', 'field names are case-insensitive on both sides', 200, '/health', [
    ['aCcEpT', '*/*'],
    ['x-TRACE', '1'],
  ], 'health'),
  g('header-value-leading-space', 'optional whitespace around a field value is stripped by both', 200, '/health', [
    ['X-Pad', '   spaced   '],
  ], 'health'),
  g('header-value-with-obs-text', 'a high-byte field value', 200, '/health', [['X-Pad', 'café']], 'health'),
  g('cookie-and-authorization', 'credentials change nothing and reach no upstream', 200, STATUS_ROUTE, [
    ['Cookie', 'session=secret'],
    ['Authorization', 'Bearer secret'],
  ]),
  g('forwarding-headers', 'forwarding headers are neither relayed nor reflected', 200, STATUS_ROUTE, [
    ['X-Forwarded-For', '10.0.0.1'],
    ['X-Forwarded-Host', 'evil.invalid'],
    ['X-Forwarded-Proto', 'https'],
    ['Forwarded', 'for=10.0.0.1;host=evil.invalid;proto=https'],
  ]),
  g('user-agent-and-referer', 'the two commonest benign fields', 200, '/v1/games', [
    ['User-Agent', 'parity-hunt/1.0'],
    ['Referer', 'https://evil.invalid/'],
  ]),
  g('te-and-upgrade', 'TE and Upgrade offered on a plain GET: no protocol switch is on offer', 200, '/health', [
    ['TE', 'trailers'],
    ['Upgrade', 'h2c'],
  ], 'health'),
];

const AGREEING_LEGS: ReadonlyArray<FuzzLeg> = [
  ...NEGOTIATION_LEGS,
  ...RANGE_LEGS,
  ...CONDITIONAL_LEGS,
  ...CONNECTION_LEGS,
  ...TARGET_FORM_LEGS,
  ...METHOD_LEGS,
  ...BULK_HEADER_LEGS,
];

// ---------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------

/** One leg's two outcomes, keyed by leg name. */
interface Sided {
  readonly python: WireOutcome;
  readonly typescript: WireOutcome;
}

/** A single mutable cell, so nothing here is a module-level `let`. */
const state: { pair: GatewayPair | null; outcomes: ReadonlyMap<string, Sided> } = {
  pair: null,
  outcomes: new Map(),
};

const LEG_TIMEOUT_MS = 20_000;
const SUITE_TIMEOUT_MS = 300_000;

const bodyFor = (outcome: WireOutcome, rule: BodyRule): string => {
  if (!isWireResponse(outcome)) return `<${outcome._tag}>`;
  const raw = bodyLatin1(outcome);
  return rule === 'health' && outcome.status === 200 ? normalizeHealthBody(raw).text : raw;
};

const headersFor = (
  outcome: WireOutcome,
  rule: BodyRule,
): Readonly<Record<string, string | null>> => {
  // `/health` reports `pid` and `port` as bare integers, so two correct
  // processes can differ by a digit and the length moves with them.  Each
  // side's own self-consistency is asserted separately.
  const names =
    rule === 'health' ? COMPARED_HEADERS.filter((name) => name !== 'content-length') : COMPARED_HEADERS;
  return Object.fromEntries(
    names.map((name) => [name, isWireResponse(outcome) ? outcome.headers.get(name) : null]),
  );
};

const sidedFor = (name: string): Sided => {
  const sided = state.outcomes.get(name);
  if (sided === undefined) throw new Error(`leg ${name} was never replayed`);
  return sided;
};

const responseFor = (outcome: WireOutcome, side: string, name: string) => {
  if (!isWireResponse(outcome)) {
    throw new Error(`${side} did not answer leg ${name}: ${JSON.stringify(outcome)}`);
  }
  return outcome;
};

beforeAll(async () => {
  const pair = unwrapPair(
    await bootGatewayPair({
      runsRoot: PARITY_RUNS_ROOT,
      // A *refused* upstream, not an unroutable one: refusal is instant on both
      // sides and lands every archive route on the disk fixture, which is what
      // this file's legs are about.  The unroutable fixture would spend the
      // configured timeout per leg and exercise a defect this file does not pin.
      serviceUrl: REFUSED_UPSTREAM_URL,
      scenario: 'hdrfuzz-agree',
      upstreamTimeoutSeconds: 2,
      viewerPublicUrl: 'http://viewer.parity.invalid',
    }),
  );
  state.pair = pair;
  state.outcomes = await AGREEING_LEGS.reduce<Promise<ReadonlyMap<string, Sided>>>(
    async (previous, entry) => {
      const request: WireRequest = { ...entry.request, timeoutMs: LEG_TIMEOUT_MS };
      const python = await wireRequest(pair.python.origin, request);
      const typescript = await wireRequest(pair.typescript.origin, request);
      return new Map(await previous).set(entry.name, { python, typescript });
    },
    Promise.resolve(new Map()),
  );
}, SUITE_TIMEOUT_MS);

afterAll(async () => {
  const stopped = await state.pair?.stop();
  state.pair?.cleanup();
  const killed = await killAllBooted();
  expect({ orphans: stopped?.orphans ?? [], killed, alive: aliveProcesses(bootedPids()) }).toEqual({
    orphans: [],
    killed: [],
    alive: [],
  });
});

describe('header/method fuzz: the legs where both gateways agree', () => {
  test('every leg was replayed against both gateways', () => {
    expect(state.outcomes.size).toBe(AGREEING_LEGS.length);
  });

  AGREEING_LEGS.forEach((entry) => {
    test(`${entry.name}: ${entry.why}`, () => {
      const { python, typescript } = sidedFor(entry.name);
      const left = responseFor(python, 'python', entry.name);
      const right = responseFor(typescript, 'typescript', entry.name);

      // CPython is the oracle: its status is transcribed from a measured run,
      // and the port is compared against it rather than against itself.
      expect({ python: left.status, typescript: right.status }).toEqual({
        python: entry.status,
        typescript: entry.status,
      });
      expect(right.reasonLine).toBe(left.reasonLine);
      expect(headersFor(right, entry.rule)).toEqual(headersFor(left, entry.rule));
      expect(bodyFor(right, entry.rule)).toBe(bodyFor(left, entry.rule));
      if (entry.allow !== undefined) {
        expect({ python: left.headers.get('allow'), typescript: right.headers.get('allow') }).toEqual({
          python: entry.allow,
          typescript: entry.allow,
        });
      }
    });
  });

  test('no leg produced a negotiation status on either side', () => {
    const offenders = AGREEING_LEGS.flatMap((entry) => {
      const { python, typescript } = sidedFor(entry.name);
      return [
        ['python', python] as const,
        ['typescript', typescript] as const,
      ].flatMap(([side, outcome]) =>
        isWireResponse(outcome) && FORBIDDEN_STATUSES.includes(outcome.status)
          ? [`${entry.name}/${side}: ${String(outcome.status)}`]
          : [],
      );
    });
    expect(offenders).toEqual([]);
  });

  test('no leg produced a range, compression, CORS or cookie header on either side', () => {
    const offenders = AGREEING_LEGS.flatMap((entry) => {
      const { python, typescript } = sidedFor(entry.name);
      return [
        ['python', python] as const,
        ['typescript', typescript] as const,
      ].flatMap(([side, outcome]) =>
        isWireResponse(outcome)
          ? FORBIDDEN_HEADERS.flatMap((name) =>
              outcome.headers.has(name) ? [`${entry.name}/${side}: ${name}`] : [],
            )
          : [],
      );
    });
    expect(offenders).toEqual([]);
  });

  test("each side's Content-Length matches its own body, /health included", () => {
    const offenders = AGREEING_LEGS.flatMap((entry) => {
      const { python, typescript } = sidedFor(entry.name);
      return [
        ['python', python] as const,
        ['typescript', typescript] as const,
      ].flatMap(([side, outcome]) => {
        if (!isWireResponse(outcome)) return [];
        const declared = outcome.headers.get('content-length');
        // A `HEAD` response declares a length it is defined not to send.
        const skip = entry.request.method === 'HEAD' || declared === null;
        return skip || Number(declared) === outcome.bodyBytes.byteLength
          ? []
          : [`${entry.name}/${side}: declared ${declared}, sent ${String(outcome.bodyBytes.byteLength)}`];
      });
    });
    expect(offenders).toEqual([]);
  });
});
