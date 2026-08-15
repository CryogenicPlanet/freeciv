/** Health, games-index, and archive JSON fallback behavior matrices. */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import {
  CANON_UTF8,
  canonicalBytes,
  type CanonValue,
  decodeJsonValueFromString,
  Gateway,
  type GameId,
  isGameId,
  isJsonObject,
  type JsonObject,
  type JsonValue,
} from '@arena/wire';
import { Predicate, Effect, Either, Layer, Option } from 'effect';
import { createHash } from 'node:crypto';
import { mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import type { GatewayConfigValues } from 'src/gateway/config.ts';
import { HttpServerResponse } from '@effect/platform';
import type { GatewayError } from 'src/gateway/errors.ts';
import type { ArchiveResultRoute, ArchiveStatusRoute } from 'src/gateway/http/dispatch.ts';
import {
  boundedGatewayJson,
  gatewayJson,
  type GatewayJsonPayload,
} from 'src/gateway/http/json.ts';
import {
  archiveJsonRoute,
  type ArchiveRouteOptions,
} from 'src/gateway/http/routes/archive.ts';
import { gamesIndexRoute } from 'src/gateway/http/routes/games.ts';
import {
  GatewayIdentity,
  healthRoute,
  identityPayload,
  layer as identityLayer,
  makeGatewayIdentity,
} from 'src/gateway/http/routes/health.ts';
import { RunsRepository, layer as runsLayer } from 'src/gateway/services/runs.ts';
import type { FetchLike, UpstreamClient } from 'src/gateway/services/upstream.ts';
import { upstreamLayerTest } from './support/upstream.ts';

// ---------------------------------------------------------------------------
// Fixture tree
// ---------------------------------------------------------------------------

const REPO_ROOT = resolve(import.meta.dir, '../../../..');
const FIXTURES = join(REPO_ROOT, 'arena/wire/test/fixtures/runs');

const asGameId = (value: string): GameId => {
  if (!isGameId(value)) throw new Error(`fixture id is not a GameId: ${value}`);
  return value;
};

/** Terminal, with a matching `report.json`: the only run an archive route can serve. */
const COMPLETED = asGameId('game_ieTomdES08hpUmFRFzCOAVMo');
/** Non-terminal with a `replay.jsonl`: relabelled `interrupted` on the offline branch. */
const RUNNING = asGameId('game_QAoITB7qSmKNSwsXX6LaZG8H');
/** A lobby husk — non-terminal, no replay tail.  Dropped when relabelling, visible raw. */
const HUSK = asGameId('game_huskNoReplayFile0001');

const readFixture = (kind: string, name: string): JsonObject => {
  const document = decodeJsonValueFromString(
    readFileSync(join(FIXTURES, kind, `${name}.json`), 'utf8'),
  );
  if (Either.isLeft(document) || !isJsonObject(document.right)) {
    throw new Error(`fixture ${kind}/${name}.json is not a JSON object`);
  }
  return document.right;
};

const writeJson = <Value>(path: string, value: Value): void => {
  writeFileSync(path, `${JSON.stringify(value)}\n`, 'utf8');
};

interface RunSpec {
  readonly id: GameId;
  readonly manifest: string;
  readonly report?: string;
  readonly replay?: string;
}

const writeRun = (root: string, spec: RunSpec): void => {
  const directory = join(root, spec.id);
  mkdirSync(directory, { recursive: true });
  writeJson(join(directory, 'manifest.json'), {
    ...readFixture('manifest', spec.manifest),
    game_id: spec.id,
  });
  if (spec.report !== undefined) {
    const report = readFixture('report', spec.report);
    const manifest = report['manifest'];
    const reportManifest = isJsonObject(manifest)
      ? { ...manifest, game_id: spec.id }
      : { game_id: spec.id };
    writeJson(join(directory, 'report.json'), {
      ...report,
      manifest: reportManifest,
    });
  }
  if (spec.replay !== undefined) {
    writeFileSync(join(directory, 'replay.jsonl'), spec.replay, 'utf8');
  }
};

/** A torn tail: the last write never finished, so the scan walks back to 44. */
const TORN_REPLAY = [
  '{"schema_version":1,"turn":41,"kind":"turn"}',
  '{"schema_version":1,"turn":44,"kind":"turn"}',
  '{"schema_version":1,"turn":9',
].join('\n');

const buildTree = (): string => {
  const root = realpathSync(mkdtempSync(join(tmpdir(), 'arena-routes-')));
  writeRun(root, {
    id: COMPLETED,
    manifest: 'completed-strategic-v1-multiplayer',
    report: 'completed-two-seats-full-score',
  });
  writeRun(root, { id: RUNNING, manifest: 'running-v2-multiplayer', replay: TORN_REPLAY });
  writeRun(root, { id: HUSK, manifest: 'running-v2-multiplayer' });
  return root;
};

// ---------------------------------------------------------------------------
// Configuration, identity, and the scripted upstream
// ---------------------------------------------------------------------------

const SERVICE_URL = 'http://127.0.0.1:8811';
const VIEWER_URL = 'https://freeciv.localhost';
const BOUND_PORT = 45123;
const PID = 4242;

const configFor = (runsRoot: string, viewerPublicUrl: Option.Option<string>): GatewayConfigValues => {
  const repoRoot = join(runsRoot, 'checkout');
  const cacheRoot = join(runsRoot, 'cache');
  const material = [
    repoRoot,
    SERVICE_URL,
    runsRoot,
    cacheRoot,
    Option.getOrElse(viewerPublicUrl, () => ''),
  ].join('\0');
  return {
    repoRoot,
    upstreamServiceUrl: SERVICE_URL,
    runsRoot,
    cacheRoot,
    identity: createHash('sha256').update(material, 'utf8').digest('hex').slice(0, 20),
    upstreamTimeoutSeconds: 10,
    viewerPublicUrl,
    host: '127.0.0.1',
    port: 0,
    readyFile: join(runsRoot, 'ready.json'),
  };
};

interface Upstream {
  /** Every URL the client asked for, in order. */
  readonly urls: () => readonly string[];
  readonly fetch: FetchLike;
}

/** A `fetch` that records its calls and answers with whatever the test scripted. */
const scriptedUpstream = (answer: () => Promise<Response>): Upstream => {
  const urls: string[] = [];
  return {
    urls: () => urls,
    fetch: (url) => {
      urls.push(url);
      return answer();
    },
  };
};

const jsonUpstream = (status: number, body: string): Upstream =>
  scriptedUpstream(() =>
    Promise.resolve(
      new Response(status === 204 ? null : body, {
        status,
        headers: { 'Content-Type': 'application/json' },
      }),
    ),
  );

/** The three-condition Portless signature: the only 502 that means "offline". */
const portlessUpstream = (): Upstream =>
  scriptedUpstream(() =>
    Promise.resolve(
      new Response('<html>gone</html>', {
        status: 502,
        headers: { 'Content-Type': 'text/html; charset=utf-8', 'X-Portless': '1' },
      }),
    ),
  );

/** A transport failure: connection refused, DNS, a socket that never answers. */
const refusedUpstream = (): Upstream =>
  scriptedUpstream(() => Promise.reject(new Error('connect ECONNREFUSED')));

interface RunsRootRef {
  current: string;
}

const runsRootRef: RunsRootRef = { current: '' };

const layersFor = (
  upstream: Upstream,
  viewerPublicUrl: Option.Option<string> = Option.none(),
): Layer.Layer<GatewayIdentity | RunsRepository | UpstreamClient> => {
  const config = configFor(runsRootRef.current, viewerPublicUrl);
  return Layer.mergeAll(
    runsLayer(config.runsRoot),
    upstreamLayerTest({ serviceUrl: SERVICE_URL, fetch: upstream.fetch }),
    identityLayer({ config, boundPort: BOUND_PORT, pid: PID }),
  );
};

const run = <A>(
  effect: Effect.Effect<A, GatewayError, GatewayIdentity | RunsRepository | UpstreamClient>,
  upstream: Upstream,
  viewerPublicUrl: Option.Option<string> = Option.none(),
): Promise<Either.Either<A, GatewayError>> =>
  Effect.runPromise(
    Effect.either(effect).pipe(Effect.provide(layersFor(upstream, viewerPublicUrl))),
  );

// ---------------------------------------------------------------------------
// Small readers
// ---------------------------------------------------------------------------

const text = (payload: GatewayJsonPayload): string => new TextDecoder().decode(payload.body);

const parsed = (payload: GatewayJsonPayload): JsonValue => {
  const value = decodeJsonValueFromString(text(payload));
  if (Either.isLeft(value)) throw new Error(`response body is not JSON: ${text(payload)}`);
  return value.right;
};

const rowsOf = (payload: GatewayJsonPayload): readonly JsonObject[] => {
  const document = parsed(payload);
  const games = isJsonObject(document) ? document['games'] : undefined;
  if (!Array.isArray(games)) throw new Error(`no games array in ${text(payload)}`);
  return games.filter(isJsonObject);
};

const rowFor = (payload: GatewayJsonPayload, id: string): JsonObject | undefined =>
  rowsOf(payload).find((row) => row['game_id'] === id);

const field = (value: JsonValue | undefined, key: string): JsonValue | undefined =>
  isJsonObject(value) ? value[key] : undefined;

const okOf = <A>(result: Either.Either<A, GatewayError>): A => {
  if (Either.isLeft(result)) {
    throw new Error(`expected success, got ${result.left._tag}: ${result.left.message}`);
  }
  return result.right;
};

const errorOf = <A>(result: Either.Either<A, GatewayError>): GatewayError => {
  if (Either.isRight(result)) throw new Error('expected a failure');
  return result.left;
};

const canonical = (value: CanonValue): Uint8Array =>
  Either.getOrThrowWith(canonicalBytes(value, CANON_UTF8), (error) => new Error(error._tag));

const statusRoute = (id: GameId, upstreamPath: string): ArchiveStatusRoute => ({
  _tag: 'ArchiveStatus',
  gameId: id,
  upstreamPath,
  bareId: false,
});

const resultRoute = (id: GameId): ArchiveResultRoute => ({
  _tag: 'ArchiveResult',
  gameId: id,
  upstreamPath: `/v1/games/${id}/result`,
});

/**
 * The archive JSON route as `../../src/gateway/server.ts` wires it — the copy
 * in `http/routes/archive.ts`.
 *
 * This block used to drive a *second* `archiveJsonRoute` that lived in
 * `http/routes/games.ts` and answered no request at all: the server dispatches
 * all four JSON archive views to `routes/archive.ts`.  The two agreed, and the
 * fifteen assertions below were still proving a property of dead code.  The
 * dead copy is gone and these now run against the served one, which answers
 * with an `HttpServerResponse` rather than a payload — so the bytes are read
 * back off the response and `source` (a provenance field only the payload
 * carries) is replaced by the stronger claim it stood for: the exact upstream
 * bytes, or the exact bytes the canonical writer would have produced.
 */
const archivePayload = async (
  route: ArchiveResultRoute | ArchiveStatusRoute,
  upstream: Upstream,
  viewerPublicUrl: Option.Option<string> = Option.none(),
): Promise<Either.Either<GatewayJsonPayload, GatewayError>> => {
  const options: ArchiveRouteOptions = {
    base: Option.getOrElse(viewerPublicUrl, () => `http://127.0.0.1:${String(BOUND_PORT)}`),
    absoluteWatch: Option.isSome(viewerPublicUrl),
  };
  const served = await run(
    Effect.map(archiveJsonRoute(route, options), (response) =>
      HttpServerResponse.toWeb(response),
    ),
    upstream,
    viewerPublicUrl,
  );
  if (Either.isLeft(served)) return Either.left(served.left);
  const body = new Uint8Array(await served.right.arrayBuffer());
  return Either.right({ status: served.right.status, body, source: 'gateway' });
};

beforeAll(() => {
  runsRootRef.current = buildTree();
});

afterAll(() => {
  if (runsRootRef.current !== '') rmSync(runsRootRef.current, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// /health
// ---------------------------------------------------------------------------

describe('/health', () => {
  test('serves the identity payload, canonically, without touching upstream', async () => {
    const upstream = refusedUpstream();
    const payload = okOf(await run(healthRoute, upstream));

    expect(payload.status).toBe(200);
    expect(upstream.urls()).toEqual([]);

    const config = configFor(runsRootRef.current, Option.none());
    expect(payload.body).toEqual(
      canonical(identityPayload({ config, boundPort: BOUND_PORT, pid: PID })),
    );
    // `local_stack.py`'s `_wait_http` greps the raw body for this exact run of
    // bytes; the canonical writer emits no space after the colon.
    expect(text(payload)).toContain('"ok":true');
  });

  test('the payload decodes as wire\'s GatewayIdentity and its digest closes', async () => {
    const payload = okOf(await run(healthRoute, refusedUpstream()));
    const document = Gateway.decodeGatewayIdentity(parsed(payload));
    expect(Either.isRight(document)).toBe(true);
    if (Either.isLeft(document)) return;

    const identity = document.right;
    expect(identity.kind).toBe(Gateway.GATEWAY_KIND);
    expect(identity.ok).toBe(true);
    expect(identity.port).toBe(BigInt(BOUND_PORT));
    expect(identity.pid).toBe(BigInt(PID));
    expect(identity.url).toBe(`http://127.0.0.1:${String(BOUND_PORT)}`);
    expect(identity.viewer_public_url).toBeUndefined();

    // The strongest available proof the four fields were read with the right
    // meanings: rebuild `_identity`'s preimage from the payload alone.
    const digest = createHash('sha256')
      .update(Gateway.gatewayIdentityMaterial(identity), 'utf8')
      .digest('hex')
      .slice(0, 20);
    expect(String(identity.identity)).toBe(digest);
  });

  test('viewer_public_url is omit-or-present, and it moves the archive base', () => {
    const runsRoot = runsRootRef.current;
    const without = makeGatewayIdentity({
      config: configFor(runsRoot, Option.none()),
      boundPort: BOUND_PORT,
      pid: PID,
    });
    const with_ = makeGatewayIdentity({
      config: configFor(runsRoot, Option.some(VIEWER_URL)),
      boundPort: BOUND_PORT,
      pid: PID,
    });

    expect(Object.hasOwn(without.payload, 'viewer_public_url')).toBe(false);
    expect(with_.payload['viewer_public_url']).toBe(VIEWER_URL);
    expect(without.archiveBase).toBe(`http://127.0.0.1:${String(BOUND_PORT)}`);
    expect(without.absoluteWatch).toBe(false);
    expect(with_.archiveBase).toBe(VIEWER_URL);
    expect(with_.absoluteWatch).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// _json / _bounded_json
// ---------------------------------------------------------------------------

describe('the two serializers', () => {
  test('_json has no ceiling and _bounded_json answers 503 over 8 MiB', async () => {
    const oversize: CanonValue = { blob: 'x'.repeat(8 * 1024 * 1024) };

    const unbounded = await Effect.runPromise(Effect.either(gatewayJson(oversize)));
    expect(Either.isRight(unbounded)).toBe(true);

    const bounded = await Effect.runPromise(Effect.either(boundedGatewayJson(oversize)));
    expect(Either.isLeft(bounded)).toBe(true);
    if (Either.isLeft(bounded)) {
      expect(bounded.left._tag).toBe('ArchiveUnavailable');
      expect(bounded.left.status).toBe(503);
      expect(bounded.left.message).toBe(
        Gateway.GATEWAY_PROBLEM_MESSAGES.archiveJsonTooLarge,
      );
    }
  });

  test('integers keep CPython\'s int spelling', async () => {
    const payload = await Effect.runPromise(gatewayJson({ turn: 7n, score: 7 }));
    // A `bigint` is a Python `int`, a `number` is a Python `float`; keys sorted,
    // no separator spaces.  This is `_canonical`, not `JSON.stringify`.
    expect(text(payload)).toBe('{"score":7.0,"turn":7}');
  });
});

// ---------------------------------------------------------------------------
// /v1/games — the four branches
// ---------------------------------------------------------------------------

describe('/v1/games', () => {
  test('offline relabels non-terminal runs and drops the lobby husk', async () => {
    const upstream = refusedUpstream();
    const payload = okOf(await run(gamesIndexRoute, upstream));

    expect(payload.status).toBe(200);
    expect(upstream.urls()).toEqual([`${SERVICE_URL}/v1/games`]);

    const running = rowFor(payload, RUNNING);
    expect(field(running, 'state')).toBe('interrupted');
    // `max(manifest current_turn, replay tail)` — the torn last line is skipped.
    expect(field(running, 'current_turn')).toBe(44);
    expect(field(field(running, 'outcome'), 'status')).toBe('interrupted');
    const summary = field(field(running, 'outcome'), 'summary');
    expect(Predicate.isString(summary) && summary.includes('turn 44')).toBe(true);

    // A husk with no recorded turn stays hidden.
    expect(rowFor(payload, HUSK)).toBeUndefined();
    expect(field(rowFor(payload, COMPLETED), 'state')).toBe('completed');
  });

  test('the portless 502 is the same offline branch, byte for byte', async () => {
    const refused = okOf(await run(gamesIndexRoute, refusedUpstream()));
    const portless = okOf(await run(gamesIndexRoute, portlessUpstream()));
    expect(portless.body).toEqual(refused.body);
  });

  test('an upstream 404 serves the disk index RAW — no relabeling, husk visible', async () => {
    const payload = okOf(await run(gamesIndexRoute, jsonUpstream(404, '{"error":"nope"}')));

    const running = rowFor(payload, RUNNING);
    expect(field(running, 'state')).toBe('running');
    expect(field(field(running, 'outcome'), 'status')).toBe('pending');
    // The value that is unreachable through any other branch.
    expect(rowFor(payload, HUSK)).toBeDefined();
    expect(field(rowFor(payload, HUSK), 'state')).toBe('running');
  });

  test('405 falls back exactly as 404 does', async () => {
    const notFound = okOf(await run(gamesIndexRoute, jsonUpstream(404, '{}')));
    const notAllowed = okOf(await run(gamesIndexRoute, jsonUpstream(405, '{}')));
    expect(notAllowed.body).toEqual(notFound.body);
  });

  test('the two disk branches disagree — which is the point of the matrix', async () => {
    const offline = okOf(await run(gamesIndexRoute, refusedUpstream()));
    const upstream404 = okOf(await run(gamesIndexRoute, jsonUpstream(404, '{}')));
    expect(upstream404.body).not.toEqual(offline.body);
  });

  test('a 2xx is merged and RE-SERIALIZED, upstream rows first, live ids winning', async () => {
    // Deliberately unsorted keys, spaces, and an integer that a JS-number port
    // would re-emit as `7.0`.
    const body = `{ "schema_version": 1, "games": [ {"game_id": "${RUNNING}", "state": "running", "current_turn": 7} ] }`;
    const upstream = jsonUpstream(200, body);
    const payload = okOf(await run(gamesIndexRoute, upstream));

    // Not a relay: the bytes are the canonical writer's, not the upstream's —
    // keys sorted, spaces gone, and `7` still spelled as a Python `int`.  A
    // port that let `JSON.parse` turn it into a JS number writes `7.0` here.
    expect(text(payload)).not.toBe(body);
    expect(
      text(payload).startsWith(
        `{"games":[{"current_turn":7,"game_id":"${RUNNING}","state":"running"},`,
      ),
    ).toBe(true);

    const rows = rowsOf(payload);
    expect(rows[0]?.['game_id']).toBe(RUNNING);
    expect(rows[0]?.['state']).toBe('running');
    // The live row wins: the disk row for the same id is dropped, not relabelled.
    expect(rows.filter((row) => row['game_id'] === RUNNING)).toHaveLength(1);
    // The disk rows that follow are still relabelled — the husk stays hidden.
    expect(rowFor(payload, COMPLETED)).toBeDefined();
    expect(rowFor(payload, HUSK)).toBeUndefined();
  });

  test('a junk upstream row is relayed but contributes no live id', async () => {
    const payload = okOf(
      await run(gamesIndexRoute, jsonUpstream(200, '{"games":[7,{"game_id":"short"}]}')),
    );
    const document = parsed(payload);
    const games = isJsonObject(document) ? document['games'] : undefined;
    // Relayed verbatim, junk and all — `live_ids` filters for id extraction only.
    expect(Array.isArray(games) ? games[0] : undefined).toBe(7);
    // Neither element named a well-formed id, so every disk row survives.
    expect(field(rowFor(payload, RUNNING), 'state')).toBe('interrupted');
  });

  test('a 500 is relayed and never masked by disk data', async () => {
    const upstream = jsonUpstream(500, '{"error":"boom"}');
    const failure = errorOf(await run(gamesIndexRoute, upstream));
    expect(failure._tag).toBe('UpstreamHttpError');
    expect(failure.status).toBe(500);
    expect(failure.message).toBe(Gateway.upstreamReturnedHttp(500));
    expect(upstream.urls()).toHaveLength(1);
  });

  test('a redirect is refused, not followed', async () => {
    const upstream = scriptedUpstream(() =>
      Promise.resolve(new Response('', { status: 302, headers: { Location: '/elsewhere' } })),
    );
    const failure = errorOf(await run(gamesIndexRoute, upstream));
    expect(failure.status).toBe(502);
    expect(failure.message).toBe(Gateway.GATEWAY_PROBLEM_MESSAGES.upstreamRedirect);
    expect(upstream.urls()).toHaveLength(1);
  });

  test('an index that is not an index is 502, not a fallback', async () => {
    const notJson = errorOf(await run(gamesIndexRoute, jsonUpstream(200, '{not json')));
    expect(notJson._tag).toBe('UpstreamInvalid');
    expect(notJson.status).toBe(502);
    expect(notJson.message).toBe(Gateway.GATEWAY_PROBLEM_MESSAGES.upstreamGamesIndexInvalid);

    const notAList = errorOf(await run(gamesIndexRoute, jsonUpstream(200, '{"games":{}}')));
    expect(notAList._tag).toBe('UpstreamInvalid');

    const notAnObject = errorOf(await run(gamesIndexRoute, jsonUpstream(200, '[]')));
    expect(notAnObject._tag).toBe('UpstreamInvalid');
  });
});

// ---------------------------------------------------------------------------
// /v1/games/{id}, /status, /result
// ---------------------------------------------------------------------------

describe('the archive JSON routes', () => {
  test('a 2xx body is relayed byte for byte, with the upstream\'s own status', async () => {
    // Unsorted keys, spaces after colons, a trailing newline: everything the
    // canonical writer would remove.
    const body = '{ "z": 1,  "a": [1, 2],\n  "note": "héllo" }\n';
    const upstream = jsonUpstream(201, body);
    const payload = okOf(
      await archivePayload(statusRoute(COMPLETED, `/v1/games/${COMPLETED}/status`), upstream),
    );

    expect(payload.status).toBe(201);
    expect(payload.body).toEqual(new TextEncoder().encode(body));
    // And it is emphatically not what we would have written ourselves.
    expect(payload.body).not.toEqual(canonical({ z: 1n, a: [1n, 2n], note: 'héllo' }));
  });

  test('the dispatched path is forwarded verbatim — bare id, and a trailing slash', async () => {
    const bare = jsonUpstream(200, '{}');
    await archivePayload(statusRoute(COMPLETED, `/v1/games/${COMPLETED}`), bare);
    expect(bare.urls()).toEqual([`${SERVICE_URL}/v1/games/${COMPLETED}`]);

    const slashed = jsonUpstream(200, '{}');
    await archivePayload(statusRoute(COMPLETED, `/v1/games/${COMPLETED}/status/`), slashed);
    expect(slashed.urls()).toEqual([`${SERVICE_URL}/v1/games/${COMPLETED}/status/`]);
  });

  test('404 and offline converge on the identical archive projection', async () => {
    const route = statusRoute(COMPLETED, `/v1/games/${COMPLETED}/status`);
    const onFourOhFour = okOf(await archivePayload(route, jsonUpstream(404, '{}')));
    const onOffline = okOf(await archivePayload(route, refusedUpstream()));
    const onPortless = okOf(await archivePayload(route, portlessUpstream()));

    expect(onOffline.body).toEqual(onFourOhFour.body);
    expect(onPortless.body).toEqual(onFourOhFour.body);
    expect(onFourOhFour.status).toBe(200);

    const document = parsed(onFourOhFour);
    expect(field(document, 'game_id')).toBe(COMPLETED);
    expect(field(document, 'state')).toBe('completed');
    // Without `--viewer-public-url` the watch link stays root-relative.
    expect(field(document, 'watch_url')).toBe(`/watch/${COMPLETED}`);
    expect(field(document, 'status_url')).toBe(
      `http://127.0.0.1:${String(BOUND_PORT)}/v1/games/${COMPLETED}/status`,
    );
  });

  test('--viewer-public-url replaces the base and absolutizes watch_url', async () => {
    const route = statusRoute(COMPLETED, `/v1/games/${COMPLETED}/status`);
    const payload = okOf(
      await archivePayload(route, jsonUpstream(404, '{}'), Option.some(VIEWER_URL)),
    );
    const document = parsed(payload);
    expect(field(document, 'watch_url')).toBe(`${VIEWER_URL}/watch/${COMPLETED}`);
    expect(field(document, 'status_url')).toBe(`${VIEWER_URL}/v1/games/${COMPLETED}/status`);
  });

  test('/result is the curated document — artifact_id, and no episode path', async () => {
    const payload = okOf(await archivePayload(resultRoute(COMPLETED), jsonUpstream(404, '{}')));
    const document = parsed(payload);
    expect(field(document, 'artifact_id')).toBe(COMPLETED);
    expect(isJsonObject(document) && Object.hasOwn(document, 'episode')).toBe(false);
    expect(isJsonObject(document) && Object.hasOwn(document, 'game_id')).toBe(false);
    expect(text(payload)).not.toContain(runsRootRef.current);
  });

  test('a live run is never exposed as a terminal archive when upstream is down', async () => {
    const route = statusRoute(RUNNING, `/v1/games/${RUNNING}/status`);
    const failure = errorOf(await archivePayload(route, refusedUpstream()));
    expect(failure._tag).toBe('NotFound');
    expect(failure.status).toBe(404);
    expect(failure.message).toBe(Gateway.GATEWAY_PROBLEM_MESSAGES.terminalArchiveNotFound);
  });

  test('a 5xx never reaches disk, even for a run the archive could serve', async () => {
    const route = statusRoute(COMPLETED, `/v1/games/${COMPLETED}/status`);
    const upstream = jsonUpstream(503, '{"error":"supervisor restarting"}');
    const failure = errorOf(await archivePayload(route, upstream));
    expect(failure._tag).toBe('UpstreamHttpError');
    expect(failure.status).toBe(503);
    expect(failure.message).toBe(Gateway.upstreamReturnedHttp(503));
    expect(upstream.urls()).toHaveLength(1);
  });

  test('405 is a fallback trigger on this family too', async () => {
    const route = statusRoute(COMPLETED, `/v1/games/${COMPLETED}/status`);
    const onFourOhFive = okOf(await archivePayload(route, jsonUpstream(405, '{}')));
    const onFourOhFour = okOf(await archivePayload(route, jsonUpstream(404, '{}')));
    expect(onFourOhFive.body).toEqual(onFourOhFour.body);
  });
});

// ---------------------------------------------------------------------------
// CPython as the oracle
// ---------------------------------------------------------------------------

/**
 * The three disk bodies, built by `agent_eval.replay_gateway` itself on the
 * *same* directory and canonicalized with its own `_canonical`.
 *
 * Only the two `_games` disk branches are re-expressed here, and only because
 * they live inside a bound `ReplayGatewayHandler` that cannot be constructed
 * without a socket: `_disk_rows_with_interrupted` (`:1582`) is six lines over
 * `_disk_games_index` and `_as_interrupted`, both of which are called for real.
 * The archive body is the untouched `_archive_status` / `_archive_result`.
 */
const ORACLE = `
import json, os, sys
sys.path.insert(0, os.environ["ARENA_REPO_ROOT"])
from pathlib import Path
from agent_eval.replay_gateway import (
    TERMINAL_STATES, _archive_result, _archive_status, _as_interrupted,
    _canonical, _disk_games_index, _terminal_archive,
)

root = Path(os.environ["ARENA_RUNS_ROOT"])
base = os.environ["ARENA_BASE"]
game_id = os.environ["ARENA_GAME_ID"]

rows = []
for row in _disk_games_index(root)["games"]:
    if row["state"] not in TERMINAL_STATES:
        row = _as_interrupted(root, row)
        if row is None:
            continue
    rows.append(row)

archive = _terminal_archive(root, game_id)
print(json.dumps({
    "raw": _canonical(_disk_games_index(root)).decode("utf-8"),
    "offline": _canonical({"schema_version": 1, "games": rows}).decode("utf-8"),
    "status": _canonical(
        _archive_status(archive, base, absolute_watch=False)
    ).decode("utf-8"),
    "result": _canonical(
        _archive_result(archive, base, absolute_watch=False)
    ).decode("utf-8"),
}))
`;

interface OracleBodies {
  readonly raw: string;
  readonly offline: string;
  readonly status: string;
  readonly result: string;
}

const askOracle = (): OracleBodies => {
  const result = Bun.spawnSync(['python3', '-c', ORACLE], {
    stdout: 'pipe',
    stderr: 'pipe',
    env: {
      ...process.env,
      ARENA_REPO_ROOT: REPO_ROOT,
      ARENA_RUNS_ROOT: runsRootRef.current,
      ARENA_BASE: `http://127.0.0.1:${String(BOUND_PORT)}`,
      ARENA_GAME_ID: COMPLETED,
    },
  });
  if (result.exitCode !== 0) throw new Error(`oracle failed: ${result.stderr.toString()}`);
  // A subprocess's stdout is untyped by construction; this is the boundary.
  const decoded = decodeJsonValueFromString(result.stdout.toString());
  const document = Either.isRight(decoded) ? decoded.right : null;
  if (!isJsonObject(document)) {
    throw new Error(`oracle emitted no JSON object: ${result.stdout.toString()}`);
  }
  const read = (key: string): string => {
    const value = document[key];
    if (!Predicate.isString(value)) throw new Error(`oracle is missing ${key}`);
    return value;
  };
  return {
    raw: read('raw'),
    offline: read('offline'),
    status: read('status'),
    result: read('result'),
  };
};

describe('differential against agent_eval.replay_gateway', () => {
  test('all four disk bodies are byte-identical to CPython\'s', async () => {
    const oracle = askOracle();

    const offline = okOf(await run(gamesIndexRoute, refusedUpstream()));
    expect(text(offline)).toBe(oracle.offline);

    const raw = okOf(await run(gamesIndexRoute, jsonUpstream(404, '{}')));
    expect(text(raw)).toBe(oracle.raw);

    const statusRouteFor = statusRoute(COMPLETED, `/v1/games/${COMPLETED}/status`);
    const status = okOf(await archivePayload(statusRouteFor, jsonUpstream(404, '{}')));
    expect(text(status)).toBe(oracle.status);

    const result = okOf(await archivePayload(resultRoute(COMPLETED), jsonUpstream(404, '{}')));
    expect(text(result)).toBe(oracle.result);
  });
});
