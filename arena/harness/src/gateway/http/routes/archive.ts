/**
 * The archive route families: `_archive_json_route` (`:1887`) and
 * `_archive_binary_route` (`:1935`).
 *
 * Every bare `:NNN` below cites `agent_eval/replay_gateway.py`.
 *
 * Seven of the twelve routes land here — `status`, `result`, `watch.json`,
 * `frames` on the JSON side, `frames/{n}.png`, `frames/latest.png` and
 * `video.mp4` on the binary side — because Python serves them with exactly two
 * functions.  They share a fallback rule and disagree about everything else:
 *
 * | | JSON family | binary family |
 * |---|---|---|
 * | upstream 2xx | relay the **bytes**, gateway headers | **stream** the socket, upstream headers |
 * | upstream 404/405 | build the archive projection | serve the archive **file** |
 * | offline | identical to 404/405 | identical to 404/405 |
 * | any other status | `upstreamProblem(status)` | `upstreamProblem(status)` — still JSON (trap B5) |
 * | `Cache-Control` | `no-store`, always | `public, max-age=31536000, immutable` on a file; whatever upstream sent (often nothing) on a proxy |
 *
 * ## The three things a port gets wrong here
 *
 * 1. **A 500 on `frames/3.png` is not an image.**  `_stream_upstream`'s
 *    non-2xx arm (`:1453-1462`) drains 64 KiB and writes a JSON problem, so the
 *    binary route answers `application/json; charset=utf-8` with
 *    `{"error":"upstream returned HTTP 500"}` under the upstream's own status.
 *    Only 404/405 and offline reach the disk (trap B5).
 * 2. **A proxied 2xx carries no `Cache-Control` unless upstream sent one.**
 *    `_send_headers` only defaults to `no-store` when the header mapping is
 *    empty *and* the status is ≥ 400, and a proxied 2xx always has a mapping
 *    (trap B6).  The immutable header belongs to `_send_local_file` (`:1510`)
 *    and to nothing else.
 * 3. **The forwarded path is the dispatcher's, verbatim.**  Both families pass
 *    `parsed.path` (`:2011`, `:2023`), so `/v1/games/{id}/status/` proxies
 *    *with* its trailing slash (trap B1).  Dispatch has already put that in
 *    `route.upstreamPath`; this module concatenates nothing.
 *
 * ## What this module does not do
 *
 * It builds **no error response**.  Every refusal is a `../../errors.ts` value
 * in the `E` channel, rendered once at the router edge by
 * `../respond.ts#respondGateway`.  The only responses constructed here are the
 * three *successful* shapes Python has — a canonical JSON body, a proxied
 * stream, and a local file — and two of them go through
 * `../respond.ts#gatewayJsonResponse` so the header discipline stays in one
 * place.
 *
 * @module
 */

import { type CanonValue, type FrameIndex, Gateway } from '@arena/wire';
import { HttpServerResponse } from '@effect/platform';
import { Effect, Either, Exit, Option, Scope, Stream } from 'effect';
import { closeSync, fstatSync, openSync, readSync } from 'node:fs';
import { join } from 'node:path';
import {
  type ArchiveFrameSource,
  archiveFrames,
  type ArchivePng,
  type ArchivePpm,
  archivePpmPlayers,
  archiveResult,
  archiveStatus,
  archiveWatch,
  pairArchiveFrames,
} from '../../archive.ts';
import { isUpstreamFallbackStatus } from '../../constants.ts';
import {
  type GatewayError,
  gatewayErrorFromUpstream,
  InternalError,
  isUpstreamOffline,
  NotFound,
  UpstreamHttpError,
} from '../../errors.ts';
import {
  archiveRegularFiles,
  O_CLOEXEC,
  O_NOFOLLOW,
  O_RDONLY,
  RunsRepository,
  type RunsRepositoryApi,
  safeArchiveDirectory,
  type TerminalArchive,
} from '../../services/runs.ts';
import {
  isSuccessStatus,
  isUpstreamBody,
  parsePythonInt,
  UpstreamClient,
  type UpstreamBinaryResponse,
  type UpstreamClientApi,
  type UpstreamFailure,
} from '../../services/upstream.ts';
import {
  ARCHIVE_JSON_VIEWS,
  type ArchiveBinaryRoute,
  type ArchiveJsonRoute,
  type ArchiveJsonView,
} from '../dispatch.ts';
import { boundedJsonResponse, jsonPayloadResponse, relayedJsonPayload } from '../json.ts';
import { withSecurityHeaders } from '../respond.ts';

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

/**
 * `_archive_base()` (`:1881`) and the `absolute_watch` flag (`:1916`), as one
 * value.
 *
 * They come from the same config field and are read at two different places in
 * Python, which is how a port ends up with a `viewer_public_url` that changes
 * `png_url` but not `watch_url`.  {@link archiveRouteOptions} derives both from
 * the one input so they cannot drift.
 */
export interface ArchiveRouteOptions {
  /** The origin every archive URL is absolute against. */
  readonly base: string;
  /** Whether `watch_url` is absolute too — `viewer_public_url is not None`. */
  readonly absoluteWatch: boolean;
}

/**
 * Derive both fields the way Python does — and note they disagree about the
 * empty string.
 *
 * `base` is `viewer_public_url or str(identity["url"])`, a Python `or`, so an
 * empty configured URL falls back to the identity origin; `absolute_watch` is
 * `viewer_public_url is not None`, which is `True` for that same empty string.
 * `gateway_config` never produces one, so the disagreement is unreachable —
 * transcribed rather than tidied because "unreachable" is a claim about the
 * *config* module, not about this one.
 */
export const archiveRouteOptions = (
  identityUrl: string,
  viewerPublicUrl: string | null,
): ArchiveRouteOptions => ({
  base: viewerPublicUrl === null || viewerPublicUrl === '' ? identityUrl : viewerPublicUrl,
  absoluteWatch: viewerPublicUrl !== null,
});

// ---------------------------------------------------------------------------
// Frame sources — the I/O half of _archive_frames
// ---------------------------------------------------------------------------

/**
 * How much of a `*.map.ppm` is read before `archivePpmPlayers` sees it.
 *
 * Python opens the autosave in text mode and iterates lines, breaking out of
 * the scan at line 513 *or* at the first non-comment line — so it reads one
 * buffered block of a file that is mostly pixels.  Reading the whole thing per
 * frame would turn a 40-frame `watch.json` into hundreds of megabytes of I/O.
 *
 * The prefix is cut back to the last complete line, so no truncated line can
 * be matched, and the scan cannot look past line 513 anyway: a header whose
 * first 512 KiB do not contain the player comments is a header whose scan had
 * already stopped.
 */
export const PPM_PREFIX_BYTES = 512 * 1024;

/** `_send_local_file`'s copy loop (`:1515`) and `_stream_upstream`'s (`:1484`). */
export const ARCHIVE_STREAM_CHUNK_BYTES = 64 * 1024;

const NOT_FOUND = (problem: 'archiveDataNotFound' | 'archiveFileNotFound'): NotFound =>
  new NotFound({ problem });

const attemptSync = <A>(thunk: () => A): Option.Option<A> => Option.getRight(Either.try(thunk));

/** The bounded head of an autosave, decoded with `errors="replace"`. */
const readPpmPrefix = (path: string): string =>
  Option.getOrElse(
    Option.flatMap(
      attemptSync(() => openSync(path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC)),
      (fd) => {
        const bytes = attemptSync(() => {
          const size = fstatSync(fd).size;
          const length = Math.min(size, PPM_PREFIX_BYTES);
          const buffer = new Uint8Array(length);
          const read = readSync(fd, buffer, 0, length, 0);
          return { chunk: buffer.subarray(0, Math.max(read, 0)), whole: length >= size };
        });
        Option.getOrElse(
          attemptSync(() => closeSync(fd)),
          () => undefined,
        );
        return Option.map(bytes, (value) => {
          // `errors="replace"`: a torn multi-byte sequence at the cut must not
          // fail the read, it must become U+FFFD exactly as CPython's decoder
          // would have made it.
          const text = new TextDecoder('utf-8').decode(value.chunk);
          const lastBreak = text.lastIndexOf('\n');
          return value.whole || lastBreak < 0 ? text : text.slice(0, lastBreak + 1);
        });
      },
    ),
    () => '',
  );

/**
 * `_archive_frames`' listing half (`:969-975`) — both directories, then the
 * positional pairing.
 *
 * Both `watch_frames/` and `saves/` must pass the containment check: an
 * archive with frames but no autosaves is a **404 `archive data not found`**,
 * not a listing with null turns.
 */
const frameSources = (
  archive: TerminalArchive,
): Either.Either<readonly ArchiveFrameSource[], GatewayError> =>
  Either.flatMap(
    safeArchiveDirectory(archive.runRoot, Gateway.ARCHIVE_FRAMES_DIRECTORY),
    (pngDirectory) =>
      Either.map(
        safeArchiveDirectory(archive.runRoot, Gateway.ARCHIVE_SAVES_DIRECTORY),
        (ppmDirectory) => {
          const pngs: readonly ArchivePng[] = archiveRegularFiles(
            pngDirectory,
            Gateway.ARCHIVE_PNG_RE,
          ).flatMap((name) =>
            Either.match(Gateway.decodeArchivePngName(name), {
              onLeft: (): readonly ArchivePng[] => [],
              onRight: (index) => [{ index, name }],
            }),
          );
          const ppms: readonly ArchivePpm[] = archiveRegularFiles(
            ppmDirectory,
            Gateway.ARCHIVE_PPM_RE,
          ).flatMap((name) =>
            Option.match(Gateway.archivePpmTurn(name), {
              onNone: (): readonly ArchivePpm[] => [],
              onSome: (turn) => [{ turn, name }],
            }),
          );
          return pairArchiveFrames(pngs, ppms).map(
            (pairing): ArchiveFrameSource => ({
              ...pairing,
              mapPlayers:
                pairing.ppmName === null
                  ? []
                  : archivePpmPlayers(
                      readPpmPrefix(join(ppmDirectory, pairing.ppmName)),
                      archive.places,
                    ),
            }),
          );
        },
      ),
  );

/**
 * `_archive_watch`'s video probe (`:1044-1048`) — Python discovers
 * availability by *catching* `_archive_video_path`'s 404, so a missing,
 * symlinked, empty or non-regular `game.mp4` is all the same answer.
 */
const videoAvailable = (
  runs: RunsRepositoryApi,
  archive: TerminalArchive,
): Effect.Effect<boolean> => Effect.map(Effect.either(runs.videoFile(archive)), Either.isRight);

// ---------------------------------------------------------------------------
// _archive_json_route
// ---------------------------------------------------------------------------

/**
 * The four projections `_archive_json_route` selects between (`:1917-1932`).
 *
 * `status` and `result` are pure over the archive; `frames` and `watch` need
 * the frame listing, and `watch` needs the video probe on top — which is why
 * this is an `Effect` and not a `switch` over a record of constants.
 */
const ARCHIVE_JSON_VALUES: {
  readonly [V in ArchiveJsonView]: (
    archive: TerminalArchive,
    options: ArchiveRouteOptions,
    runs: RunsRepositoryApi,
  ) => Effect.Effect<CanonValue, GatewayError>;
} = {
  status: (archive, options) =>
    Effect.succeed(archiveStatus(archive, options.base, options.absoluteWatch)),
  result: (archive, options) =>
    Effect.succeed(archiveResult(archive, options.base, options.absoluteWatch)),
  frames: (archive, options) =>
    Effect.map(frameSources(archive), (frames) =>
      archiveFrames(archive.gameId, options.base, frames),
    ),
  // `_archive_watch` lists the frames first and probes the video second
  // (`:1036-1048`); `flatMap` keeps that order, which is what a recording
  // filesystem would see.
  watch: (archive, options, runs) =>
    Effect.flatMap(frameSources(archive), (frames) =>
      Effect.map(videoAvailable(runs, archive), (video) =>
        archiveWatch(archive, options.base, frames, video, options.absoluteWatch),
      ),
    ),
};

/** The disk arm of `_archive_json_route` (`:1912-1933`), shared by 404/405 and offline. */
const archiveJsonFromDisk = (
  route: ArchiveJsonRoute,
  options: ArchiveRouteOptions,
): Effect.Effect<HttpServerResponse.HttpServerResponse, GatewayError, RunsRepository> =>
  Effect.flatMap(RunsRepository, (runs) =>
    Effect.flatMap(runs.terminalArchive(route.gameId), (archive) =>
      Effect.flatMap(
        ARCHIVE_JSON_VALUES[ARCHIVE_JSON_VIEWS[route._tag]](archive, options, runs),
        boundedJsonResponse,
      ),
    ),
  );

/**
 * `_archive_json_route` (`:1887-1933`) — `status`, `result`, `watch.json`,
 * `frames`.
 *
 * Upstream first, always.  The 2xx body is relayed as the **exact bytes** that
 * came off the socket (`:1898`): no parse, no re-serialization, no key
 * reordering — `test_watch_json_is_byte_preserved_and_credentials_are_not_forwarded`
 * compares them byte for byte.  What is *not* relayed is the upstream's
 * headers: `_send` forces `application/json; charset=utf-8` and
 * `Cache-Control: no-store`, so an upstream `ETag` survives only on the binary
 * path (§7.4).
 *
 * Offline and 404/405 converge on the identical disk arm — there is no
 * manifest-only degradation here, so a live run with upstream down answers
 * **404 `terminal archive not found`**, not a partial status
 * (`test_offline_live_run_is_not_exposed_as_terminal_archive`).
 */
export const archiveJsonRoute = (
  route: ArchiveJsonRoute,
  options: ArchiveRouteOptions,
): Effect.Effect<
  HttpServerResponse.HttpServerResponse,
  GatewayError,
  RunsRepository | UpstreamClient
> =>
  Effect.flatMap(UpstreamClient, (upstream) =>
    Effect.matchEffect(upstream.jsonOrStatus({ path: route.upstreamPath }), {
      onFailure: (
        failure,
      ): Effect.Effect<
        HttpServerResponse.HttpServerResponse,
        GatewayError,
        RunsRepository
      > =>
        isUpstreamOffline(failure)
          ? archiveJsonFromDisk(route, options)
          : Effect.fail(gatewayErrorFromUpstream(failure)),
      onSuccess: (result) =>
        isUpstreamBody(result)
          ? // 2xx: the upstream's exact bytes and its own status (§6).
            Effect.succeed(jsonPayloadResponse(relayedJsonPayload(result.status, result.body)))
          : isUpstreamFallbackStatus(result.status)
            ? archiveJsonFromDisk(route, options)
            : Effect.fail(new UpstreamHttpError({ upstreamStatus: result.status })),
    }),
  );

// ---------------------------------------------------------------------------
// _archive_binary_route
// ---------------------------------------------------------------------------

const CONTENT_HEADERS: ReadonlySet<string> = new Set(['content-type', 'content-length']);

/** `headers.get(name)` over the allowlisted subset the client already collected. */
const proxyHeader = (
  response: UpstreamBinaryResponse,
  name: string,
): Option.Option<string> =>
  Option.map(
    Option.fromNullable(
      response.headers.find(([key]) => key.toLowerCase() === name.toLowerCase()),
    ),
    ([, value]) => value,
  );

/**
 * `_stream_upstream`'s `Content-Type` default (`:1468-1471`).
 *
 * `binary=True` here always, so the fallback is `application/octet-stream` —
 * the JSON fallback belongs to a code path (`_proxy(binary=False)`) no route
 * in the port reaches.
 */
const proxiedContentType = (response: UpstreamBinaryResponse): string =>
  Option.getOrElse(proxyHeader(response, 'Content-Type'), () => 'application/octet-stream');

/**
 * `_stream_upstream`'s `Content-Length` re-parse (`:1474-1479`): **Python's**
 * `int()`, and an unparseable value **omits the header entirely** rather than
 * relaying it, leaving the body delimited by connection close.
 */
const proxiedContentLength = (response: UpstreamBinaryResponse): Option.Option<number> =>
  Option.flatMap(
    Option.flatMap(proxyHeader(response, 'Content-Length'), parsePythonInt),
    (length) =>
      length >= 0n && length <= BigInt(Number.MAX_SAFE_INTEGER)
        ? Option.some(Number(length))
        : Option.none(),
  );

/**
 * The allowlisted headers that are relayed *as headers* — the pair
 * `_send_headers` skips (`:1344`) is spent on the status line instead.
 *
 * This is why a proxied 2xx has no `Cache-Control` of its own: the mapping is
 * non-empty, so the `status >= 400` default never fires (trap B6).
 */
const proxiedHeaders = (response: UpstreamBinaryResponse): Record<string, string> =>
  Object.fromEntries(
    response.headers.filter(([name]) => !CONTENT_HEADERS.has(name.toLowerCase())),
  );

/** An open upstream response plus the handle that closes it early. */
interface OwnedBinary {
  readonly response: UpstreamBinaryResponse;
  readonly close: Effect.Effect<void>;
}

/**
 * `_open_upstream(accept="*&#47;*")` inside a scope this route can close on its
 * own schedule.
 *
 * Python's `with response:` closes the socket the moment the route decides to
 * fall back (`:1948`), and it holds it open exactly as long as the copy loop
 * runs.  A reader parked on the *request* scope cannot do the first, because
 * `@effect/platform` defers the request scope until a streamed body finishes —
 * so the reader lives in a child scope: closed explicitly on a fallback,
 * attached to the stream on a proxy, and registered on the request scope as
 * the backstop for a client that walks away.  Closing a closed scope is a
 * no-op, so all three may fire.
 */
const openOwnedBinary = (
  upstream: UpstreamClientApi,
  path: string,
): Effect.Effect<OwnedBinary, UpstreamFailure, Scope.Scope> =>
  Effect.flatMap(Scope.make(), (child) =>
    Effect.zipRight(
      Effect.addFinalizer(() => Scope.close(child, Exit.void)),
      Effect.map(
        Scope.extend(upstream.openBinary({ path }), child),
        (response): OwnedBinary => ({ response, close: Scope.close(child, Exit.void) }),
      ),
    ),
  );

/** The 2xx arm of `_stream_upstream` (`:1463-1489`) — headers, then the copy loop. */
const proxiedBinaryResponse = (
  owned: OwnedBinary,
): HttpServerResponse.HttpServerResponse => {
  const contentLength = proxiedContentLength(owned.response);
  return withSecurityHeaders(
    HttpServerResponse.stream(Stream.ensuring(owned.response.stream, owned.close), {
      status: owned.response.status,
      contentType: proxiedContentType(owned.response),
      ...(Option.isSome(contentLength) ? { contentLength: contentLength.value } : {}),
      headers: proxiedHeaders(owned.response),
    }),
  );
};

/** `index=None` is `latest.png`; `video=True` has no index at all (`:2022-2033`). */
const frameIndex = (route: ArchiveBinaryRoute): Option.Option<FrameIndex> =>
  route._tag === 'FramePng' ? Option.some(route.index) : Option.none();

/** `"video/mp4" if video else "image/png"` (`:1962`). */
const localContentType = (route: ArchiveBinaryRoute): string =>
  route._tag === 'VideoMp4'
    ? Gateway.ARCHIVE_VIDEO_CONTENT_TYPE
    : Gateway.ARCHIVE_FRAME_CONTENT_TYPE;

/** One `read(2)` of at most `length` bytes; {@link Option.none} at EOF or on error. */
const readAt = (fd: number, position: number, length: number): Option.Option<Uint8Array> =>
  Option.flatMap(
    attemptSync(() => {
      const buffer = new Uint8Array(length);
      const read = readSync(fd, buffer, 0, length, position);
      return buffer.subarray(0, Math.max(read, 0));
    }),
    (chunk) => (chunk.byteLength === 0 ? Option.none() : Option.some(chunk)),
  );

/**
 * `while chunk := stream.read(64 * 1024)` (`:1515`), as a stream.
 *
 * A read error ends the body rather than failing it: the status line and
 * `Content-Length` are already on the wire, and Python's own answer — an
 * `OSError` unwinding into a handler that has already responded — is a short
 * body and a dead connection.  This is that, without the second response
 * Python would try to append.
 */
const fileStream = (fd: number, size: number): Stream.Stream<Uint8Array> =>
  Stream.unfoldEffect(0, (position) =>
    Effect.sync(() =>
      position >= size
        ? Option.none()
        : Option.map(
            readAt(fd, position, Math.min(ARCHIVE_STREAM_CHUNK_BYTES, size - position)),
            (chunk) => [chunk, position + chunk.byteLength] as const,
          ),
    ),
  );

/**
 * `_send_local_file` (`:1492-1519`).
 *
 * `O_NOFOLLOW` and the `fstat` on the *descriptor* are the security property:
 * a `watch_frames/000000.png` replaced by a symlink to `auth.json` is a 404,
 * not a secret (`test_terminal_archive_is_fully_viewable_with_upstream_offline`,
 * pytest dossier X10).  A non-regular file and a zero-length file are the same
 * 404.  `O_CLOEXEC` is hardcoded — Bun's `node:fs` constants do not define it
 * on darwin (spike law).
 *
 * The immutable `Cache-Control` (`:1510`) lives here and nowhere else; every
 * gateway-built JSON body is `no-store`.
 */
const sendLocalFile = (
  path: string,
  contentType: string,
): Effect.Effect<HttpServerResponse.HttpServerResponse, GatewayError, Scope.Scope> =>
  Effect.flatMap(
    Effect.acquireRelease(
      Effect.suspend(() =>
        Either.mapLeft(
          Either.try(() => openSync(path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC)),
          (): GatewayError => NOT_FOUND('archiveFileNotFound'),
        ),
      ),
      (fd) => Effect.ignore(Effect.try(() => closeSync(fd))),
    ),
    (fd) =>
      Effect.flatMap(
        Effect.suspend(() =>
          Either.mapLeft(
            Either.try(() => fstatSync(fd)),
            (cause): GatewayError => new InternalError({ cause }),
          ),
        ),
        (info) =>
          !info.isFile() || info.size <= 0
            ? Effect.fail(NOT_FOUND('archiveFileNotFound'))
            : Effect.succeed(
                withSecurityHeaders(
                  HttpServerResponse.stream(fileStream(fd, info.size), {
                    status: 200,
                    contentType,
                    contentLength: info.size,
                    headers: { 'cache-control': Gateway.ARCHIVE_BINARY_CACHE_CONTROL },
                  }),
                ),
              ),
      ),
  );

/** The disk arm of `_archive_binary_route` (`:1956-1963`). */
const archiveBinaryFromDisk = (
  route: ArchiveBinaryRoute,
): Effect.Effect<
  HttpServerResponse.HttpServerResponse,
  GatewayError,
  RunsRepository | Scope.Scope
> =>
  Effect.flatMap(RunsRepository, (runs) =>
    Effect.flatMap(runs.terminalArchive(route.gameId), (archive) =>
      Effect.flatMap(
        route._tag === 'VideoMp4'
          ? runs.videoFile(archive)
          : runs.frameFile(archive, frameIndex(route)),
        (path) => sendLocalFile(path, localContentType(route)),
      ),
    ),
  );

/**
 * `_archive_binary_route` (`:1935-1963`) — `frames/{n}.png`,
 * `frames/latest.png`, `video.mp4`.
 *
 * Four arms, and the third is the one ports lose:
 *
 * - **2xx** → stream the socket through, with the `PROXY_RESPONSE_HEADERS`
 *   subset and no `Cache-Control` of the gateway's own;
 * - **404/405** → close the upstream *without draining* and serve the archive
 *   file;
 * - **anything else** → drain 64 KiB, discard it, and answer
 *   `upstreamProblem(status)` as **JSON** — a 500 on `frames/3.png` is
 *   `{"error":"upstream returned HTTP 500"}` with the upstream's status, not an
 *   image and not a passthrough (trap B5);
 * - **offline** → the archive file, exactly as 404/405.
 *
 * Nothing here is ever buffered: the proxy is a socket-to-socket copy and the
 * file is read 64 KiB at a time, so a 50 MiB video costs a chunk of memory
 * (spike S5).
 */
export const archiveBinaryRoute = (
  route: ArchiveBinaryRoute,
): Effect.Effect<
  HttpServerResponse.HttpServerResponse,
  GatewayError,
  RunsRepository | Scope.Scope | UpstreamClient
> =>
  Effect.flatMap(UpstreamClient, (upstream) =>
    Effect.matchEffect(openOwnedBinary(upstream, route.upstreamPath), {
      onFailure: (
        failure,
      ): Effect.Effect<
        HttpServerResponse.HttpServerResponse,
        GatewayError,
        RunsRepository | Scope.Scope
      > =>
        isUpstreamOffline(failure)
          ? archiveBinaryFromDisk(route)
          : Effect.fail(gatewayErrorFromUpstream(failure)),
      onSuccess: (owned) =>
        isSuccessStatus(owned.response.status)
          ? Effect.succeed(proxiedBinaryResponse(owned))
          : isUpstreamFallbackStatus(owned.response.status)
            ? // `with response:` closes it; the 404/405 arm never drains.
              Effect.zipRight(owned.close, archiveBinaryFromDisk(route))
            : Effect.zipRight(
                Effect.ignore(owned.response.drainError),
                Effect.zipRight(
                  owned.close,
                  Effect.fail(new UpstreamHttpError({ upstreamStatus: owned.response.status })),
                ),
              ),
    }),
  );
