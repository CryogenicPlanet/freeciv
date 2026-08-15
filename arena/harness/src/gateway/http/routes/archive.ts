/**
 * Archive JSON and binary routes. Upstream is attempted before disk, and only offline/404/405
 * permit fallback. Local files use `O_NOFOLLOW` plus descriptor `fstat` so symlinks and non-files
 * cannot expose private data; request-scope finalizers close readers and descriptors.
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
import { boundedJsonResponse, jsonPayloadResponse } from '../json.ts';
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

/** Python's scan consumes at most 513 complete logical lines. */
const PPM_LOGICAL_LINES = 513;

/** Bounded by line count rather than bytes: PPM comments have no byte limit. */
const PPM_READ_CHUNK_BYTES = 64 * 1024;

/** Python stops once a post-magic line terminates the comment header. */
const ppmHeaderDone = (text: string): boolean => {
  const lines = text.split(/\r\n|\r|\n/u);
  let playerSeen = false;
  for (let index = 0; index < lines.length - 1; index += 1) {
    const line = lines[index] ?? '';
    if (index > 0 && !line.startsWith('#')) {
      const stripped = line.trim();
      if (playerSeen || (stripped !== '' && stripped !== 'P3')) return true;
    }
    if (/^#\s*playerno:/u.test(line)) playerSeen = true;
  }
  const partial = lines.at(-1) ?? '';
  if (lines.length > 1 && !partial.startsWith('#')) {
    const stripped = partial.trimStart();
    if (stripped !== '' && (playerSeen || !'P3'.startsWith(stripped))) return true;
  }
  return false;
};

/** `_send_local_file`'s copy loop (`:1515`) and `_stream_upstream`'s (`:1484`). */
export const ARCHIVE_STREAM_CHUNK_BYTES = 64 * 1024;

const NOT_FOUND = (problem: 'archiveDataNotFound' | 'archiveFileNotFound'): NotFound =>
  new NotFound({ problem });

const attemptSync = <A>(thunk: () => A): Option.Option<A> => Option.getRight(Either.try(thunk));

/** The first 513 complete universal-newline lines, decoded with `errors="replace"`. */
const readPpmLines = (path: string): string =>
  Option.getOrElse(
    Option.flatMap(
      attemptSync(() => openSync(path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC)),
      (fd) => {
        const bytes = attemptSync(() => {
          const parts: Uint8Array[] = [];
          let lines = 0;
          let previousWasCr = false;
          let done = false;
          while (!done && lines < PPM_LOGICAL_LINES) {
            const buffer = new Uint8Array(PPM_READ_CHUNK_BYTES);
            const count = readSync(fd, buffer, 0, buffer.byteLength, null);
            if (count <= 0) break;
            let keep = count;
            for (let index = 0; index < count; index += 1) {
              const byte = buffer[index];
              if (byte === 0x0d) {
                lines += 1;
                previousWasCr = true;
              } else if (byte === 0x0a) {
                if (!previousWasCr) lines += 1;
                previousWasCr = false;
              } else {
                previousWasCr = false;
              }
              if (lines >= PPM_LOGICAL_LINES) {
                keep = index + 1;
                done = true;
                break;
              }
            }
            parts.push(buffer.subarray(0, keep));
            if (!done) {
              const length = parts.reduce((total, part) => total + part.byteLength, 0);
              const prefix = new Uint8Array(length);
              parts.reduce((offset, part) => {
                prefix.set(part, offset);
                return offset + part.byteLength;
              }, 0);
              done = ppmHeaderDone(new TextDecoder('utf-8').decode(prefix));
            }
          }
          const length = parts.reduce((total, part) => total + part.byteLength, 0);
          const joined = new Uint8Array(length);
          parts.reduce((offset, part) => {
            joined.set(part, offset);
            return offset + part.byteLength;
          }, 0);
          return joined;
        });
        Option.getOrElse(attemptSync(() => closeSync(fd)), () => undefined);
        return Option.map(bytes, (value) => new TextDecoder('utf-8').decode(value));
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
                      readPpmLines(join(ppmDirectory, pairing.ppmName)),
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
const ARCHIVE_JSON_VALUES = {
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
} satisfies {
  readonly [V in ArchiveJsonView]: (
    archive: TerminalArchive,
    options: ArchiveRouteOptions,
    runs: RunsRepositoryApi,
  ) => Effect.Effect<CanonValue, GatewayError>;
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
            Effect.succeed(jsonPayloadResponse({ status: result.status, body: result.body }))
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
  const responseOptions = {
    status: owned.response.status,
    contentType: proxiedContentType(owned.response),
    headers: proxiedHeaders(owned.response),
  };
  const response = Option.isSome(contentLength)
    ? HttpServerResponse.stream(Stream.ensuring(owned.response.stream, owned.close), {
        ...responseOptions,
        contentLength: contentLength.value,
      })
    : HttpServerResponse.stream(
        Stream.ensuring(owned.response.stream, owned.close),
        responseOptions,
      );
  return withSecurityHeaders(response);
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
 *
 * Bun omits `Content-Length` from asynchronous streamed responses and uses
 * chunked framing. The parity waiver owns this known framing difference; a
 * `Blob` is not equivalent because Bun then applies `Range` and returns 206
 * where Python deliberately returns the full 200 response.
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
