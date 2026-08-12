/**
 * `/health` — the one route that never opens the upstream — and the gateway's
 * own identity.
 *
 * Bare `:NNN` citations are `agent_eval/replay_gateway.py`.
 *
 * ## Two things live here, and they belong together
 *
 * 1. **{@link GatewayIdentity}** — `identity_payload()` (`:1301`), the single
 *    object that is `/health`'s body, the ready file's record and the stdout
 *    line, plus the two facts every archive projection derives from it
 *    (`_archive_base`, `:1881`).  It is a *service* rather than a pure function
 *    of {@link GatewayConfigValues} because it needs the **bound** port: with
 *    `--port 0` the kernel picks, and `identity_payload` reads
 *    `server.server_address` after the socket is listening (`:1302`).  Building
 *    it once at bind time is also what keeps `/health` from re-deriving a
 *    digest on every request.
 *
 * 2. **{@link healthRoute}** — dispatch step 1 (`:1970`).  Two lines of code,
 *    and the interesting part is what it does *not* do: no upstream contact, no
 *    disk read, no size bound (`_json`, `:1976`).  The query rejection that
 *    guards it (`400 healthQuery`) belongs to `../dispatch.ts` and has already
 *    happened by the time this runs.
 *
 * ## Handlers return values, not responses
 *
 * Every route in this directory answers with a `../json.ts#GatewayJsonPayload`
 * — a status and the exact bytes — or fails with a `../../errors.ts` value.
 * The single response site (`../respond.ts`) turns either into an
 * `HttpServerResponse`.  That is the port's first invariant (behavior dossier
 * §0) and the reason nothing here imports `HttpServerResponse`.
 *
 * The two serializers themselves (`_json`, `_bounded_json`) used to live in
 * this module and now live in `../json.ts`, because three route modules needed
 * them and only one of the three is `/health`.
 *
 * @module
 */

import { type CanonRecord, Gateway } from '@arena/wire';
import { Context, Effect, Layer, Option } from 'effect';
import type { GatewayConfigValues } from '../../config.ts';
import type { InternalError } from '../../errors.ts';
import { gatewayJson, type GatewayJsonPayload } from '../json.ts';

// ---------------------------------------------------------------------------
// identity_payload
// ---------------------------------------------------------------------------

/** What `identity_payload` needs beyond the configuration (`:1302`, `:1310`). */
export interface GatewayIdentityInput {
  /** The validated configuration, as `gateway_config` produced it (`:186`). */
  readonly config: GatewayConfigValues;
  /**
   * The **bound** port, read off the listening socket.
   *
   * Never `config.port`: `--port 0` is a request, and `/health` reports the
   * answer (`:1312`).  Publishing the request would advertise `http://host:0`
   * and break `local_stack`'s `_wait_http`.
   */
  readonly boundPort: number;
  /** `os.getpid()` (`:1310`).  Injected so the payload is a pure function. */
  readonly pid: number;
}

/**
 * `identity_payload()` (`:1301-1324`) as canonical-ready values.
 *
 * Every integer is a `bigint` because CPython spells them `1`, not `1.0`, and
 * the ready file, the stdout line and this response are all compared byte for
 * byte somewhere.  `viewer_public_url` is **omit-or-present, never `null`**
 * (`:1321`): its absence is what makes watch URLs relative, so a `null` would
 * mean something the producer never says.
 */
export const identityPayload = (input: GatewayIdentityInput): CanonRecord => ({
  schema_version: BigInt(Gateway.GATEWAY_IDENTITY_SCHEMA_VERSION),
  ok: true,
  kind: Gateway.GATEWAY_KIND,
  protocol_version: BigInt(Gateway.GATEWAY_PROTOCOL_VERSION),
  identity: input.config.identity,
  pid: BigInt(input.pid),
  host: input.config.host,
  port: BigInt(input.boundPort),
  url: Gateway.gatewaySelfUrl(input.config.host, input.boundPort),
  repo_root: input.config.repoRoot,
  upstream_service_url: input.config.upstreamServiceUrl,
  runs_root: input.config.runsRoot,
  cache_root: input.config.cacheRoot,
  ...Option.match(input.config.viewerPublicUrl, {
    onNone: (): CanonRecord => ({}),
    onSome: (viewerPublicUrl): CanonRecord => ({ viewer_public_url: viewerPublicUrl }),
  }),
});

/**
 * The gateway's own identity, resolved once the socket is listening.
 *
 * Three facts, one source.  `archiveBase` and `absoluteWatch` are
 * `_archive_base()` (`:1881`) and `absolute_watch` (`:1916`) — the two values
 * every archive projection is parameterized by — and they are derived here
 * rather than at each call site so that "the viewer URL replaces the gateway's
 * own origin" is stated once.
 */
export interface GatewayIdentityService {
  /** The `/health` body, the ready record and the stdout line, verbatim. */
  readonly payload: CanonRecord;
  /**
   * `_archive_base` (`:1881`): `viewer_public_url` when configured, otherwise
   * the gateway's own `http://{host}:{port}`.  Every absolute artifact URL in
   * a status, result, watch or frames document is built on it.
   */
  readonly archiveBase: string;
  /**
   * `absolute_watch` (`:1916`): true exactly when `--viewer-public-url` was
   * given.  It reaches `game.watch_url` and nothing else — a status served
   * through a tunnel links to the tunnel, one served locally stays relative.
   */
  readonly absoluteWatch: boolean;
}

/** Build the identity from a bound socket's coordinates. */
export const makeGatewayIdentity = (input: GatewayIdentityInput): GatewayIdentityService => ({
  payload: identityPayload(input),
  archiveBase: Option.getOrElse(input.config.viewerPublicUrl, () =>
    Gateway.gatewaySelfUrl(input.config.host, input.boundPort),
  ),
  absoluteWatch: Option.isSome(input.config.viewerPublicUrl),
});

/**
 * The gateway's identity, as a service.
 *
 * A `Context.Tag` and not a module constant for the reason the whole port uses
 * tags: the parity rig runs two gateways in one process, and a hidden global
 * would give them one identity between them.
 */
export class GatewayIdentity extends Context.Tag('@arena/harness/gateway/GatewayIdentity')<
  GatewayIdentity,
  GatewayIdentityService
>() {}

/** A `Layer` carrying one bound gateway's identity. */
export const layer = (input: GatewayIdentityInput): Layer.Layer<GatewayIdentity> =>
  Layer.succeed(GatewayIdentity, makeGatewayIdentity(input));

// ---------------------------------------------------------------------------
// The route
// ---------------------------------------------------------------------------

/**
 * `path == "/health"` (`:1970`) — `_json(200, identity_payload())`.
 *
 * Local, unbounded, never proxied, and the only route in the gateway with no
 * fallback matrix at all.  `local_stack.py`'s `_wait_http` greps the body for
 * `"ok":true`, which the canonical writer emits with no space after the colon;
 * `test_health_has_exact_checkout_and_upstream_identity` reads the four
 * resolved paths and the 20-hex `identity` back out of it.
 */
export const healthRoute: Effect.Effect<GatewayJsonPayload, InternalError, GatewayIdentity> =
  Effect.flatMap(GatewayIdentity, (identity) => gatewayJson(identity.payload));
