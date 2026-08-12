/**
 * `/v1/games/{id}/events.json` — the derived event log.
 *
 * `_events` (`agent_eval/replay_gateway.py:1727-1797`) is
 * {@link viewerJsonRoute} with the events loader in the hole; the upstream leg,
 * the fallback matrix and the two size errors belong to `./replay.ts` and are
 * not re-decided here.  What is left is two facts, and both are easy to lose.
 *
 * ## 1. Any query at all is a 400 — from the handler, not the router
 *
 * `if query: raise GatewayProblem(400, "game events do not accept query
 * parameters")` (`:1728`).  The router's own query gate (`:2005`,
 * `viewerRouteQuery`) is *skipped* for this route — `events.json` is matched at
 * `:2002`, before the gate — so the message a client sees is the events one and
 * not the generic one.  Dispatch deliberately forwards the raw query here so
 * that this handler can be the one to refuse it.
 *
 * Empty means empty: `parsed.query` is `''` for a bare trailing `?`, so
 * `events.json?` is a **200**, not a 400.
 *
 * ## 2. The projection is not optional
 *
 * `_bounded_json(200, _public_events(events, game_id))` (`:1797`) — the only
 * one of the three viewer routes that reshapes what the loader returned.
 * `_public_events` re-derives three fields the loader does not own
 * (`schema_version`, the **requested** `game_id`, and a `total_events` that
 * counts the rows which survived the filter), so serving the loader's document
 * verbatim would publish a different payload *and* a `game_id` that came from
 * disk rather than from the request.
 *
 * The projection applies only to the disk path.  A 2xx from upstream is relayed
 * byte for byte and is never re-shaped — upstream has already published its own
 * projection, and re-encoding it would destroy the parity the port is measured
 * on.
 *
 * @module
 */

import { Effect } from 'effect';
import { BadRequest } from '../../errors.ts';
import { publicEvents } from '../../public.ts';
import { ReplayDerivation } from '../../services/derivation.ts';
import type { EventsJsonRoute } from '../dispatch.ts';
import { type ViewerRouteEffect, viewerJsonRoute } from './replay.ts';

/**
 * `_events` (`:1727-1797`).
 *
 * The refusal comes first and costs no I/O: a 400 here is a promise that
 * nothing was proxied and no savegame was opened.
 */
export const eventsRoute = (route: EventsJsonRoute): ViewerRouteEffect =>
  route.query !== ''
    ? Effect.fail(new BadRequest({ problem: 'eventsQuery' }))
    : viewerJsonRoute({
        operation: 'events',
        gameId: route.gameId,
        upstreamPath: route.upstreamPath,
        // `_upstream_json_or_status(path)` (`:1735`) — the default `query=""`.
        // Nothing is normalized because nothing is accepted.
        upstreamQuery: '',
        derive: (context) =>
          Effect.flatMap(ReplayDerivation, (derivation) =>
            derivation.events({
              gameId: route.gameId,
              places: context.places,
              complete: context.complete,
            }),
          ),
        project: (value) => publicEvents(value, route.gameId),
      });
