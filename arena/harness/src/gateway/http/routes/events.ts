/** Events route: queries are rejected before I/O, then the shared viewer fallback applies. */

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
