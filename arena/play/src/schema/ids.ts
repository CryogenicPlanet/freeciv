/**
 * Opaque-ID domain schemas.
 *
 * The regexes live in `src/constants.ts`; this module is the typed vocabulary
 * every other decoder and every alias table reads them through, so no unit
 * re-derives "is this an actor ID" from a raw pattern.
 */
import { Schema } from 'effect';
import {
  ACTOR_ID_RE,
  ALIAS_ENTITY_TYPES,
  CATALOG_RE,
  CITY_ID_RE,
  CONTROLLER_RE,
  CURSOR_RE,
  ENTITY_ALIAS_RE,
  GAME_ID_RE,
  OPAQUE_ID_RE,
  RELATION_ID_RE,
  TILE_ID_RE,
} from 'src/constants';
import { isJsonString, type JsonValue } from 'src/schema/primitives';

export type ActorType = 'player' | 'city' | 'unit';
export const ACTOR_TYPES: ReadonlySet<string> = new Set<ActorType>(['player', 'city', 'unit']);

const identifierSchema = (identifier: string, pattern: RegExp) =>
  Schema.String.pipe(Schema.pattern(pattern)).annotations({ identifier });

export const isOpaqueId = Schema.is(identifierSchema('OpaqueId', OPAQUE_ID_RE));
export const isGameId = Schema.is(identifierSchema('GameId', GAME_ID_RE));
export const isControllerName = Schema.is(identifierSchema('ControllerName', CONTROLLER_RE));
export const isActorId = Schema.is(identifierSchema('ActorId', ACTOR_ID_RE));
export const isRelationId = Schema.is(identifierSchema('RelationId', RELATION_ID_RE));
export const isTileId = Schema.is(identifierSchema('TileId', TILE_ID_RE));
export const isCityId = Schema.is(identifierSchema('CityId', CITY_ID_RE));
export const isCursor = Schema.is(identifierSchema('Cursor', CURSOR_RE));
export const isCatalogId = Schema.is(identifierSchema('CatalogId', CATALOG_RE));

const ActorTypeSchema = Schema.Literal('player', 'city', 'unit').annotations({
  identifier: 'ActorType',
});
export const isActorType = Schema.is(ActorTypeSchema);

/** `"unit_0123…"` → `"unit"`. Python spells this `actor_id.split("_", 1)[0]`. */
export const idPrefix = (identifier: string): string => {
  const cut = identifier.indexOf('_');
  return cut < 0 ? identifier : identifier.slice(0, cut);
};

/** Report whether a stored entity alias still names its own ID type. */
export const entityAliasIdMatches = (alias: string, identifier: JsonValue): boolean => {
  const match = ENTITY_ALIAS_RE.exec(alias);
  if (match === null || !isJsonString(identifier)) {
    return false;
  }
  const prefix = match[1];
  if (prefix === undefined) {
    return false;
  }
  const kind = ALIAS_ENTITY_TYPES.get(prefix);
  if (kind === undefined) {
    return false;
  }
  const pattern = kind === 'relation' ? RELATION_ID_RE : ACTOR_ID_RE;
  return pattern.test(identifier) && identifier.startsWith(`${kind}_`);
};
