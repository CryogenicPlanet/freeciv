/**
 * The one authoritative technology-catalog schema used for both
 * `replay-catalog.json` and the catalog embedded in `replay.json`.
 *
 * Two producers write the on-disk file under `schema_version: 1`: the Lua
 * strategic-v1 bridge omits `requires` and `depth`, while the v2 autosave
 * reconstruction includes them. The supervisor republishes the validated
 * catalog without changing its shape, so separate disk and response schemas
 * only create an opportunity for their bounds to drift.
 *
 * @module
 */

import { Schema } from 'effect';
import { WireInt, WireNonNegativeInt, WireNumber } from '../numeric.ts';
import {
  decodeTolerant,
  encodeTolerant,
  type TolerantDecoder,
  type TolerantEncoder,
} from '../tolerant.ts';

/** Highest Freeciv technology id accepted by the catalog reader (`supervisor.py:427`). */
export const MAX_TECHNOLOGY_ID = 511;

/** A Freeciv technology id, including prerequisite and replay-state references. */
export const TechnologyId = WireInt.pipe(
  Schema.betweenBigInt(0n, BigInt(MAX_TECHNOLOGY_ID)),
).annotations({ identifier: 'TechnologyId' });
/** A Freeciv technology id in the inclusive range 0..511. */
export type TechnologyId = typeof TechnologyId.Type;

/**
 * One technology from the ruleset catalog.
 *
 * `cost_base` remains {@link WireNumber}: the Lua producer writes real float
 * costs while the autosave reconstruction writes integer zero. `requires` and
 * `depth` are optional because both producer shapes exist at schema version 1.
 */
export const Technology = Schema.Struct({
  id: TechnologyId,
  rule_name: Schema.String,
  name: Schema.String,
  cost_base: WireNumber,
  requires: Schema.optional(Schema.Array(TechnologyId)),
  depth: Schema.optional(WireNonNegativeInt),
}).annotations({ identifier: 'Technology' });
/** One technology in a replay catalog. */
export type Technology = typeof Technology.Type;

/** The catalog used identically on disk and when embedded in a replay response. */
export const TechnologyCatalog = Schema.Struct({
  schema_version: WireNonNegativeInt,
  technologies: Schema.Array(Technology),
}).annotations({ identifier: 'TechnologyCatalog' });
/** A replay technology catalog. */
export type TechnologyCatalog = typeof TechnologyCatalog.Type;

/** Decode either an on-disk or embedded technology catalog. */
export const decodeTechnologyCatalog: TolerantDecoder<TechnologyCatalog> = decodeTolerant(
  TechnologyCatalog,
  'TechnologyCatalog',
);

/** Re-encode a decoded technology catalog, preserving unknown fields. */
export const encodeTechnologyCatalog: TolerantEncoder<
  TechnologyCatalog,
  typeof TechnologyCatalog.Encoded
> = encodeTolerant(TechnologyCatalog, 'TechnologyCatalog');
