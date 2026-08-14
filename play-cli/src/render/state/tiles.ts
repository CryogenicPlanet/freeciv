/**
 * The tile sections — `tile_window`, `known_tiles`, `map_tiles`.
 *
 * Ports `_terrain_code` / `_terrain_codes` (client.py:4433-4466), `_tile_cells`
 * (4469-4486) and `_render_tiles` (4489-4542).
 *
 * A terrain glyph must mean the same thing on every page: a code derived from
 * whichever terrains happened to appear on one page would silently rename
 * Desert when Deep Ocean joined it, and an agent reading two windows would
 * route a land unit onto water.  `TERRAIN_CODES` (constants) carries the fixed
 * table; everything else is a pure function of the terrain's own name.
 */
import { Effect } from 'effect';
import type { PlayerError } from 'src/errors';
import { TERRAIN_CODES } from 'src/constants';
import {
  drift,
  isJsonObject,
  named,
  needInt,
  needText,
  scalar,
  table,
  type AliasMap,
  type JsonValue,
} from 'src/render/primitives';
import { field, isJsonString } from 'src/schema/primitives';

/** CPython `str.isalnum()` over one character. */
const ALPHANUMERIC = /[\p{L}\p{N}]/u;

/** Derive one terrain's code from its own name and nothing else. */
export const terrainCode = (name: string): string => {
  const known = TERRAIN_CODES.get(name);
  if (known !== undefined) return known;
  const letters = [...name].filter((character) => ALPHANUMERIC.test(character));
  const head = (letters.at(0) ?? 'X').toUpperCase();
  const tail = (letters.at(1) ?? '0').toLowerCase();
  return head + tail;
};

const DIGITS = '0123456789';

/**
 * Assign each terrain a page-independent two-character code.
 *
 * Only a genuine collision between two unlisted names is broken here, and it is
 * broken deterministically over the colliding names alone — so a name keeps its
 * code whenever the same set of names collides, on any page.
 */
export const terrainCodes = (names: ReadonlySet<string>): ReadonlyMap<string, string> => {
  const collisions = new Map<string, string[]>();
  for (const name of [...names].toSorted()) {
    const code = terrainCode(name);
    const bucket = collisions.get(code);
    if (bucket === undefined) collisions.set(code, [name]);
    else bucket.push(name);
  }
  const codes = new Map<string, string>();
  for (const [code, colliding] of collisions) {
    const only = colliding.length === 1 ? colliding.at(0) : undefined;
    if (only !== undefined) {
      codes.set(only, code);
      continue;
    }
    [...colliding].toSorted().forEach((name, index) => {
      codes.set(
        name,
        index === 0
          ? code
          : (code.at(0) ?? 'X') + (DIGITS.at(Math.min(index - 1, 9)) ?? '9')
      );
    });
  }
  return codes;
};

/** One cell of the rendered window: its terrain (or `null`) and visibility. */
export type TileCell = readonly [terrain: string | null, visibility: string];

export interface TileCells {
  readonly cells: ReadonlyMap<string, TileCell>;
  readonly xs: ReadonlyArray<number>;
  readonly ys: ReadonlyArray<number>;
  readonly terrains: ReadonlySet<string>;
}

const cellKey = (x: number, y: number): string => `${x},${y}`;

export const tileCells = (
  items: ReadonlyArray<JsonValue>
): Effect.Effect<TileCells, PlayerError> =>
  Effect.gen(function* () {
    const cells = new Map<string, TileCell>();
    const xs: number[] = [];
    const ys: number[] = [];
    const terrains = new Set<string>();
    for (const item of items) {
      const x = yield* needInt(item, 'x', 'tile');
      const y = yield* needInt(item, 'y', 'tile');
      const visibility = yield* needText(item, 'visibility', 'tile');
      const terrain = isJsonObject(item) ? field(item, 'terrain') : null;
      xs.push(x);
      ys.push(y);
      if (terrain === null) {
        cells.set(cellKey(x, y), [null, visibility]);
        continue;
      }
      if (!isJsonString(terrain) || terrain === '') {
        return yield*drift('tile terrain');
      }
      terrains.add(terrain);
      cells.set(cellKey(x, y), [terrain, visibility]);
    }
    return {
      cells,
      xs: [...new Set(xs)].toSorted((left, right) => left - right),
      ys: [...new Set(ys)].toSorted((left, right) => left - right),
      terrains,
    };
  });

/** Render a small coordinate grid; fogged and unknown tiles render `?`. */
export const renderTiles = (
  items: ReadonlyArray<JsonValue>,
  aliases: AliasMap | null = null
): Effect.Effect<ReadonlyArray<string>, PlayerError> =>
  Effect.gen(function* () {
    void aliases;
    const { cells, xs, ys, terrains } = yield* tileCells(items);
    const firstX = xs.at(0) ?? 0;
    const lastX = xs.at(-1) ?? firstX;
    const firstY = ys.at(0) ?? 0;
    const lastY = ys.at(-1) ?? firstY;
    const width = lastX - firstX + 1;
    const height = lastY - firstY + 1;
    const codes = terrainCodes(terrains);
    const lines: string[] = [];
    if (width > 40 || width * height > 1024) {
      const sorted = [...cells.entries()].toSorted((left, right) => {
        const [leftXText = '0', leftYText = '0'] = left[0].split(',');
        const [rightXText = '0', rightYText = '0'] = right[0].split(',');
        const leftX = Number(leftXText);
        const leftY = Number(leftYText);
        const rightX = Number(rightXText);
        const rightY = Number(rightYText);
        return leftX === rightX ? leftY - rightY : leftX - rightX;
      });
      lines.push(
        ...table(
          sorted.map(([key, [terrain, visibility]]) => [
            key,
            terrain === null ? '?' : (codes.get(terrain) ?? terrainCode(terrain)),
            visibility,
          ])
        )
      );
    } else {
      const columns: string[] = ['y\\x'];
      for (let x = firstX; x <= lastX; x += 1) columns.push(String(x));
      const grid: string[][] = [columns];
      for (let y = firstY; y <= lastY; y += 1) {
        const row = [String(y)];
        for (let x = firstX; x <= lastX; x += 1) {
          const cell = cells.get(cellKey(x, y));
          if (cell === undefined) row.push('.');
          else if (cell[0] === null) row.push('?');
          else if (cell[1] === 'visible') row.push(codes.get(cell[0]) ?? terrainCode(cell[0]));
          else row.push((codes.get(cell[0]) ?? terrainCode(cell[0])).toLowerCase());
        }
        grid.push(row);
      }
      lines.push(...table(grid));
    }
    const legend = [...codes.entries()]
      .toSorted((left, right) => (left[0] < right[0] ? -1 : left[0] > right[0] ? 1 : 0))
      .map(([name, code]) => `${code}=${name}`);
    legend.push('?=unknown/fogged', '.=not on this page', 'lowercase=remembered');
    lines.push(`legend ${legend.join(' ')}`);
    for (const item of items) {
      const fields = isJsonObject(item) ? item : {};
      const notes: string[] = [];
      const owner = fields['owner_player_id'] ?? null;
      if (owner !== null) notes.push(`owner=${scalar(owner)}`);
      const placement = fields['infrastructure_placement'] ?? null;
      if (placement !== null) notes.push(`building=${named(placement)}`);
      if (notes.length > 0) {
        lines.push(
          `T(${scalar(fields['x'] ?? null)},${scalar(fields['y'] ?? null)}) ` +
            `${notes.join(' ')} ${yield* needText(item, 'id', 'tile')}`
        );
      }
    }
    return lines;
  });
