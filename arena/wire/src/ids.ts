/**
 * Identifiers and the run-state vocabulary, branded so a raw `string` can
 * never be mistaken for a validated one.
 *
 * Every rule here is transcribed from the Python gateway, which remains the
 * authority while both implementations run side by side. `test/ids.test.ts`
 * re-reads `arena/archive/agent_eval/replay_gateway.py` and compares the patterns and
 * vocabularies directly.
 *
 * @module
 */

import { Either, Schema } from 'effect';
import { decodeWire, type WireDecoder } from './codec.ts';

/**
 * `GAME_ID_RE` — `arena/archive/agent_eval/replay_gateway.py:35`:
 * `re.compile(r"^[A-Za-z0-9_-]{20,80}$")`.
 *
 * The gateway applies it with `re.fullmatch` at every trust boundary: reading
 * a manifest (`:558`), building a disk index row (`:1126`), scanning the runs
 * directory (`:1254`), filtering upstream live rows (`:1631`) and routing a
 * request path (`:1991`).  `fullmatch` plus the explicit anchors makes the
 * Python and JavaScript semantics identical here — in particular Python's `$`
 * would otherwise also match before a trailing newline, and `fullmatch`
 * forbids that.
 */
export const GAME_ID_RE: RegExp = /^[A-Za-z0-9_-]{20,80}$/;

/**
 * A validated game id: 20–80 characters of `[A-Za-z0-9_-]`.
 *
 * The character class is exactly URL-safe base64 minus padding, which is why
 * a game id is safe to interpolate into a path segment — and why the gateway
 * treats "matches this pattern" as sufficient to keep a filesystem lookup
 * inside `runs_root`.
 */
export const GameId = Schema.String.pipe(
  Schema.pattern(GAME_ID_RE, {
    identifier: 'GameId',
    description: 'Gateway game id: 20-80 chars of [A-Za-z0-9_-] (GAME_ID_RE)',
  }),
  Schema.brand('GameId'),
);
/** A validated game id. Obtain one via {@link decodeGameId} or `GameId.make`. */
export type GameId = typeof GameId.Type;

/** Decode an unknown value as a {@link GameId}. */
export const decodeGameId: WireDecoder<GameId> = decodeWire(GameId, 'GameId');

/** True when `input` is a well-formed game id. */
export const isGameId = (input: string): input is GameId => GAME_ID_RE.test(input);

/**
 * `FRAME_INDEX_RE` — `arena/archive/agent_eval/replay_gateway.py:36`:
 * `re.compile(r"^(?:0|[1-9][0-9]*)\.png$")`.
 *
 * Matched against the last path segment of `/v1/games/{id}/frames/{n}.png`
 * (`:2031`), after which the gateway takes the index as `int(suffix[1][:-4])`
 * (`:2034`).  The `(?:0|[1-9][0-9]*)` alternation is the load-bearing part:
 * **leading zeros are forbidden**, so `7.png` is the one and only URL for
 * frame 7 and `007.png` is a 404.  One frame, one URL — no cache-splitting
 * aliases.
 *
 * This is *not* the on-disk name.  Frames are stored zero-padded
 * (`ARCHIVE_PNG_RE = ^([0-9]{6})\.png$`, `:37`) and the gateway maps between
 * the two; a port must keep the padded form off the wire.
 */
export const FRAME_INDEX_RE: RegExp = /^(?:0|[1-9][0-9]*)\.png$/;

/**
 * The digits-only form of {@link FRAME_INDEX_RE}, sans `.png` suffix.
 *
 * **Not** an upstream identifier: the Python has no such constant.  It exists
 * because the index also travels as a bare decimal — `_archive_frames` emits
 * `index` as a number and the catalog spells it without the suffix — and the
 * two must not be spelled by the same name.  {@link FRAME_INDEX_RE} is the one
 * that gates a *route path segment*; this one gates a value.
 */
export const FRAME_INDEX_DIGITS_RE: RegExp = /^(?:0|[1-9][0-9]*)$/;

/**
 * A frame index: a non-negative integer, zero-based, dense over an archive's
 * frames (`_archive_frames`, `:968`, emits `index` straight from the padded
 * file name).
 *
 * No upper bound is imposed, matching Python's unbounded `int`.  Values above
 * `Number.MAX_SAFE_INTEGER` would lose precision, but a frame index is one
 * per game turn — six digits on disk — so the bound is unreachable in
 * practice and a divergence-by-rejection would be the worse trade.
 */
export const FrameIndex = Schema.Number.pipe(
  Schema.int(),
  Schema.nonNegative(),
  // Annotations go inside `brand`, not in a trailing `.annotations()` call:
  // the latter widens the result out of `BrandSchema` and takes `.make`'s
  // branded return type with it.
  Schema.brand('FrameIndex', {
    identifier: 'FrameIndex',
    description: 'Zero-based index of an archived map frame',
  }),
);
/** A validated frame index. */
export type FrameIndex = typeof FrameIndex.Type;

/** Decode an unknown value as a {@link FrameIndex}. */
export const decodeFrameIndex: WireDecoder<FrameIndex> = decodeWire(
  FrameIndex,
  'FrameIndex',
);

/**
 * The canonical *string* form of a frame index — digits with no leading
 * zeros, per {@link FRAME_INDEX_DIGITS_RE}.  Encoding produces the canonical form,
 * so a decode/encode round trip normalizes nothing and rejects everything
 * non-canonical: `"0"` decodes, `"00"` and `"007"` do not.
 */
export const FrameIndexFromString: Schema.Schema<FrameIndex, string> = Schema.transform(
  Schema.String.pipe(
    Schema.pattern(FRAME_INDEX_DIGITS_RE, {
      identifier: 'FrameIndexString',
      description: 'Decimal frame index with no leading zeros (FRAME_INDEX_DIGITS_RE)',
    }),
  ),
  FrameIndex,
  {
    strict: true,
    // Total: the pattern admits only non-negative integer literals, so the
    // brand constructor cannot reject what reaches it.
    decode: (text) => FrameIndex.make(Number(text)),
    encode: (index) => String(index),
  },
).annotations({ identifier: 'FrameIndexFromString' });

/** Decode a frame index from its canonical decimal string. */
export const decodeFrameIndexFromString: WireDecoder<FrameIndex> =
  decodeWire(FrameIndexFromString);

/**
 * The frame's URL path segment, `"{index}.png"` — the exact thing
 * `FRAME_INDEX_RE` is matched against at `:2031`.
 */
export const FrameIndexFromPngName: Schema.Schema<FrameIndex, string> = Schema.transform(
  Schema.String.pipe(
    Schema.pattern(FRAME_INDEX_RE, {
      identifier: 'FramePngName',
      description: 'Frame PNG path segment, e.g. "12.png" (FRAME_INDEX_RE)',
    }),
  ),
  FrameIndex,
  {
    strict: true,
    decode: (name) => FrameIndex.make(Number(name.slice(0, -4))),
    encode: (index) => `${String(index)}.png`,
  },
).annotations({ identifier: 'FrameIndexFromPngName' });

/** Decode a frame index from a `"{n}.png"` path segment. */
export const decodeFrameIndexFromPngName: WireDecoder<FrameIndex> =
  decodeWire(FrameIndexFromPngName);

/**
 * `TERMINAL_STATES` — `arena/archive/agent_eval/replay_gateway.py:43`:
 * `{"completed", "invalid", "failed", "cancelled"}`.
 *
 * A run in one of these states has a finalized manifest, so the gateway will
 * serve its archive routes (`_terminal_archive`, `:778`) and will not relabel
 * it as interrupted (`:1592`).  Only `"completed"` can carry
 * `benchmark_valid: true` (`:790`).
 */
export const TERMINAL_RUN_STATES = ['completed', 'invalid', 'failed', 'cancelled'] as const;

/**
 * States a run reports while the supervisor still owns it.  These reach the
 * gateway index verbatim from the upstream live rows (`:1637`); the
 * supervisor writes `"lobby"` when the game is created
 * (`arena/archive/agent_eval/supervisor.py:10769`) and advances through `"starting"` to
 * `"running"` (`supervisor.py:4063`, `supervisor.py:3916`).
 */
export const LIVE_RUN_STATES = ['lobby', 'starting', 'running'] as const;

/**
 * States the gateway synthesizes rather than reads.
 *
 * - `"interrupted"` — `_as_interrupted`, `:1211`, sets `row["state"]` at
 *   `:1227` for a run that is on disk, is not live upstream, and never
 *   reached a terminal state: the supervisor went away before finalizing the
 *   manifest.  Its replay is still watchable, so the row must not vanish from
 *   the index; a run with no recorded turns is dropped instead of relabeled.
 * - `"unknown"` — the `_public_text(..., "unknown", 32)` fallback used
 *   wherever a manifest's `state`/`status` is missing or not a usable string
 *   (`:776`, `:1131`, `:1698`, `:1772`).
 */
export const DERIVED_RUN_STATES = ['interrupted', 'unknown'] as const;

/**
 * Every run state the gateway is known to emit.
 *
 * This is the current version's closed vocabulary. A future state requires a
 * new schema version and an explicit migration rather than a branded fallback.
 */
export const RunState = Schema.Literal(
  ...LIVE_RUN_STATES,
  ...TERMINAL_RUN_STATES,
  ...DERIVED_RUN_STATES,
).annotations({
  identifier: 'RunState',
  description: 'A run state the replay gateway is known to emit',
});
/** A run state the port understands. */
export type RunState = typeof RunState.Type;

/** Decode an unknown value as a known {@link RunState}. */
export const decodeRunState: WireDecoder<RunState> = decodeWire(RunState, 'RunState');

const TERMINAL_RUN_STATE_SET: ReadonlySet<string> = new Set<string>(TERMINAL_RUN_STATES);

/**
 * The gateway's `state in TERMINAL_STATES` test (`:778`, `:1136`, `:1592`).
 * Accepts any string so it can be asked about a value that has not been
 * narrowed yet — an unrecognized state is, correctly, not terminal.
 */
export const isTerminalRunState = (state: string): state is (typeof TERMINAL_RUN_STATES)[number] =>
  TERMINAL_RUN_STATE_SET.has(state);

/** True when a string belongs to the current schema version. */
export const isKnownRunState = (state: string): state is RunState =>
  Either.isRight(decodeRunState(state));
