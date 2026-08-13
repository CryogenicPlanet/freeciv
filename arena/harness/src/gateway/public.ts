/**
 * The `_public_*` family of `agent_eval/replay_gateway.py`, as values.
 *
 * Every bare `:NNN` below cites that file.  These ten functions are the
 * gateway's *sanitizer*: they take a value read off disk — a manifest a
 * supervisor wrote, a loader's derived event log — and narrow it to something
 * a spectator may see.  They are the reason a run directory full of
 * `controller_fingerprint`, `controller_metadata`, `owner_token` and absolute
 * filesystem paths can be served to a browser at all.
 *
 * ## Three rules the port keeps
 *
 * 1. **Total, never failing.**  Python annotates the inputs `Any` and answers
 *    for every one of them: a fallback, a default, an empty list, or `None`
 *    meaning "drop this row".  Nothing here raises, so nothing here returns an
 *    `Either`.  A shape that cannot be published is dropped
 *    ({@link Option.none}), which is the only failure mode in this module.
 * 2. **Allowlist, not denylist.**  A key reaches the output because it was
 *    named here, never because it survived a filter.  Adding a field to a
 *    manifest cannot leak it.
 * 3. **Byte-exact.**  The output is canonicalized by the caller
 *    (`canonicalBytes`, sorted keys, `(",", ":")`), so a Python `int` must be a
 *    `bigint` here and a Python `float` a `number` — see `@arena/wire`'s
 *    `numeric.ts`.  That is why {@link publicInt} answers `bigint` and
 *    {@link publicNumber} answers `number` for the same JSON input.
 *
 * ## Why the return types say `Canonical<...>`
 *
 * A decoded `Gateway.GamePlace` is not directly canonicalizable: `@arena/wire`
 * spells "this key may be absent" as `Schema.optional(...)`, whose type
 * includes `undefined`, and `undefined` is not a `CanonValue`.  Every value
 * this module produces *is* canonicalizable — a key it does not publish is
 * absent, never present-and-`undefined` — and {@link Canonical} is the type
 * that says so, so a caller can hand a result straight to `canonicalBytes`
 * with no conversion pass and no cast.
 *
 * ## Divergences from CPython, all of them deliberate
 *
 * - **`int` vs `float` is unobservable after `JSON.parse`.**  Python's
 *   `isinstance(turn, int)` refuses the `3.0` that `json` produced from
 *   `3.0`; JavaScript cannot tell it from `3`.  Affects {@link publicEvent}'s
 *   `turn` and `weight` only, where Python drops the row and this port keeps
 *   it.  Unreachable from a real loader, whose turns are Python `int`s.
 * - **`str.strip()` is not `String.prototype.trim()`.**  Python strips `\x85`
 *   (NEL) and does not strip U+FEFF; ECMAScript does the opposite.
 *   {@link publicText} uses an explicit transcription of the code points
 *   `str.isspace()` accepts.
 * - **Slicing counts code points.**  `rendered[:limit]` is code points in
 *   Python and UTF-16 units in JavaScript; an astral character would be cut in
 *   half and would change the byte length of the canonical body.
 *
 * @module
 */

import { Option } from 'effect';
import {
  type CanonValue,
  compareCodePoints,
  type ControlProtocol,
  type FrameIndex,
  type GameId,
  Gateway,
  isControlProtocol,
  type JsonValue,
} from '@arena/wire';
import { isCanonRecord } from './python-json.ts';

// ---------------------------------------------------------------------------
// The two shared types
// ---------------------------------------------------------------------------

/**
 * `true` exactly when `A` is the *whole* `JsonValue` union — a field the
 * producer relays without interpreting it.
 *
 * Mutual assignability, not one-way: `number` is assignable to `JsonValue` and
 * must not match, or every float in the protocol would be treated as opaque
 * JSON.
 */
type IsOpaqueJson<A> = [A] extends [JsonValue]
  ? [JsonValue] extends [A]
    ? true
    : false
  : false;

/**
 * A wire payload in the form the canonical writer takes.
 *
 * Three rewrites of the schema's decoded type, applied all the way down:
 *
 * 1. **`undefined` is removed from every optional property.**  `@arena/wire`
 *    spells "this key may be absent" as `Schema.optional(...)`, whose decoded
 *    type includes `undefined` — and `undefined` is not a `CanonValue`.  The
 *    constructors here never write an `undefined`-valued key, so the rewrite
 *    is honest rather than hopeful, and it is what lets a caller hand a result
 *    straight to `canonicalBytes` with no conversion pass and no cast.
 * 2. **A `FrameIndex` becomes a `bigint`.**  Wire types it as a branded
 *    *number* on the stated ground that a frame index is never a field in a
 *    canonicalized body.  It is one: `_archive_frames` writes
 *    `int(match.group(1))` into `_bounded_json` (`:983`), so CPython spells it
 *    `0` and a JavaScript number would spell it `0.0`.  The differential
 *    oracle in `test/gateway/archive.test.ts` catches nothing else.
 * 3. **A field typed as *opaque* `JsonValue` becomes a `CanonValue`.**  Such a
 *    field is relayed, not built — `victory.turn` and `victory.year` are read
 *    out of `victory.json` and passed through unvalidated (`:704`) — so its
 *    integers must keep the `int` spelling CPython read them with.  Only the
 *    whole `JsonValue` union matches; an ordinary `number` field such as
 *    `created_at` is a Python `float` and stays one.
 *
 * The result is *not* assignable back to `A` where rules 2 and 3 apply.  That
 * is the correct direction of travel: `A` describes the document after a
 * decode, this describes the value that reproduces its bytes.
 */
export type Canonical<A> =
  IsOpaqueJson<A> extends true
    ? CanonValue
    : A extends FrameIndex
      ? bigint
      : A extends bigint | boolean | number | string | null
        ? A
        : A extends readonly (infer Element)[]
          ? readonly Canonical<Element>[]
          : A extends object
            ? { readonly [K in keyof A]: Canonical<Exclude<A[K], undefined>> }
            : A;

export type Untrusted = { readonly [key: string]: unknown };

/** `isinstance(value, Mapping)` — a JSON object, never an array. */
export const isUntrusted = (value: unknown): value is Untrusted =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

/**
 * `value.get(key)` — `undefined` when the value is not a mapping or the key is
 * not an **own** property.
 *
 * The `Object.hasOwn` guard is load-bearing: `record["constructor"]` finds an
 * inherited function on any object literal, and a manifest key named
 * `constructor` or `toString` is a value whoever wrote the manifest controls.
 */
export const untrustedField = (value: unknown, key: string): unknown =>
  isUntrusted(value) && Object.hasOwn(value, key) ? value[key] : undefined;

/**
 * `mapping.get(key, mapping.get(fallbackKey))` — the two-argument form Python
 * uses in exactly one shape, `manifest.get("state", manifest.get("status"))`
 * (`:775`, `:1130`).
 *
 * The subtlety is that a **present** `state` of `null` wins over `status`,
 * while an absent one does not: `dict.get` distinguishes the two and `??` does
 * not.
 */
export const untrustedFieldOr = (value: unknown, key: string, fallbackKey: string): unknown =>
  isUntrusted(value) && Object.hasOwn(value, key) ? value[key] : untrustedField(value, fallbackKey);

// ---------------------------------------------------------------------------
// _public_text / _public_int / _public_number
// ---------------------------------------------------------------------------

/** `_public_text`'s default `limit` (`:327`). */
export const PUBLIC_TEXT_LIMIT = 160;

/**
 * `"".join(c for c in value if c in "\t " or ord(c) >= 0x20)` (`:331-333`) —
 * every code point below `0x20` is deleted except the tab Python names
 * explicitly.
 *
 * Written as a filter rather than a regular expression for two reasons: it is
 * the Python line, character for character, and it iterates **code points**,
 * which is the unit Python counts in.  Everything from `0x20` up survives,
 * including `\x7f` and the bidi controls — Python keeps those too.
 */
const withoutControls = (value: string): string =>
  [...value].filter((character) => character === '\t' || character >= ' ').join('');

/**
 * The code points `str.strip()` removes: Unicode `White_Space` plus the four
 * C0 separators `\x1c`-`\x1f`, which Python alone calls whitespace.
 *
 * Differs from `String.prototype.trim()` in exactly two reachable code points:
 * `\x85` (NEL) is whitespace to Python and not to ECMAScript, and U+FEFF is
 * the reverse.  Both survive {@link withoutControls}, so both are
 * observable in a label read off a hand-edited manifest.
 *
 * **The** table: `./config.ts` used to carry a second one as an explicit
 * `ReadonlySet<number>` and now imports {@link pythonStrip} and
 * {@link isPythonSpace} from here.  The two agreed over every code point when
 * they were merged; nothing had made them agree, which is the problem.
 */
const PYTHON_SPACE_CLASS =
  '\\t\\n\\v\\f\\r\\u001C-\\u001F \\u0085\\u00A0\\u1680\\u2000-\\u200A\\u2028\\u2029\\u202F\\u205F\\u3000';

/** {@link PYTHON_SPACE_CLASS} anchored at both ends, for a single `replace`. */
const PYTHON_STRIP_RE = new RegExp(`^[${PYTHON_SPACE_CLASS}]+|[${PYTHON_SPACE_CLASS}]+$`, 'gu');

/** `value.strip()` for a value already known to be a `str`. */
export const pythonStrip = (value: string): string => value.replace(PYTHON_STRIP_RE, '');

/** {@link PYTHON_SPACE_CLASS} as a single-character test — `str.isspace()`. */
const PYTHON_SPACE_RE = new RegExp(`^[${PYTHON_SPACE_CLASS}]$`, 'u');

/**
 * `str.isspace()` for one character.
 *
 * Exported for `./config.ts`'s `_PyUnicode_TransformDecimalAndSpaceToASCII`,
 * which needs to recognize a space rather than strip one — and which must not
 * grow a third copy of the table to do it.
 */
export const isPythonSpace = (character: string): boolean => PYTHON_SPACE_RE.test(character);

/**
 * `_public_text` (`:327`) — the only way a string from disk becomes a string
 * on the wire.
 *
 * Non-strings become `fallback` without inspection; a string is stripped of
 * control characters, `str.strip()`-ed, truncated to `limit` **code points**,
 * and replaced by `fallback` when nothing is left.
 */
export const publicText = (value: unknown, fallback: string, limit = PUBLIC_TEXT_LIMIT): string => {
  if (typeof value !== 'string') return fallback;
  const rendered = pythonStrip(withoutControls(value));
  const truncated = [...rendered].slice(0, limit).join('');
  return truncated === '' ? fallback : truncated;
};

const maxBigInt = (left: bigint, right: bigint): bigint => (left > right ? left : right);

/**
 * `_public_int` (`:337`) — a Python `int`, clamped to `minimum`, or the
 * default.
 *
 * Rejects booleans (Python's explicit `isinstance(value, bool)` guard, which
 * matters because `True` *is* an `int` there), non-finite floats, and any
 * float with a fractional part: `1.5` defaults rather than truncating, while
 * `3.0` is accepted and becomes `3`.
 *
 * `bigint` input is accepted alongside `number` — a loader's output may
 * already have been decoded through `@arena/wire`, where a Python `int` is a
 * `bigint`.  `JSON.parse` cannot produce one, so this widens nothing CPython
 * would have seen.
 */
export const publicInt = (value: unknown, fallback = 0n, minimum = 0n): bigint => {
  if (typeof value === 'bigint') return maxBigInt(minimum, value);
  if (typeof value !== 'number' || !Number.isFinite(value) || !Number.isInteger(value)) {
    return fallback;
  }
  return maxBigInt(minimum, BigInt(value));
};

/**
 * `_public_number` (`:348`) — `float(value)`, or the default.
 *
 * The result stays a JavaScript `number` so the canonical writer spells it the
 * way `repr(float)` does: `publicNumber(180)` is `180.0` on the wire, never
 * `180`.
 */
export const publicNumber = (value: unknown, fallback = 0): number => {
  if (typeof value === 'bigint') return Number(value);
  return typeof value === 'number' && Number.isFinite(value) ? value : fallback;
};

// ---------------------------------------------------------------------------
// _public_event / _public_counts / _public_events
// ---------------------------------------------------------------------------

/**
 * `1 <= weight <= 100` (`:374`), with the `bool` and float rejections.
 *
 * `bigint`, because a Python `int` is a `bigint` in the canonical model
 * (`../python-json.ts`).  That is what makes the two rejections *structural*
 * rather than remembered: `2.0` arrives as a `number` and `True` as a
 * `boolean`, and neither is a `bigint`, so `isinstance(weight, int) and not
 * isinstance(weight, bool)` needs no second clause here.
 */
const isEventWeight = (value: unknown): value is bigint =>
  typeof value === 'bigint' &&
  value >= BigInt(Gateway.GAME_EVENT_MIN_WEIGHT) &&
  value <= BigInt(Gateway.GAME_EVENT_MAX_WEIGHT);

/**
 * `_public_event` (`:358`) — one spectator-safe row, or {@link Option.none}
 * when the shape is unusable.
 *
 * The four rejections (`:365-376`) run *before* any narrowing, so a row with a
 * good `summary` and a `weight` of `0` is dropped rather than clamped.  `data`
 * passes through whole — it is the derivation's own output, not the
 * manifest's — but only when it is an object; a list becomes `{}`.
 *
 * `turn` must be a Python `int`, i.e. a `bigint`: `isinstance(turn, bool) or
 * not isinstance(turn, int)` (`:369`) drops a row whose turn is `2.0` or
 * `True`, and the canonical model is what lets this reproduce both rejections
 * instead of guessing at a `number`.  `data` keeps its own integer spelling
 * for the same reason — it is relayed, not rebuilt.
 */
export const publicEvent = (value: unknown): Option.Option<Canonical<Gateway.GameEvent>> => {
  if (!isUntrusted(value)) return Option.none();
  const turn = untrustedField(value, 'turn');
  const kind = untrustedField(value, 'kind');
  const summary = untrustedField(value, 'summary');
  const weight = untrustedField(value, 'weight');
  if (typeof turn !== 'bigint' || turn < 0n) return Option.none();
  if (typeof kind !== 'string' || kind === '') return Option.none();
  if (typeof summary !== 'string' || summary === '') return Option.none();
  if (!isEventWeight(weight)) return Option.none();
  const actors = untrustedField(value, 'actors');
  const data = untrustedField(value, 'data');
  return Option.some({
    turn,
    kind: publicText(kind, 'event', Gateway.GAME_EVENT_KIND_MAX_LENGTH),
    summary: publicText(summary, '', Gateway.GAME_EVENT_SUMMARY_MAX_LENGTH),
    weight,
    actors: (Array.isArray(actors) ? actors : [])
      .filter((actor): actor is string => typeof actor === 'string')
      .map((actor) => publicText(actor, '', Gateway.GAME_EVENT_ACTOR_MAX_LENGTH))
      .slice(0, Gateway.GAME_EVENT_ACTORS_MAX),
    data: isCanonRecord(data) ? data : {},
  });
};

/** `{kind: count}` once `_public_counts` has narrowed both halves. */
export type PublicCounts = { readonly [kind: string]: bigint };

/**
 * `_public_counts` (`:392`) — narrow every key with {@link publicText} and
 * every value with {@link publicInt}.
 *
 * Two kinds that truncate to the same 40 characters collapse into one entry,
 * last writer wins, exactly as the Python dict comprehension does.
 */
export const publicCounts = (value: unknown): PublicCounts =>
  isUntrusted(value)
    ? Object.fromEntries(
        Object.entries(value).map(([kind, count]) => [
          publicText(kind, 'event', Gateway.GAME_EVENT_KIND_MAX_LENGTH),
          publicInt(count),
        ]),
      )
    : {};

/** `event_warnings` (`:429-439`): `{turn, message}`, message-bearing rows only. */
const publicWarnings = (value: unknown): readonly Canonical<Gateway.ReplayWarning>[] =>
  (Array.isArray(value) ? value : [])
    .filter((row) => typeof untrustedField(row, 'message') === 'string')
    .slice(0, Gateway.GAME_EVENT_WARNINGS_MAX)
    .map((row) => {
      const turn = untrustedField(row, 'turn');
      // `isinstance(row["turn"], int) and not isinstance(..., bool)` (`:431`):
      // a `bigint` and nothing else.
      return {
        turn: typeof turn === 'bigint' ? turn : null,
        message: publicText(
          untrustedField(row, 'message'),
          '',
          Gateway.GAME_EVENT_WARNING_MESSAGE_MAX_LENGTH,
        ),
      };
    });

/**
 * `_public_events` (`:400`) — the whole `events.json` body, reshaped.
 *
 * The three fields that are *not* what the loader said, and the reason this
 * projection can never be skipped on the fallback path:
 *
 * - `schema_version` is hardcoded and `game_id` is the **requested** id
 *   (`:415-416`);
 * - `total_events` defaults to the number of rows that *survived*, so it may
 *   disagree with both `events.length` and the loader's own total (`:420`);
 * - `min_included_weight` is the minimum over the surviving rows, falling back
 *   to the loader's value only when nothing survived (`:423-426`).
 */
export const publicEvents = (
  value: unknown,
  gameId: GameId,
): Canonical<Gateway.GameEventsResponse> => {
  const rows = untrustedField(value, 'events');
  const events = (Array.isArray(rows) ? rows : []).flatMap((row) =>
    Option.match(publicEvent(row), {
      onNone: (): readonly Canonical<Gateway.GameEvent>[] => [],
      onSome: (event) => [event],
    }),
  );
  const weights = events.map((event) => event.weight);
  return {
    schema_version: 1n,
    game_id: gameId,
    available: untrustedField(value, 'available') === true,
    events,
    event_counts: publicCounts(untrustedField(value, 'event_counts')),
    total_events: publicInt(untrustedField(value, 'total_events'), BigInt(events.length)),
    truncated: untrustedField(value, 'truncated') === true,
    omitted_counts: publicCounts(untrustedField(value, 'omitted_counts')),
    min_included_weight:
      weights.length === 0
        ? publicInt(untrustedField(value, 'min_included_weight'))
        : weights.reduce((left, right) => (left <= right ? left : right)),
    last_turn: publicInt(untrustedField(value, 'last_turn')),
    complete: untrustedField(value, 'complete') === true,
    event_warnings: publicWarnings(untrustedField(value, 'event_warnings')),
  };
};

// ---------------------------------------------------------------------------
// _public_timing / _public_control_protocol / _public_ai_difficulty
// ---------------------------------------------------------------------------

/**
 * The empty record `_public_timing` and `_public_control_protocol` return when
 * they decline to publish (`:447`, `:478`).
 *
 * Spelled as a type so `{...base, ...publicTiming(config)}` still type-checks
 * to a record with **no** `undefined`-valued key, which is what
 * {@link Canonical} promises.
 */
export type NoKeys = { readonly [key: string]: never };

/** `{"timing_mode": ..., "action_timeout_s": ...}` — always both, or neither. */
export interface PublicTiming {
  readonly timing_mode: Gateway.TimingMode;
  readonly action_timeout_s: number | null;
}

/** `{"control_protocol": ...}`, or nothing at all. */
export interface PublicControlProtocol {
  readonly control_protocol: ControlProtocol;
}

/** `default` and `blitz` publish only at their canonical timeout (`:456-461`). */
const TIMING_MODE_FIXED_TIMEOUT: ReadonlyMap<string, number> = new Map([
  ['default', 180],
  ['blitz', 60],
]);

/**
 * `_public_timing` (`:443`) — preserve an explicit timing pair, never invent
 * one.
 *
 * Four ways to publish nothing, every one of them in the corpus: an
 * unrecognized `timing_mode`; `infinite` with a non-null timeout; a
 * non-positive, non-finite or non-numeric timeout; and — the subtle one — a
 * `default` or `blitz` archive whose recorded timeout disagrees with the
 * constant this function hardcodes, which is why most `full-control-v2` runs
 * (600 s under a `default` label) publish neither key.
 */
export const publicTiming = (config: unknown): PublicTiming | NoKeys => {
  const mode = untrustedField(config, 'timing_mode');
  const timeout = untrustedField(config, 'action_timeout_s');
  if (typeof mode !== 'string' || !Gateway.isTimingMode(mode)) return {};
  if (mode === 'infinite') {
    // `config.get("action_timeout_s")` is `None` for a *missing* key as much
    // as for an explicit `null`, and `infinite` publishes on both — the
    // differential oracle caught this port requiring the explicit one.
    return timeout === null || timeout === undefined
      ? { timing_mode: mode, action_timeout_s: null }
      : {};
  }
  if (typeof timeout !== 'number' || !Number.isFinite(timeout) || timeout <= 0) return {};
  const fixed = TIMING_MODE_FIXED_TIMEOUT.get(mode);
  if (fixed !== undefined && timeout !== fixed) return {};
  return { timing_mode: mode, action_timeout_s: timeout };
};

/**
 * `_public_control_protocol` (`:468`) — the config's protocol, else the
 * manifest's top-level one, else nothing.
 *
 * An unrecognized value publishes **no key at all** rather than a `null`: the
 * viewer then renders an unqualified match instead of claiming a protocol the
 * run may not have used.
 */
export const publicControlProtocol = (
  config: unknown,
  manifest: unknown,
): PublicControlProtocol | NoKeys => {
  const found = [
    untrustedField(config, 'control_protocol'),
    untrustedField(manifest, 'control_protocol'),
  ].find(
    (candidate): candidate is ControlProtocol =>
      typeof candidate === 'string' && isControlProtocol(candidate),
  );
  return found === undefined ? {} : { control_protocol: found };
};

/**
 * `_public_ai_difficulty` (`:480`) — a recorded server AI level, or `null` for
 * an archive that predates the field.
 *
 * `null` and *absent* are both reachable and mean different things downstream:
 * a status row always carries the key (possibly `null`), while a place carries
 * it only when it resolved.  See {@link publicPlaces}.
 */
export const publicAiDifficulty = (level: unknown): Gateway.AiDifficulty | null =>
  typeof level === 'string' && Gateway.isAiDifficulty(level) ? level : null;

// ---------------------------------------------------------------------------
// _public_places
// ---------------------------------------------------------------------------

/** `_public_places`' field caps (`:501-548`). */
const PLACE_SEAT_LIMIT = 80;
const PLACE_NAME_LIMIT = 80;
const PLACE_COLOR_LIMIT = 16;
const PLACE_CONTROLLER_LIMIT = 32;
const PLACE_LABEL_LIMIT = 80;
const PLACE_TYPE_LIMIT = 32;
const PLACE_MODEL_LIMIT = 80;

/** The conditional tail of a place row: a key is present only when non-blank. */
type PlaceExtras = { readonly [key: string]: string };

/**
 * Python's truthiness, for the `label or "Freeciv Classic AI"` idiom
 * (`:524-528`, `:533`).
 *
 * Every falsy JSON value defaults: `null`, `""`, `0`, `false`, `[]` and `{}`.
 * A truthy non-string (say `5`) is *kept* and then fails the `isinstance(str)`
 * test below, so the key is omitted rather than rendered — which is why this
 * answers `unknown` rather than `string`.
 */
const orDefault = (value: unknown, fallback: string): unknown => {
  if (value === undefined || value === null || value === '' || value === false || value === 0) {
    return fallback;
  }
  if (Array.isArray(value)) return value.length === 0 ? fallback : value;
  if (isUntrusted(value)) return Object.keys(value).length === 0 ? fallback : value;
  return value;
};

/**
 * `if isinstance(x, str) and x.strip(): result[key] = _public_text(x, "", n)`
 * (`:544-552`).
 *
 * The emptiness test is on the **raw** string's `strip()`, not on the narrowed
 * one, so a label of `"\x01"` passes the test and then publishes `""` —
 * present and blank, which no other path produces.
 */
const namedText = (key: string, value: unknown, limit: number): PlaceExtras =>
  typeof value === 'string' && pythonStrip(value) !== ''
    ? { [key]: publicText(value, '', limit) }
    : {};

/**
 * `_public_places` (`:492`) — the seats of a match, as a spectator sees them.
 *
 * The filter (`:505-510`) drops a row that is not an object, whose `place` is
 * below 1, or whose `seat_id` or `player_name` narrows to blank.  What
 * survives carries six always-present keys and up to four conditional ones,
 * and which branch fills them depends on `controller`:
 *
 * - **`native_classic_ai`** — label, type and model default to the Freeciv
 *   constants, and `ai_difficulty` falls back from the per-place field to the
 *   *game's* `config.difficulty` (`:530`), so a row archived before the
 *   per-place field existed still names its level.
 * - **joined external** — `controller_type` defaults to `external` and the
 *   model may be lifted out of `controller_metadata` (`:535-541`).
 *   `controller_metadata` and `controller_fingerprint` are read and then
 *   **dropped**: this is the sanitizer's whole point.
 * - **neither** — an unclaimed place publishes none of the four.
 *
 * The sort is `(place, seat_id)` with the seat id compared by **code point**,
 * as Python compares `str`; `<` on JavaScript strings compares UTF-16 units
 * and disagrees above U+FFFF.
 */
export const publicPlaces = (
  value: unknown,
  manifest?: unknown,
): readonly Canonical<Gateway.GamePlace>[] => {
  const config = untrustedField(manifest, 'config');
  const gameDifficulty = publicAiDifficulty(untrustedField(config, 'difficulty'));
  const rows = (Array.isArray(value) ? value : []).flatMap(
    (raw): readonly Canonical<Gateway.GamePlace>[] => {
      if (!isUntrusted(raw)) return [];
      const place = publicInt(untrustedField(raw, 'place'), -1n, -1n);
      const seatId = publicText(untrustedField(raw, 'seat_id'), '', PLACE_SEAT_LIMIT);
      const playerName = publicText(untrustedField(raw, 'player_name'), '', PLACE_NAME_LIMIT);
      if (place < 1n || seatId === '' || playerName === '') return [];
      const controller = publicText(
        untrustedField(raw, 'controller'),
        'agent',
        PLACE_CONTROLLER_LIMIT,
      );
      const joined = untrustedField(raw, 'joined') === true;
      const rawLabel = untrustedField(raw, 'controller_label');
      const rawType = untrustedField(raw, 'controller_type');
      const rawModel = untrustedField(raw, 'model');
      const metadata = untrustedField(raw, 'controller_metadata');
      const metadataModel = untrustedField(metadata, 'model');
      const native = controller === Gateway.NATIVE_CONTROLLER;
      const label = native ? orDefault(rawLabel, Gateway.NATIVE_CONTROLLER_LABEL) : rawLabel;
      const controllerType = native
        ? orDefault(rawType, Gateway.NATIVE_CONTROLLER_TYPE)
        : joined
          ? orDefault(rawType, Gateway.EXTERNAL_CONTROLLER_TYPE)
          : rawType;
      const model = native
        ? orDefault(rawModel, Gateway.NATIVE_CONTROLLER_MODEL)
        : joined &&
            (rawModel === undefined || rawModel === null) &&
            isUntrusted(metadata) &&
            typeof metadataModel === 'string'
          ? metadataModel
          : rawModel;
      const difficulty = native
        ? (publicAiDifficulty(untrustedField(raw, 'ai_difficulty')) ?? gameDifficulty)
        : null;
      return [
        {
          place,
          seat_id: seatId,
          player_name: playerName,
          player_color: publicText(untrustedField(raw, 'player_color'), '', PLACE_COLOR_LIMIT),
          controller,
          joined,
          ...(difficulty === null ? {} : { ai_difficulty: difficulty }),
          ...namedText('controller_label', label, PLACE_LABEL_LIMIT),
          ...namedText('controller_type', controllerType, PLACE_TYPE_LIMIT),
          ...namedText('model', model, PLACE_MODEL_LIMIT),
        },
      ];
    },
  );
  return rows.toSorted((left, right) =>
    left.place === right.place
      ? compareCodePoints(left.seat_id, right.seat_id)
      : left.place < right.place
        ? -1
        : 1,
  );
};
