/**
 * Wire-payload primitives.
 *
 * Ports `play/client.py` 1247-1300 (`_exact`, `_opaque`, `_json_value`),
 * 2295-2305 (`_safe_number`) and 1010-1056 (`_validate_evaluation_context`,
 * which `_validate_health` and `_v2_session` both need and so cannot live in a
 * command unit).
 *
 * Decoded values keep the wire's own snake_case keys. That is deliberate:
 * `--json` prints the validated envelope, so the decoded object has to be the
 * object that gets serialized, byte for byte.
 */
import { Effect, Schema } from 'effect';
import { invalid, type DriftError } from 'src/errors';
import { OPAQUE_ID_RE, V2_EVALUATION_FIELDS } from 'src/constants';

// ---------------------------------------------------------------------------
// JSON value domain
// ---------------------------------------------------------------------------

export type JsonValue = null | boolean | number | string | JsonArray | JsonObject;
export interface JsonArray extends ReadonlyArray<JsonValue> {}
export interface JsonObject {
  readonly [key: string]: JsonValue;
}

/** Input accepted by JSON boundary decoders before validation. */
export type JsonValueInput = Schema.Schema.Encoded<typeof Schema.Unknown>;

const UnknownRecordSchema = Schema.Record({
  key: Schema.String,
  value: Schema.Unknown,
});
const JsonScalarSchema = Schema.Union(
  Schema.Null,
  Schema.Boolean,
  Schema.JsonNumber,
  Schema.String
);
const isUnknownRecord = Schema.is(UnknownRecordSchema);
const isJsonScalarDomain = Schema.is(JsonScalarSchema);
const hasJsonPrototype = (value: Record<string, JsonValueInput>): boolean => {
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
};

const isJsonValueDomain = (value: JsonValueInput): value is JsonValue => {
  if (isJsonScalarDomain(value)) return true;
  if (Array.isArray(value)) return value.every(isJsonValueDomain);
  return isUnknownRecord(value) &&
    hasJsonPrototype(value) &&
    Object.values(value).every(isJsonValueDomain);
};

const isJsonObjectDomain = (value: JsonValueInput): value is JsonObject =>
  isUnknownRecord(value) &&
  hasJsonPrototype(value) &&
  Object.values(value).every(isJsonValueDomain);

/**
 * The recursive domain accepted from JSON.parse, HTTP bodies, and private JSON files.
 * A declaration validates in place instead of rebuilding records through ordinary
 * property assignment, so an own `__proto__` key stays an own enumerable key.
 */
export const JsonValueSchema: Schema.Schema<JsonValue> = Schema.declare(
  isJsonValueDomain,
  { identifier: 'JsonValue' }
);

/** The object branch of {@link JsonValueSchema}, preserving every own JSON key. */
export const JsonObjectSchema: Schema.Schema<JsonObject> = Schema.declare(
  isJsonObjectDomain,
  { identifier: 'JsonObject' }
);

const JsonArraySchema: Schema.Schema<JsonArray> = Schema.declare(
  (value): value is JsonArray => Array.isArray(value) && value.every(isJsonValueDomain),
  { identifier: 'JsonArray' }
);
const JsonStringSchema = Schema.String.annotations({ identifier: 'JsonString' });
const JsonBooleanSchema = Schema.Boolean.annotations({ identifier: 'JsonBoolean' });
const JsonNumberSchema = Schema.JsonNumber.annotations({ identifier: 'JsonNumber' });
const NonEmptyJsonStringSchema = Schema.NonEmptyString.annotations({
  identifier: 'NonEmptyJsonString',
});

/** A mutable JSON object under construction. */
export type MutableJsonObject = Record<string, JsonValue>;

export const isJsonValue = (value: JsonValueInput): value is JsonValue =>
  Schema.is(JsonValueSchema)(value);
export const isJsonObject = (value: JsonValueInput): value is JsonObject =>
  Schema.is(JsonObjectSchema)(value);
export const isJsonArray = (value: JsonValueInput): value is JsonArray =>
  Schema.is(JsonArraySchema)(value);
export const isJsonString = (value: JsonValueInput): value is string =>
  Schema.is(JsonStringSchema)(value);
export const isJsonBoolean = (value: JsonValueInput): value is boolean =>
  Schema.is(JsonBooleanSchema)(value);
export const isJsonNumber = (value: JsonValueInput): value is number =>
  Schema.is(JsonNumberSchema)(value);
export const isJsonScalar = (value: JsonValue): value is null | boolean | number | string =>
  value === null || isJsonBoolean(value) || isJsonNumber(value) || isJsonString(value);

/** Python's `sorted()` over ASCII field names. */
export const sortedNames = (names: Iterable<string>): ReadonlyArray<string> =>
  [...names].toSorted((left, right) => (left < right ? -1 : left > right ? 1 : 0));

/** Read one own property, treating an absent field as `null`. */
export const field = (value: JsonObject, key: string): JsonValue =>
  Object.hasOwn(value, key) ? (value[key] ?? null) : null;

/** Report whether a key is physically present, `null` values included. */
export const hasField = (value: JsonObject, key: string): boolean =>
  Object.prototype.hasOwnProperty.call(value, key);

/** Python's `isinstance(x, int) and not isinstance(x, bool)`. */
export const isWholeNumber = (value: JsonValueInput): value is number =>
  isJsonNumber(value) && Number.isInteger(value);

/** Python's `isinstance(x, (int, float)) and not isinstance(x, bool)`. */
export const isFiniteNumber = (value: JsonValueInput): value is number =>
  Schema.is(JsonNumberSchema)(value);

export const isNonEmptyString = (value: JsonValueInput): value is string =>
  Schema.is(NonEmptyJsonStringSchema)(value);

// ---------------------------------------------------------------------------
// _exact (client.py:1247-1269)
// ---------------------------------------------------------------------------

/** Require a JSON object whose key set is exactly `fields`. */
export const exact = (
  value: JsonValue,
  fields: ReadonlySet<string>,
  label: string
): Effect.Effect<JsonObject, DriftError> => {
  const expected = sortedNames(fields).join(', ');
  if (!isJsonObject(value)) {
    return Effect.fail(invalid(label, `expected a JSON object with exactly ${expected}`));
  }
  const present = new Set(Object.keys(value));
  const missing = sortedNames([...fields].filter((name) => !present.has(name)));
  const unexpected = sortedNames([...present].filter((name) => !fields.has(name)));
  if (missing.length === 0 && unexpected.length === 0) {
    return Effect.succeed(value);
  }
  const parts = [
    missing.length > 0 ? `missing ${missing.join(', ')}` : '',
    unexpected.length > 0 ? `unexpected ${unexpected.join(', ')}` : '',
  ].filter((part) => part !== '');
  return Effect.fail(invalid(label, `${parts.join('; ')}. Expected exactly ${expected}`));
};

// ---------------------------------------------------------------------------
// _opaque (client.py:1271-1274)
// ---------------------------------------------------------------------------

export const opaque = (value: JsonValue, label: string): Effect.Effect<string, DriftError> =>
  isJsonString(value) && OPAQUE_ID_RE.test(value)
    ? Effect.succeed(value)
    : Effect.fail(invalid(label));

// ---------------------------------------------------------------------------
// _json_value (client.py:1277-1300)
// ---------------------------------------------------------------------------

const JSON_MAX_DEPTH = 12;
const JSON_MAX_ITEMS = 8192;
const JSON_MAX_KEYS = 2048;
const JSON_MAX_KEY_BYTES = 128;

const jsonValueSync = (
  value: JsonValueInput,
  label: string,
  depth: number
): { readonly ok: true; readonly value: JsonValue } | { readonly ok: false; readonly error: DriftError } => {
  if (depth > JSON_MAX_DEPTH) {
    return { ok: false, error: invalid(label, 'JSON is nested too deeply') };
  }
  if (value === null || isJsonString(value) || isJsonBoolean(value)) {
    return { ok: true, value };
  }
  if (Schema.is(Schema.Number)(value)) {
    return Number.isFinite(value)
      ? { ok: true, value }
      : { ok: false, error: invalid(label, 'number is not finite') };
  }
  if (Array.isArray(value)) {
    if (value.length > JSON_MAX_ITEMS) {
      return { ok: false, error: invalid(label, 'too many items') };
    }
    const items: JsonValue[] = [];
    for (const item of value) {
      const decoded = jsonValueSync(item, label, depth + 1);
      if (!decoded.ok) return decoded;
      items.push(decoded.value);
    }
    return { ok: true, value: items };
  }
  if (isUnknownRecord(value)) {
    const keys = Object.keys(value);
    if (
      keys.length > JSON_MAX_KEYS ||
      keys.some((key) => key.length === 0 || key.length > JSON_MAX_KEY_BYTES)
    ) {
      return { ok: false, error: invalid(label, 'invalid object') };
    }
    const entries: Array<readonly [string, JsonValue]> = [];
    for (const key of keys) {
      const decoded = jsonValueSync(value[key] ?? null, label, depth + 1);
      if (!decoded.ok) return decoded;
      entries.push([key, decoded.value]);
    }
    return { ok: true, value: Object.fromEntries(entries) };
  }
  return { ok: false, error: invalid(label, 'non-JSON value') };
};

/** Copy arbitrary input into the JSON domain, refusing values the wire cannot carry. */
export const jsonValue = (
  value: JsonValueInput,
  label: string,
  depth = 0
): Effect.Effect<JsonValue, DriftError> => {
  const decoded = jsonValueSync(value, label, depth);
  return decoded.ok ? Effect.succeed(decoded.value) : Effect.fail(decoded.error);
};

/** {@link jsonValue} narrowed to an object, for payload sub-trees. */
export const jsonObject = (
  value: JsonValue,
  label: string
): Effect.Effect<JsonObject, DriftError> =>
  Effect.flatMap(jsonValue(value, label), (copied) =>
    isJsonObject(copied) ? Effect.succeed(copied) : Effect.fail(invalid(label))
  );

// ---------------------------------------------------------------------------
// _safe_number (client.py:2295-2305)
// ---------------------------------------------------------------------------

export interface SafeNumberOptions {
  readonly nullable: true;
}

/** A finite, non-negative number — optionally `null` when requested. */
export function safeNumber(value: JsonValue, label: string): Effect.Effect<number, DriftError>;
export function safeNumber(
  value: JsonValue,
  label: string,
  options: SafeNumberOptions
): Effect.Effect<number | null, DriftError>;
export function safeNumber(
  value: JsonValue,
  label: string,
  options?: SafeNumberOptions
): Effect.Effect<number | null, DriftError> {
  if (value === null && options?.nullable === true) {
    return Effect.succeed(null);
  }
  if (!isJsonNumber(value) || value < 0) {
    return Effect.fail(invalid(label));
  }
  return Effect.succeed(value);
}

// ---------------------------------------------------------------------------
// _validate_evaluation_context (client.py:1010-1056)
// ---------------------------------------------------------------------------

export interface EvaluationContext {
  readonly objective: string;
  readonly max_turns: number;
  readonly turns_remaining: number | null;
}

export interface EvaluationOptions {
  readonly expected?: EvaluationContext | null;
  readonly required?: boolean;
}

const MAX_TURNS_CEILING = 5000;

export const decodeEvaluationContext = (
  value: JsonValue,
  label: string,
  options: EvaluationOptions = {}
): Effect.Effect<EvaluationContext | null, DriftError> => {
  if (!isJsonObject(value)) {
    return Effect.fail(invalid(label));
  }
  const present = Object.keys(value).filter((key) => V2_EVALUATION_FIELDS.has(key));
  if (present.length === 0) {
    return options.required === true
      ? Effect.fail(invalid(label, 'evaluation context is missing'))
      : Effect.succeed(null);
  }
  if (present.length !== V2_EVALUATION_FIELDS.size) {
    return Effect.fail(invalid(label, 'evaluation context is incomplete'));
  }
  const objective = field(value, 'objective');
  const maxTurns = field(value, 'max_turns');
  const turnsRemaining = field(value, 'turns_remaining');
  const malformed =
    !isJsonString(objective) ||
    objective.length === 0 ||
    objective.trim() !== objective ||
    !isWholeNumber(maxTurns) ||
    maxTurns < 1 ||
    maxTurns > MAX_TURNS_CEILING ||
    (turnsRemaining !== null &&
      (!isWholeNumber(turnsRemaining) ||
        turnsRemaining < 0 ||
        turnsRemaining > maxTurns));
  if (malformed) {
    return Effect.fail(invalid(label, 'evaluation context is malformed'));
  }
  const expected = options.expected ?? null;
  if (expected !== null) {
    if (objective !== expected.objective) {
      return Effect.fail(invalid(label, 'evaluation objective changed'));
    }
    if (maxTurns !== expected.max_turns) {
      return Effect.fail(invalid(label, 'evaluation max_turns changed'));
    }
  }
  return Effect.succeed({
    objective,
    max_turns: maxTurns,
    turns_remaining: turnsRemaining,
  });
};

// ---------------------------------------------------------------------------
// The identity every wire decoder checks a response against.
// ---------------------------------------------------------------------------

/** The subset of a loaded session the schema layer reads. */
export interface SessionIdentity {
  readonly gameId: string;
  readonly agentId: string;
  readonly controllerLabel: string;
  readonly place: number | null;
  readonly seatId: string | null;
  readonly playerName: string | null;
  readonly evaluation: EvaluationContext | null;
}
