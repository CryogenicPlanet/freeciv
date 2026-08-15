import { Schema } from 'effect';
import { decodeWire } from './codec.ts';

export type JsonValue = null | boolean | number | string | JsonArray | JsonObject;
export type JsonArray = ReadonlyArray<JsonValue>;
export type JsonObject = { readonly [key: string]: JsonValue };

export const JsonValue: Schema.Schema<JsonValue> = Schema.suspend(() =>
  Schema.Union(Schema.Null, Schema.Boolean, Schema.JsonNumber, Schema.String, JsonArray, JsonObject),
);
export const JsonArray: Schema.Schema<JsonArray> = Schema.Array(
  Schema.suspend((): Schema.Schema<JsonValue> => JsonValue),
);
export const JsonObject: Schema.Schema<JsonObject> = Schema.Record({
  key: Schema.String,
  value: Schema.suspend((): Schema.Schema<JsonValue> => JsonValue),
});

export const decodeJsonValue = decodeWire(JsonValue);
export const decodeJsonObject = decodeWire(JsonObject);
export const decodeJsonArray = decodeWire(JsonArray);
export const isJsonValue = Schema.is(JsonValue);
export const isJsonObject = Schema.is(JsonObject);
export const isJsonArray = Schema.is(JsonArray);
export const jsonField = (value: JsonValue, key: string): JsonValue | undefined =>
  isJsonObject(value) && Object.hasOwn(value, key) ? value[key] : undefined;
export const JsonValueFromString = Schema.parseJson(JsonValue);
export const decodeJsonValueFromString = decodeWire(JsonValueFromString);
