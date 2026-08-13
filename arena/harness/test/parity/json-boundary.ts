/**
 * Effect-schema parsers for JSON read off the disk or wire in parity tests.
 *
 * Every `unknown` boundary in this directory should decode through here rather
 * than `typeof`/`as`/`Record<string, unknown>`.
 */

import {
  decodeJsonValueFromString,
  isJsonArray,
  isJsonObject,
  jsonField,
  type JsonObject,
  type JsonValue,
} from '@arena/wire';
import { Either, Schema } from 'effect';
import type { AddressInfo } from 'node:net';

/** A gateway ready record after {@link parseGatewayReadyRecord} succeeds. */
export type GatewayReadyRecord = JsonObject & Readonly<{ port: number; url: string }>;

const NumberSchema = Schema.Number;
const StringSchema = Schema.String;

/** Parse JSON text; non-objects and invalid JSON become `null`. */
export const parseJsonValueFromText = (text: string): JsonValue | null =>
  Either.getOrNull(decodeJsonValueFromString(text));

/** Parse JSON text as an object; arrays and invalid JSON become `null`. */
export const parseJsonObjectFromText = (text: string): JsonObject | null => {
  const value = parseJsonValueFromText(text);
  return value !== null && isJsonObject(value) ? value : null;
};

/** Ready file with a numeric `port` and string `url`. */
export const parseGatewayReadyRecord = (value: JsonValue): GatewayReadyRecord | null => {
  if (!isJsonObject(value)) return null;
  const port = jsonField(value, 'port');
  const url = jsonField(value, 'url');
  if (!Schema.is(NumberSchema)(port) || !Schema.is(StringSchema)(url)) return null;
  return { ...value, port, url };
};

/** The `port` field when it is a finite number. */
const jsonNumberField = (value: JsonObject, key: string): number | null => {
  const field = jsonField(value, key);
  return field !== undefined && Schema.is(NumberSchema)(field) ? field : null;
};

/** The `port` of a `http://127.0.0.1:NNNNN` service URL, when it has one. */
export const portsFromServiceUrl = (value: string): ReadonlyArray<number> => {
  if (!URL.canParse(value)) return [];
  const port = Number(new URL(value).port);
  return Number.isInteger(port) && port > 0 ? [port] : [];
};

/** Every listening port named by a live-stack ready record. */
export const portsFromStackRecord = (record: JsonObject): ReadonlyArray<number> => {
  const direct = jsonNumberField(record, 'port');
  return [
    ...(direct === null ? [] : [direct]),
    ...(['url', 'internal_service_url', 'upstream_service_url'] as const).flatMap((key) => {
      const field = jsonField(record, key);
      return field !== undefined && Schema.is(StringSchema)(field) ? portsFromServiceUrl(field) : [];
    }),
  ];
};

/** The `pid` field when it is a finite number. */
export const pidFromStackRecord = (record: JsonObject): number | null =>
  jsonNumberField(record, 'pid');

/** Flat JSON object: every value is a primitive or null, never nested. */
export const isFlatJsonObject = (value: JsonValue): boolean =>
  isJsonObject(value) &&
  Object.values(value).every((entry) => !isJsonObject(entry) && !isJsonArray(entry));

/** A piped subprocess stream, or nothing when stdio was not `'pipe'`. */
type PipedSubprocessStream =
  | ReadableStream<Uint8Array>
  | number
  | null
  | undefined;

/** Drain a piped stream to text; absent streams become empty. */
export const pipedStreamText = (stream: PipedSubprocessStream): Promise<string> =>
  stream instanceof ReadableStream ? new Response(stream).text() : Promise.resolve('');

const isListenAddressInfo = (address: string | AddressInfo): address is AddressInfo =>
  Object.prototype.hasOwnProperty.call(address, 'port');

/** TCP listen address port; pipe and unix addresses become zero. */
export const portFromListenAddress = (address: string | AddressInfo | null): number => {
  if (address === null) return 0;
  return isListenAddressInfo(address) ? address.port : 0;
};
