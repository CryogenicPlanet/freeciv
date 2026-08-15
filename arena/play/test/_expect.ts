import { Either, Schema } from 'effect';
import { isJsonObject, type JsonObject, type JsonValue } from 'src/schema/primitives';

const stringSchema = Schema.String;
const numberSchema = Schema.Number;
const booleanSchema = Schema.Boolean;
const jsonSchema = Schema.parseJson(Schema.Unknown);

/** Read an indexed test observation only after proving that it exists. */
export const observedAt = <A>(values: ReadonlyArray<A>, index: number): A => {
  const value = values[index];
  if (value === undefined) throw new Error(`expected observation at index ${index}`);
  return value;
};

export const observedFirst = <A>(values: ReadonlyArray<A>): A => observedAt(values, 0);

export const observedLast = <A>(values: ReadonlyArray<A>): A => {
  if (values.length === 0) throw new Error('expected at least one observation');
  return observedAt(values, values.length - 1);
};

/** Decode a fixture field at the point where its JSON representation is inspected. */
export const fixtureString = (value: JsonValue | undefined): string =>
  Schema.decodeUnknownSync(stringSchema)(value);

export const fixtureNumber = (value: JsonValue | undefined): number =>
  Schema.decodeUnknownSync(numberSchema)(value);

export const fixtureBoolean = (value: JsonValue | undefined): boolean =>
  Schema.decodeUnknownSync(booleanSchema)(value);

export const fixtureObject = (value: JsonValue | undefined): JsonObject => {
  if (!isJsonObject(value)) throw new Error('expected a JSON object fixture');
  return value;
};

/** Parse command output and prove that its top-level JSON value is an object. */
export const parseFixtureObject = (text: string): JsonObject => {
  const value = Schema.decodeUnknownSync(jsonSchema)(text);
  if (!isJsonObject(value)) throw new Error('expected a JSON object document');
  return value;
};

export const rightValue = <A, E>(either: Either.Either<A, E>): A => {
  if (Either.isLeft(either)) throw new Error('expected a successful result');
  return either.right;
};

export const leftValue = <A, E>(either: Either.Either<A, E>): E => {
  if (Either.isRight(either)) throw new Error('expected a failed result');
  return either.left;
};
