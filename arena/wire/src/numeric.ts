import { Schema } from 'effect';

/** Python integers use bigint internally so canonical encoding keeps integer spelling. */
export const WireInt: Schema.Schema<bigint, number> = Schema.BigIntFromNumber;
export type WireInt = typeof WireInt.Type;

export const WireNonNegativeInt: Schema.Schema<bigint, number> = WireInt.pipe(
  Schema.nonNegativeBigInt(),
);
export type WireNonNegativeInt = typeof WireNonNegativeInt.Type;

/** Python floats remain numbers so canonical encoding retains float spelling. */
export const WireFloat: Schema.Schema<number> = Schema.JsonNumber;
export type WireFloat = typeof WireFloat.Type;

/** Fields that are legitimately emitted as either an integer or a float. */
export const WireNumber: Schema.Schema<bigint | number, number> = Schema.Union(WireInt, WireFloat);
export type WireNumber = typeof WireNumber.Type;
