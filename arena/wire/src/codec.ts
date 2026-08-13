import { Data, Either, ParseResult, Schema } from 'effect';

const options = {
  errors: 'all',
  onExcessProperty: 'error',
  propertyOrder: 'original',
} as const;

export interface WireIssue {
  readonly kind: string;
  readonly path: ReadonlyArray<string>;
  readonly message: string;
}

const issues = (error: ParseResult.ParseError): ReadonlyArray<WireIssue> =>
  ParseResult.ArrayFormatter.formatErrorSync(error).map((issue) => ({
    kind: issue._tag,
    path: issue.path.map(String),
    message: issue.message,
  }));

export class WireDecodeError extends Data.TaggedError('WireDecodeError')<{
  readonly schemaName: string;
  readonly message: string;
  readonly issues: ReadonlyArray<WireIssue>;
}> {}

export class WireEncodeError extends Data.TaggedError('WireEncodeError')<{
  readonly schemaName: string;
  readonly message: string;
  readonly issues: ReadonlyArray<WireIssue>;
}> {}

export type WireDecoder<A> = (input: unknown) => Either.Either<A, WireDecodeError>;
export type WireEncoder<A, I> = (value: A) => Either.Either<I, WireEncodeError>;

/** Decode one supported packet shape. Unknown fields are version errors. */
export const decodeWire = <A, I>(schema: Schema.Schema<A, I, never>, name = 'wire value'): WireDecoder<A> => {
  const decode = Schema.decodeUnknownEither(schema, options);
  return (input) =>
    Either.mapLeft(decode(input), (error) =>
      new WireDecodeError({
        schemaName: name,
        message: ParseResult.TreeFormatter.formatErrorSync(error),
        issues: issues(error),
      }));
};

/** Encode the current packet shape. */
export const encodeWire = <A, I>(schema: Schema.Schema<A, I, never>, name = 'wire value'): WireEncoder<A, I> => {
  const encode = Schema.encodeEither(schema, options);
  return (value) =>
    Either.mapLeft(encode(value), (error) =>
      new WireEncodeError({
        schemaName: name,
        message: ParseResult.TreeFormatter.formatErrorSync(error),
        issues: issues(error),
      }));
};

export const isWire = <A, I>(schema: Schema.Schema<A, I>): ((input: unknown) => input is A) =>
  Schema.is(schema, options);
