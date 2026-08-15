import { Option, Predicate } from 'effect';
import { createError, EvlogError, type ErrorOptions } from 'evlog';

interface ParsedFailure {
  readonly tag: string;
  readonly message: string;
  readonly detail: string;
  readonly error: Option.Option<Error>;
}

/** Parse an untrusted failure once; the rest of telemetry uses this domain value. */
const parseFailure = (cause: unknown): ParsedFailure => {
  try {
    const tagged =
      Predicate.isObject(cause) &&
      !Predicate.isFunction(cause) &&
      Predicate.hasProperty(cause, '_tag') &&
      Predicate.isString(cause._tag) &&
      cause._tag !== ''
        ? Option.some(cause._tag)
        : Option.none<string>();
    const tag = Option.getOrElse(
      tagged,
      () => cause instanceof Error && cause.name !== '' ? cause.name : 'Untagged',
    );
    let message: string;
    if (cause instanceof Error) message = cause.message === '' ? tag : cause.message;
    else if (Predicate.isString(cause)) message = cause;
    else if (cause === null) message = 'non-Error failure of type null';
    else if (Predicate.isUndefined(cause)) message = 'non-Error failure of type undefined';
    else if (Predicate.isFunction(cause)) message = 'non-Error failure of type function';
    else if (
      Predicate.isNumber(cause) ||
      Predicate.isBoolean(cause) ||
      Predicate.isBigInt(cause) ||
      Predicate.isSymbol(cause)
    ) message = String(cause);
    else message = 'non-Error failure of type object';

    const wrapped = Predicate.hasProperty(cause, 'cause') ? cause.cause : cause;
    const detail = wrapped instanceof Error
      ? wrapped.message || wrapped.name
      : String(wrapped);
    return {
      tag,
      message,
      detail,
      error: cause instanceof Error ? Option.some(cause) : Option.none(),
    };
  } catch {
    return {
      tag: 'UnreadableFailure',
      message: '@arena/telemetry could not read the failure value',
      detail: '<unreadable>',
      error: Option.none(),
    };
  }
};

/** Map an arbitrary failure to the small structured error shape evlog records. */
export const toEvlogError = (cause: unknown): EvlogError => {
  try {
    if (EvlogError.isEvlogError(cause)) return cause;
    const parsed = parseFailure(cause);
    const options: ErrorOptions = {
      message: parsed.message,
      code: parsed.tag,
    };
    if (Option.isSome(parsed.error)) options.cause = parsed.error.value;
    return createError(options);
  } catch {
    return createError({
      message: '@arena/telemetry could not read the failure value',
      code: 'UnreadableFailure',
    });
  }
};

export interface FailureSummary {
  readonly reason: string;
  readonly detail: string;
}

/** A total, log-safe description of a telemetry backend failure. */
export const summarizeFailure = (cause: unknown): FailureSummary => {
  const parsed = parseFailure(cause);
  return { reason: parsed.tag, detail: parsed.detail };
};
