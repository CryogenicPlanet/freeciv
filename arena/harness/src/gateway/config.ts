/**
 * Gateway configuration with Python-compatible URL, numeric, path, identity, and loopback rules.
 * JavaScript trim/number/url helpers are not equivalent for the accepted edge cases.
 */

import { FileSystem } from '@effect/platform';
import { Context, Data, Effect, Either, Layer, Option } from 'effect';
import type { Redacted } from 'effect';
import { createHash } from 'node:crypto';
import { homedir } from 'node:os';

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/**
 * The exact `ValueError` texts the Python raises while building a config.
 *
 * They are not HTTP problems — no request has been accepted yet — which is why
 * they are declared here rather than taken from `@arena/wire`'s
 * `gateway/problem.ts`.  `main` prints them verbatim as `error: {message}` on
 * stderr before exiting 2 (`:2206`), so they are observable and must match.
 */
export const GATEWAY_CONFIG_MESSAGES = {
  /** Anything `urlsplit` or `SplitResult.port` rejects (`:132-134`). */
  serviceUrlInvalidPort: 'service URL has an invalid port',
  /** Non-`http(s)` scheme, or no host at all (`:136-137`). */
  serviceUrlNotHttp: 'service URL must be an http(s) URL',
  /** Userinfo, `?query` or `#fragment` present (`:138-141`). */
  serviceUrlCredentials: 'service URL cannot contain credentials, query, or fragment',
  /** A codepoint below `U+0020` survived in the path (`:142-143`). */
  serviceUrlControlCharacters: 'service URL path contains control characters',
  /** A `.` or `..` path segment (`:144-146`). */
  serviceUrlDotSegments: 'service URL path cannot contain dot segments',
  /** `ipaddress.ip_address` could not parse the host (`:166-169`). */
  hostNotLiteral: 'gateway host must be a literal loopback address',
  /** It parsed, but it is not a loopback address (`:170-171`). */
  hostNotLoopback: 'gateway host must be a loopback address',
  /** Non-finite, non-numeric or non-positive `--upstream-timeout-s` (`:194-200`). */
  timeoutNotPositive: 'upstream timeout must be greater than zero',
  /** `--port` outside `[0, 65535]` (`:2082-2083`). */
  portOutOfRange: 'gateway port must be in [0, 65535]',
  /** `Path.expanduser()` with no resolvable home (`pathlib`, not the gateway). */
  noHomeDirectory: 'Could not determine home directory.',
  /**
   * `--materialize-root` equals, contains or sits inside `--cache-root`.
   *
   * TypeScript-only, like the flag pair that can produce it: it is the
   * construction-time form of `save_replay`'s own refusal, moved to the one
   * exit-2 site so it cannot become a 503 on the first replay request.
   */
  materializeRootNestsCacheRoot: 'materialize root must be separate from the replay cache root',
} as const;

/** One of the construction-time failure texts. */
export type GatewayConfigMessage =
  (typeof GATEWAY_CONFIG_MESSAGES)[keyof typeof GATEWAY_CONFIG_MESSAGES];

/**
 * A configuration rejection, carrying the Python `ValueError` text.
 *
 * Errors are values here for the same reason they are everywhere else in this
 * package: the CLI has exactly one place that renders a failure, and it needs
 * the message, not a stack.
 */
export class GatewayConfigError extends Data.TaggedError('GatewayConfigError')<{
  readonly message: string;
}> {}

const fail = (message: string): Either.Either<never, GatewayConfigError> =>
  Either.left(new GatewayConfigError({ message }));

// ---------------------------------------------------------------------------
// Python string primitives
// ---------------------------------------------------------------------------

/**
 * `str.strip()` — whitespace by Python's definition, not JavaScript's.
 *
 * Deliberately *not* `String.prototype.trim()`: Python strips `U+001C`–
 * `U+001F` and `U+0085`, which JavaScript does not, and JavaScript strips
 * `U+FEFF`, which Python does not.  `_normalize_service_url` calls
 * `value.strip()` before parsing (`:131`), so the difference is reachable from
 * `--service-url`.
 *
 * Re-exported from `./public.ts` rather than transcribed a second time.  There
 * used to be two implementations — a recursive index walk over an explicit
 * `ReadonlySet<number>` here, a `RegExp` over a character class there — and I
 * checked them against `Py_UNICODE_ISSPACE` over all 1 114 112 code points and
 * they agreed.  Nothing tied them together, so the *next* correction to
 * Python's whitespace table would have landed on one of them and left
 * `--service-url` validation disagreeing with every label that reaches the
 * wire about what a blank string is.
 */
export { pythonStrip } from './public.ts';

import { isPythonSpace, pythonStrip } from './public.ts';

const ND_PATTERN = /^\p{Nd}$/u;

const isDecimalDigit = (codePoint: number): boolean =>
  codePoint >= 0 && ND_PATTERN.test(String.fromCodePoint(codePoint));

/**
 * The first code point of the maximal run of `Nd` code points containing
 * `codePoint`.
 *
 * Unicode lays decimal digits out in contiguous ascending runs of ten, but
 * *adjacent* runs exist (`U+116D0`+ and the five mathematical alphabets at
 * `U+1D7CE`+), so "walk back until the value is 0" is wrong and "walk back to
 * the run start, then take the offset modulo ten" is right.  Checked against
 * `unicodedata.decimal` for all 760 `Nd` code points; the longest walk is 49
 * steps, so the cap below is never reached in practice and only exists to
 * bound the recursion.
 */
const decimalRunStart = (codePoint: number, steps: number): number =>
  steps >= 64 || !isDecimalDigit(codePoint - 1)
    ? codePoint
    : decimalRunStart(codePoint - 1, steps + 1);

/**
 * `_PyUnicode_TransformDecimalAndSpaceToASCII` — what `int()` and `float()`
 * apply to a non-ASCII string before parsing it: every `Nd` code point becomes
 * the ASCII digit of the same value, and every whitespace code point becomes a
 * plain space.
 *
 * The transform runs *only* for non-ASCII strings, which is why `"5"` is
 * an `int()` error (`U+001C` is ASCII, and the ASCII fast path strips only
 * `" \t\n\v\f\r"`) while `"5"` is `5`.
 */
const transformDecimalAndSpace = (text: string): string =>
  [...text]
    .map((character) => {
      const codePoint = character.codePointAt(0) ?? 0;
      return isDecimalDigit(codePoint)
        ? String((codePoint - decimalRunStart(codePoint, 0)) % 10)
        : isPythonSpace(character)
          ? ' '
          : character;
    })
    .join('');

const isAscii = (text: string): boolean => [...text].every((c) => (c.codePointAt(0) ?? 0) < 0x80);

/** ASCII whitespace as `Py_ISSPACE` sees it, used to trim a numeric literal. */
const ASCII_SPACE = ' \t\n\v\f\r';

const asciiTrim = (text: string): string =>
  text.replace(new RegExp(`^[${ASCII_SPACE}]+|[${ASCII_SPACE}]+$`, 'g'), '');

/**
 * `repr()` of a `str`, close enough for the message argparse builds around a
 * rejected value.  Python prefers single quotes and switches to double quotes
 * only when the value contains a single quote and no double quote.
 */
export const pythonRepr = (text: string): string => {
  const quote = text.includes("'") && !text.includes('"') ? '"' : "'";
  const body = [...text]
    .map((character) => {
      const codePoint = character.codePointAt(0) ?? 0;
      return character === '\\'
        ? '\\\\'
        : character === quote
          ? `\\${quote}`
          : character === '\n'
            ? '\\n'
            : character === '\r'
              ? '\\r'
              : character === '\t'
                ? '\\t'
                : codePoint < 0x20 || codePoint === 0x7f
                  ? `\\x${codePoint.toString(16).padStart(2, '0')}`
                  : character;
    })
    .join('');
  return `${quote}${body}${quote}`;
};

// ---------------------------------------------------------------------------
// Python numeric primitives
// ---------------------------------------------------------------------------

/**
 * Reject a digit group that misuses PEP 515 underscores: they are legal
 * *between* digits only, so no leading, trailing or doubled underscore.
 */
const digitsWithUnderscores = (text: string, pattern: RegExp): Option.Option<string> => {
  const stripped = text.replace(/_/g, '');
  return text.length > 0 &&
    pattern.test(stripped) &&
    !text.startsWith('_') &&
    !text.endsWith('_') &&
    !text.includes('__') &&
    ![...text].some((c, i) => c === '_' && !(pattern.test(text.charAt(i - 1) || '') && pattern.test(text.charAt(i + 1) || '')))
    ? Option.some(stripped)
    : Option.none();
};

const DIGIT = /^[0-9]+$/;

const prepareNumeric = (text: string): string =>
  asciiTrim(isAscii(text) ? text : transformDecimalAndSpace(text));

/**
 * `int(text)` with base 10.
 *
 * Returns a `bigint` because Python's `int` is unbounded and `--port` is
 * range-checked afterwards rather than during parsing — `--port 99999999999999999999`
 * must reach the `gateway port must be in [0, 65535]` message, not overflow
 * into it.
 */
export const pythonInt = (text: string): Either.Either<bigint, GatewayConfigError> => {
  const prepared = prepareNumeric(text);
  const signed = prepared.startsWith('+') || prepared.startsWith('-');
  const digits = signed ? prepared.slice(1) : prepared;
  const parsed = digitsWithUnderscores(digits, DIGIT);
  return Option.isSome(parsed)
    ? Either.right(BigInt(`${prepared.startsWith('-') ? '-' : ''}${parsed.value}`))
    : fail(`invalid literal for int() with base 10: ${pythonRepr(text)}`);
};

const FLOAT_BODY = /^(?:(?:[0-9](?:_?[0-9])*)?\.?(?:[0-9](?:_?[0-9])*)?)$/;

const floatMagnitude = (body: string): Option.Option<number> => {
  const lower = body.toLowerCase();
  return lower === 'inf' || lower === 'infinity'
    ? Option.some(Number.POSITIVE_INFINITY)
    : lower === 'nan'
      ? Option.some(Number.NaN)
      : finiteFloat(body);
};

const finiteFloat = (body: string): Option.Option<number> => {
  const [mantissa, exponent, ...extra] = body.split(/[eE]/);
  return extra.length > 0 || mantissa === undefined
    ? Option.none()
    : Option.flatMap(mantissaDigits(mantissa), (clean) =>
        exponent === undefined
          ? Option.some(Number(clean))
          : Option.map(exponentDigits(exponent), (power) => Number(`${clean}e${power}`)),
      );
};

const mantissaDigits = (mantissa: string): Option.Option<string> => {
  const [whole, fraction, ...extra] = mantissa.split('.');
  return extra.length > 0 || whole === undefined || !FLOAT_BODY.test(mantissa)
    ? Option.none()
    : Option.flatMap(underscoreFree(whole), (left) =>
        Option.flatMap(underscoreFree(fraction ?? ''), (right) =>
          left.length === 0 && right.length === 0
            ? Option.none()
            : Option.some(`${left || '0'}.${right || '0'}`),
        ),
      );
};

const underscoreFree = (group: string): Option.Option<string> =>
  group.length === 0 ? Option.some('') : digitsWithUnderscores(group, DIGIT);

const exponentDigits = (exponent: string): Option.Option<string> => {
  const signed = exponent.startsWith('+') || exponent.startsWith('-');
  return Option.map(
    digitsWithUnderscores(signed ? exponent.slice(1) : exponent, DIGIT),
    (digits) => `${exponent.startsWith('-') ? '-' : ''}${digits}`,
  );
};

/**
 * `float(text)`.
 *
 * Accepts everything CPython accepts and nothing more: `1e3`, `.5`, `5.`,
 * `1_0.5`, `inf`/`infinity`/`nan` in any case, an optional sign, surrounding
 * whitespace and Unicode decimal digits.  Overflow saturates to infinity
 * exactly as CPython's `strtod` does (`float("1e400") == inf`), and the
 * caller — not this function — is what rejects a non-finite timeout.
 */
export const pythonFloat = (text: string): Either.Either<number, GatewayConfigError> => {
  const prepared = prepareNumeric(text);
  const negative = prepared.startsWith('-');
  const body = negative || prepared.startsWith('+') ? prepared.slice(1) : prepared;
  const magnitude = floatMagnitude(body);
  return Option.isSome(magnitude)
    ? Either.right(negative ? -magnitude.value : magnitude.value)
    : fail(`could not convert string to float: ${pythonRepr(text)}`);
};

// ---------------------------------------------------------------------------
// IP addresses (`ipaddress.ip_address`)
// ---------------------------------------------------------------------------

/** A parsed IP literal: the value `str()` renders, and whether it loops back. */
interface IpAddress {
  readonly compressed: string;
  readonly loopback: boolean;
}

const ASCII_DIGITS = /^[0-9]+$/;
const ASCII_HEX = /^[0-9A-Fa-f]{1,4}$/;

/** `IPv4Address._parse_octet`: ASCII digits, at most three, no leading zero. */
const parseOctet = (octet: string): Option.Option<number> =>
  ASCII_DIGITS.test(octet) &&
  octet.length <= 3 &&
  (octet.length === 1 || !octet.startsWith('0')) &&
  Number(octet) <= 255
    ? Option.some(Number(octet))
    : Option.none();

const parseIpv4Int = (text: string): Option.Option<number> => {
  const parts = text.split('.');
  return parts.length !== 4
    ? Option.none()
    : parts.reduce<Option.Option<number>>(
        (accumulated, octet) =>
          Option.flatMap(accumulated, (value) =>
            Option.map(parseOctet(octet), (byte) => value * 256 + byte),
          ),
        Option.some(0),
      );
};

const renderIpv4 = (value: number): string =>
  [24, 16, 8, 0].map((shift) => String((value >>> shift) & 0xff)).join('.');

const HEXTET_COUNT = 8;

/** `IPv6Address._split_scope_id`: at most one `%`, and a non-empty zone. */
const splitScopeId = (text: string): Option.Option<readonly [string, string | null]> => {
  const index = text.indexOf('%');
  return index < 0
    ? Option.some([text, null] as const)
    : ((zone) =>
        zone.length === 0 || zone.includes('%')
          ? Option.none()
          : Option.some([text.slice(0, index), zone] as const))(text.slice(index + 1));
};

const expandTrailingIpv4 = (parts: readonly string[]): Option.Option<readonly string[]> => {
  const last = parts[parts.length - 1];
  return last === undefined || !last.includes('.')
    ? Option.some(parts)
    : Option.map(parseIpv4Int(last), (value) => [
        ...parts.slice(0, -1),
        ((value >>> 16) & 0xffff).toString(16),
        (value & 0xffff).toString(16),
      ]);
};

const skipIndexOf = (parts: readonly string[]): Option.Option<number | null> =>
  parts
    .slice(1, -1)
    .reduce<Option.Option<number | null>>(
      (accumulated, part, offset) =>
        part.length > 0
          ? accumulated
          : Option.flatMap(accumulated, (found) =>
              found === null ? Option.some(offset + 1) : Option.none(),
            ),
      Option.some(null),
    );

const hextetValues = (parts: readonly string[]): Option.Option<readonly bigint[]> =>
  parts.reduce<Option.Option<readonly bigint[]>>(
    (accumulated, part) =>
      Option.flatMap(accumulated, (values) =>
        ASCII_HEX.test(part)
          ? Option.some([...values, BigInt(`0x${part}`)])
          : Option.none(),
      ),
    Option.some([]),
  );

const assemble = (
  high: readonly bigint[],
  low: readonly bigint[],
  skipped: number,
): bigint =>
  [...high, ...Array.from({ length: skipped }, () => 0n), ...low].reduce(
    (value, hextet) => (value << 16n) | hextet,
    0n,
  );

/** `_BaseV6._ip_int_from_string`, transcribed. */
const parseIpv6Int = (address: string): Option.Option<bigint> => {
  const split = splitScopeId(address);
  if (Option.isNone(split)) return Option.none();
  const [literal] = split.value;
  const raw = literal.split(':');
  if (raw.length < 3) return Option.none();
  const expanded = expandTrailingIpv4(raw);
  if (Option.isNone(expanded)) return Option.none();
  const parts = expanded.value;
  if (parts.length > HEXTET_COUNT + 1) return Option.none();
  const skip = skipIndexOf(parts);
  if (Option.isNone(skip)) return Option.none();
  const skipIndex = skip.value;
  if (skipIndex === null) {
    return parts.length !== HEXTET_COUNT ||
      parts[0]?.length === 0 ||
      parts[parts.length - 1]?.length === 0
      ? Option.none()
      : Option.map(hextetValues(parts), (values) => assemble(values, [], 0));
  }
  const leadingEmpty = parts[0]?.length === 0;
  const trailingEmpty = parts[parts.length - 1]?.length === 0;
  const high = skipIndex - (leadingEmpty ? 1 : 0);
  const low = parts.length - skipIndex - 1 - (trailingEmpty ? 1 : 0);
  if ((leadingEmpty && high !== 0) || (trailingEmpty && low !== 0)) return Option.none();
  const skipped = HEXTET_COUNT - (high + low);
  if (skipped < 1) return Option.none();
  return Option.flatMap(hextetValues(parts.slice(leadingEmpty ? 1 : 0, skipIndex)), (heads) =>
    Option.map(
      hextetValues(parts.slice(skipIndex + 1, trailingEmpty ? parts.length - 1 : parts.length)),
      (tails) => assemble(heads, tails, skipped),
    ),
  );
};

interface ZeroRun {
  readonly start: number;
  readonly length: number;
}

const longestZeroRun = (hextets: readonly string[]): ZeroRun =>
  hextets.reduce<{ readonly best: ZeroRun; readonly current: ZeroRun }>(
    (state, hextet, index) => {
      const current =
        hextet === '0'
          ? state.current.length === 0
            ? { start: index, length: 1 }
            : { start: state.current.start, length: state.current.length + 1 }
          : { start: -1, length: 0 };
      return {
        best: current.length > state.best.length ? current : state.best,
        current,
      };
    },
    { best: { start: -1, length: 0 }, current: { start: -1, length: 0 } },
  ).best;

/** `_BaseV6._compress_hextets` + `_string_from_ip_int`. */
const renderIpv6 = (value: bigint): string => {
  // CPython 3.14 renders an IPv4-mapped address in mixed notation, so
  // `::ffff:7f00:1` prints as `::ffff:127.0.0.1` (`IPv6Address.__str__`).
  if (value >> 32n === 0xffffn) return `::ffff:${renderIpv4(Number(value & 0xffffffffn))}`;
  const hextets = Array.from({ length: HEXTET_COUNT }, (_, index) =>
    ((value >> BigInt(16 * (HEXTET_COUNT - 1 - index))) & 0xffffn).toString(16),
  );
  const run = longestZeroRun(hextets);
  if (run.length <= 1) return hextets.join(':');
  const end = run.start + run.length;
  const collapsed = [
    ...hextets.slice(0, run.start),
    '',
    ...(end === hextets.length ? [''] : hextets.slice(end)),
  ];
  return (run.start === 0 ? ['', ...collapsed] : collapsed).join(':');
};

/** `ipaddress.ip_address(text)` — IPv4 first, then IPv6, as Python tries them. */
const parseIpAddress = (text: string): Option.Option<IpAddress> => {
  const v4 = parseIpv4Int(text);
  if (Option.isSome(v4)) {
    return Option.some({
      compressed: renderIpv4(v4.value),
      // `IPv4Address.is_loopback`: membership in 127.0.0.0/8.
      loopback: (v4.value >>> 24) === 127,
    });
  }
  const scope = splitScopeId(text);
  const v6 = parseIpv6Int(text);
  return Option.isNone(v6) || Option.isNone(scope)
    ? Option.none()
    : Option.some({
        compressed: `${renderIpv6(v6.value)}${scope.value[1] === null ? '' : `%${scope.value[1]}`}`,
        // `IPv6Address.is_loopback` delegates to the mapped IPv4 address when
        // there is one, so `::ffff:127.0.0.1` is loopback and `::` is not.
        loopback:
          v6.value >> 32n === 0xffffn
            ? Number(v6.value & 0xffffffffn) >>> 24 === 127
            : v6.value === 1n,
      });
};

/**
 * `_loopback_host` (`:164-172`).
 *
 * A hostname is *never* resolved: only a literal loopback address is accepted,
 * so the gateway cannot be published to a LAN by a typo.  The returned value
 * is `ipaddress`'s `compressed` form, which is what ends up in the `/health`
 * payload's `host` and in the `url` it renders (`:1302-1313`).
 */
export const loopbackHost = (value: string): Either.Either<string, GatewayConfigError> => {
  const address = parseIpAddress(value);
  return Option.isNone(address)
    ? fail(GATEWAY_CONFIG_MESSAGES.hostNotLiteral)
    : address.value.loopback
      ? Either.right(address.value.compressed)
      : fail(GATEWAY_CONFIG_MESSAGES.hostNotLoopback);
};

// ---------------------------------------------------------------------------
// `urllib.parse.urlsplit`
// ---------------------------------------------------------------------------

/** The five components `urlsplit` yields, before any interpretation. */
export interface SplitUrl {
  readonly scheme: string;
  readonly netloc: string;
  readonly path: string;
  readonly query: string;
  readonly fragment: string;
}

const SCHEME_CHARS = /^[A-Za-z0-9+.-]+$/;

const lstripC0Space = (text: string, index: number): string =>
  index < text.length && (text.codePointAt(index) ?? 0x21) <= 0x20
    ? lstripC0Space(text, index + 1)
    : text.slice(index);

const splitOnce = (text: string, separator: string): readonly [string, string | null] => {
  const index = text.indexOf(separator);
  return index < 0
    ? ([text, null] as const)
    : ([text.slice(0, index), text.slice(index + 1)] as const);
};

const netlocEnd = (text: string): number => {
  const candidates = ['/', '?', '#']
    .map((character) => text.indexOf(character, 2))
    .filter((index) => index >= 0);
  return candidates.length === 0 ? text.length : Math.min(...candidates);
};

/**
 * `_checknetloc`: a netloc that NFKC-normalizes into one of `/?#@:` is
 * rejected, because a later parse of the normalized form would see a
 * different authority (`U+2100` "℀" decomposes to `a/c`).
 */
const NETLOC_FORBIDDEN_AFTER_NFKC: readonly string[] = ['/', '?', '#', '@', ':'];

const netlocIsSafe = (netloc: string): boolean => {
  if (netloc.length === 0 || isAscii(netloc)) return true;
  const stripped = netloc.replace(/[@:#?]/g, '');
  const normalized = stripped.normalize('NFKC');
  return (
    stripped === normalized ||
    !NETLOC_FORBIDDEN_AFTER_NFKC.some((character) => normalized.includes(character))
  );
};

/**
 * `_check_bracketed_host`: the text between `[` and `]` must be an IPv6
 * literal, and specifically not an IPv4 one.
 */
const bracketedHostIsSafe = (netloc: string): boolean => {
  const open = netloc.indexOf('[');
  const close = netloc.indexOf(']');
  if (open < 0 || close < 0) return open < 0 && close < 0;
  const host = netloc.slice(open + 1, close);
  const withoutScope = splitOnce(host, '%')[0];
  return Option.isNone(parseIpv4Int(withoutScope)) && Option.isSome(parseIpv6Int(host));
};

/**
 * `urllib.parse.urlsplit`, transcribed from CPython 3.14.
 *
 * `Option.none()` stands for every `ValueError` the function can raise —
 * `Invalid IPv6 URL`, the NFKC netloc check, the bracketed-host check — because
 * `_normalize_service_url` wraps the call *and* the `.port` access in one
 * `except ValueError` and reports all of them as
 * {@link GATEWAY_CONFIG_MESSAGES.serviceUrlInvalidPort} (`:131-135`).
 */
export const urlsplit = (input: string): Option.Option<SplitUrl> => {
  const cleaned = lstripC0Space(input, 0).replace(/[\t\r\n]/g, '');
  const colon = cleaned.indexOf(':');
  const schemeCandidate = cleaned.slice(0, colon);
  const hasScheme =
    colon > 0 && /^[A-Za-z]/.test(cleaned) && SCHEME_CHARS.test(schemeCandidate);
  const scheme = hasScheme ? schemeCandidate.toLowerCase() : '';
  const afterScheme = hasScheme ? cleaned.slice(colon + 1) : cleaned;
  const hasNetloc = afterScheme.startsWith('//');
  const end = hasNetloc ? netlocEnd(afterScheme) : 2;
  const netloc = hasNetloc ? afterScheme.slice(2, end) : '';
  const rest = hasNetloc ? afterScheme.slice(end) : afterScheme;
  if (hasNetloc && (netloc.includes('[') !== netloc.includes(']') || !bracketedHostIsSafe(netloc))) {
    return Option.none();
  }
  if (!netlocIsSafe(netloc)) return Option.none();
  const [beforeFragment, fragment] = splitOnce(rest, '#');
  const [path, query] = splitOnce(beforeFragment, '?');
  return Option.some({
    scheme,
    netloc,
    path,
    query: query ?? '',
    fragment: fragment ?? '',
  });
};

const rpartition = (text: string, separator: string): readonly [string, boolean, string] => {
  const index = text.lastIndexOf(separator);
  return index < 0
    ? (['', false, text] as const)
    : ([text.slice(0, index), true, text.slice(index + separator.length)] as const);
};

/** `SplitResult._hostinfo` — the host and the raw port text. */
const hostInfo = (netloc: string): readonly [string, string | null] => {
  const [, , hostinfo] = rpartition(netloc, '@');
  const open = hostinfo.indexOf('[');
  if (open < 0) {
    const [host, port] = splitOnce(hostinfo, ':');
    return [host, port === null || port.length === 0 ? null : port] as const;
  }
  const bracketed = hostinfo.slice(open + 1);
  const [host, afterHost] = splitOnce(bracketed, ']');
  const [, port] = splitOnce(afterHost ?? '', ':');
  return [host, port === null || port.length === 0 ? null : port] as const;
};

/**
 * `SplitResult.hostname` — lowercased, with any `%zone` suffix left as typed.
 *
 * The zone exemption is `urllib`'s, and `_normalize_service_url` immediately
 * undoes it with a second `.lower()` (`:148`); both are transcribed so that
 * this function stays usable as the `hostname` property it names.
 */
const hostnameOf = (netloc: string): string | null => {
  const [host] = hostInfo(netloc);
  if (host.length === 0) return null;
  const index = host.indexOf('%');
  return index < 0 ? host.toLowerCase() : `${host.slice(0, index).toLowerCase()}${host.slice(index)}`;
};

/** `SplitResult.port` — `Option.none()` is the `ValueError` both arms raise. */
const portOf = (netloc: string): Option.Option<number | null> => {
  const [, port] = hostInfo(netloc);
  if (port === null) return Option.some(null);
  if (!ASCII_DIGITS.test(port)) return Option.none();
  const value = Number(port);
  return value >= 0 && value <= 65535 ? Option.some(value) : Option.none();
};

/** `SplitResult.username` / `.password` — both `null` without an `@`. */
const userInfoPresent = (netloc: string): boolean => {
  const [userinfo, found] = rpartition(netloc, '@');
  if (!found) return false;
  const [username, password] = splitOnce(userinfo, ':');
  // Python tests truthiness, so `http://@host` (an empty username and no
  // password) is *accepted* (`:138`).
  return username.length > 0 || (password !== null && password.length > 0);
};

/**
 * `_normalize_service_url` (`:129-161`) — one credential-free HTTP(S) origin
 * plus an optional path prefix.
 *
 * The result is concatenated onto every proxied path, so the trailing-slash
 * strip and the default-port elision are not cosmetic: they decide whether the
 * upstream sees `/v1/games` or `//v1/games`.
 */
export const normalizeServiceUrl = (value: string): Either.Either<string, GatewayConfigError> => {
  const parsed = urlsplit(pythonStrip(value));
  if (Option.isNone(parsed)) return fail(GATEWAY_CONFIG_MESSAGES.serviceUrlInvalidPort);
  const port = portOf(parsed.value.netloc);
  if (Option.isNone(port)) return fail(GATEWAY_CONFIG_MESSAGES.serviceUrlInvalidPort);
  const { scheme, netloc, path, query, fragment } = parsed.value;
  const hostname = hostnameOf(netloc);
  if ((scheme !== 'http' && scheme !== 'https') || hostname === null) {
    return fail(GATEWAY_CONFIG_MESSAGES.serviceUrlNotHttp);
  }
  if (userInfoPresent(netloc) || query.length > 0 || fragment.length > 0) {
    return fail(GATEWAY_CONFIG_MESSAGES.serviceUrlCredentials);
  }
  if ([...path].some((character) => (character.codePointAt(0) ?? 0) < 0x20)) {
    return fail(GATEWAY_CONFIG_MESSAGES.serviceUrlControlCharacters);
  }
  if (path.split('/').some((segment) => segment === '.' || segment === '..')) {
    return fail(GATEWAY_CONFIG_MESSAGES.serviceUrlDotSegments);
  }
  // `:148` lowercases the whole hostname again, which — unlike the `hostname`
  // property — also folds a `%2F` escape and an IPv6 zone id.
  const folded = hostname.toLowerCase();
  const host = folded.includes(':') ? `[${folded}]` : folded;
  const isDefaultPort =
    (scheme === 'http' && port.value === 80) || (scheme === 'https' && port.value === 443);
  const rendered = port.value === null || isDefaultPort ? host : `${host}:${String(port.value)}`;
  return Either.right(`${scheme}://${rendered}${path.replace(/\/+$/, '')}`);
};

// ---------------------------------------------------------------------------
// `Path(...).expanduser().resolve()`
// ---------------------------------------------------------------------------

/** The ambient inputs `expanduser()` and `resolve()` read. */
export interface PathEnvironment {
  /** `os.getcwd()` — already symlink-free, so relative paths need no walk. */
  readonly cwd: string;
  /** `os.path.expanduser("~")`. */
  readonly home: Option.Option<string>;
}

/**
 * Read {@link PathEnvironment} from the process.
 *
 * `$HOME` is read directly rather than through `Config` on purpose: this is
 * not configuration the operator supplies to the harness, it is the same
 * lookup `os.path.expanduser` performs, and routing it through a `Config`
 * would let a `HOME` set in an Effect layer disagree with the one CPython
 * would have seen.  `homedir()` is the `pwd` fallback `expanduser` uses when
 * the variable is unset.
 */
export const pathEnvironment: Effect.Effect<PathEnvironment> = Effect.sync(() => ({
  cwd: process.cwd(),
  // `expanduser`'s own lookup, not harness config: see the docstring above.
  // The directive below needs its plugin prefix *and* has to sit immediately
  // before the offending line; the previous spelling had neither, so the
  // warning stayed in `bun run lint`'s output while reading like a settled
  // decision.
  // oxlint-disable-next-line effecttsgo/process-env-in-effect
  home: Option.fromNullable(process.env['HOME'] ?? homedir() ?? null).pipe(
    Option.filter((value) => value.length > 0),
  ),
}));

/**
 * `Path.expanduser()`.
 *
 * **Known divergence**: `~user` is rejected rather than looked up in the
 * password database, which Bun exposes no API for.  Python raises
 * `RuntimeError` for an *unknown* user and expands a known one; the gateway is
 * always invoked with absolute paths by `local_stack.py:540-548`, so the gap is
 * unreachable there.
 */
export const expandUser = (
  input: string,
  home: Option.Option<string>,
): Either.Either<string, GatewayConfigError> => {
  if (!input.startsWith('~')) return Either.right(input);
  const [head, ...tail] = input.split('/');
  return head === '~' && Option.isSome(home)
    ? Either.right([home.value, ...tail].join('/'))
    : fail(GATEWAY_CONFIG_MESSAGES.noHomeDirectory);
};

const joinPosix = (base: string, name: string): string =>
  base === '/' ? `/${name}` : `${base}/${name}`;

const dirnamePosix = (path: string): string => {
  const index = path.lastIndexOf('/');
  return index <= 0 ? '/' : path.slice(0, index);
};

const MAX_SYMLINKS = 40;

const walk = (
  resolved: string,
  pending: readonly string[],
  hops: number,
): Effect.Effect<string, never, FileSystem.FileSystem> =>
  Effect.gen(function* () {
    const [name, ...rest] = pending;
    if (name === undefined) return resolved;
    if (name === '' || name === '.') return yield* walk(resolved, rest, hops);
    if (name === '..') return yield* walk(dirnamePosix(resolved), rest, hops);
    const candidate = joinPosix(resolved, name);
    const fs = yield* FileSystem.FileSystem;
    // `readLink` fails with EINVAL on a regular file and ENOENT on a missing
    // one; `realpath(strict=False)` treats both as "stop resolving here".
    const target = yield* fs.readLink(candidate).pipe(Effect.option);
    return Option.isNone(target) || hops >= MAX_SYMLINKS
      ? yield* walk(candidate, rest, hops)
      : yield* walk(
          target.value.startsWith('/') ? '/' : resolved,
          [...target.value.split('/'), ...rest],
          hops + 1,
        );
  });

/**
 * `Path(input).expanduser().resolve()` — `posixpath.realpath(strict=False)`
 * followed by `pathlib`'s normalization.
 *
 * Symlinks are resolved component by component and `..` is applied *after*
 * that resolution, which is why `link/..` lands in the link's target's parent
 * and not in the link's own. Missing components are kept verbatim (the cache
 * root usually does not exist yet — `:2092` creates it later), and a symlink
 * cycle stops resolving rather than failing, both matching `strict=False`.
 *
 * POSIX only: the gateway binds a loopback socket and reads a run archive on
 * the developer's machine, and `replay_gateway.py` has no Windows path either.
 */
export const resolvePath = (
  input: string,
  environment: PathEnvironment,
): Effect.Effect<string, GatewayConfigError, FileSystem.FileSystem> =>
  Either.match(expandUser(input, environment.home), {
    onLeft: (error) => Effect.fail(error),
    onRight: (expanded) =>
      walk(
        '/',
        (expanded.startsWith('/') ? expanded : `${environment.cwd}/${expanded}`).split('/'),
        0,
      ),
  });

// ---------------------------------------------------------------------------
// Identity
// ---------------------------------------------------------------------------

/** The five strings `_identity` hashes, in order (`:175-183`). */
export interface GatewayIdentityMaterial {
  readonly repoRoot: string;
  readonly upstreamServiceUrl: string;
  readonly runsRoot: string;
  readonly cacheRoot: string;
  readonly viewerPublicUrl: Option.Option<string>;
}

/**
 * `_identity` (`:175-183`): the first 20 lowercase hex characters of
 * `sha256("\0".join(repo_root, upstream, runs, cache, viewer or ""))`.
 *
 * `local_stack.py` derives the state-file name `replay-gateway-{identity}.json`
 * from this, so the separator, the field order and the empty-string stand-in
 * for an absent viewer URL are all load-bearing: a port that joined with `":"`
 * would pass every shape assertion and silently orphan the state file.
 * `@arena/wire`'s `Gateway.gatewayIdentityMaterial` builds the same string
 * from a decoded `/health` payload; this one builds it from the config, and
 * the test asserts they agree.
 */
export const gatewayIdentity = (material: GatewayIdentityMaterial): string =>
  createHash('sha256')
    .update(
      [
        material.repoRoot,
        material.upstreamServiceUrl,
        material.runsRoot,
        material.cacheRoot,
        Option.getOrElse(material.viewerPublicUrl, () => ''),
      ].join('\0'),
      'utf8',
    )
    .digest('hex')
    .slice(0, 20);

// ---------------------------------------------------------------------------
// The service
// ---------------------------------------------------------------------------

/**
 * `--backend postgres` and the URL that goes with it.
 *
 * There is no `Fs` twin on purpose: the filesystem backend is spelled as the
 * **absence** of this value, so an argv that names neither `--backend` nor
 * `--database-url` — which is every argv `local_stack.py` and the parity rig
 * have ever spawned — produces exactly the record it produced before the two
 * flags existed.  `undefined` is the default, and the default is the port.
 *
 * The URL is {@link Redacted.Redacted} from the moment it leaves the parser and
 * is never unwrapped in this module.  Unlike `--service-url` it may carry a
 * password, so it must not reach `/health`, the ready record, a log line or
 * `describeStartupError` — and it is deliberately **not** part of
 * {@link gatewayIdentity}'s material either: `local_stack.py` derives
 * `replay-gateway-{identity}.json` from that digest, so hashing the URL in
 * would both orphan existing state files and hash a password into a filename.
 */
export interface PostgresBackend {
  readonly _tag: 'Postgres';
  readonly databaseUrl: Redacted.Redacted<string>;
  /**
   * `--materialize-root`, resolved, with `<cacheRoot>-saves` filled in.
   *
   * Always present once the configuration is validated: the python bridge
   * needs real files, so a Postgres-backed gateway always has somewhere to put
   * them, whether or not an operator named it.
   */
  readonly materializeRoot: string;
}

/**
 * {@link PostgresBackend} as the parser produces it, before resolution.
 *
 * `materializeRoot` is the flag's text or `undefined`; {@link makeGatewayConfig}
 * is what turns it into a resolved path and what refuses one that nests with
 * the cache root.
 */
export interface PostgresBackendInput {
  readonly _tag: 'Postgres';
  readonly databaseUrl: Redacted.Redacted<string>;
  readonly materializeRoot: string | undefined;
}

/**
 * Does one path contain, equal or sit inside the other?
 *
 * `save_replay._cache_directory` raises `SaveReplayError("Replay cache must be
 * separate from game saves.")` for exactly this relation between the resolved
 * cache root and the saves directory — checked twice, before and after
 * `_safe_directory` — so a `--materialize-root` that nests with `--cache-root`
 * would surface as a 503 on the first replay request instead of as a refusal to
 * start.  `@arena/db`'s `Materialize.nestsWith` is the same predicate for
 * callers on the other side of the package boundary; `test/gateway/cli.test.ts`
 * asserts the two agree rather than trusting that they do.  It is duplicated
 * here, three lines of it, because importing `@arena/db` from this module would
 * put a database driver in the *filesystem* gateway's module graph.
 */
export const pathsNest = (left: string, right: string): boolean => {
  const a = left.replace(/\/+$/, '');
  const b = right.replace(/\/+$/, '');
  return a === b || a.startsWith(`${b}/`) || b.startsWith(`${a}/`);
};

/** `<cacheRoot>-saves` — the sibling `--materialize-root` defaults to. */
export const defaultMaterializeRoot = (cacheRoot: string): string => `${cacheRoot}-saves`;

/**
 * Everything the gateway needs before it binds, resolved and validated.
 *
 * The first eight fields are the Python `GatewayConfig` dataclass (`:114-122`)
 * plus its derived `identity`.  The last three are the arguments
 * `make_replay_gateway_server` and `run_replay_gateway` take alongside it
 * (`:2079-2081`, `:2125`); they live here because the port has one
 * construction step and one place where a bad invocation is reported, and
 * splitting them across two services would give it two.
 */
export interface GatewayConfigValues {
  /** `--repo-root`, resolved (`:205`). */
  readonly repoRoot: string;
  /** `--service-url`, normalized (`:208`). */
  readonly upstreamServiceUrl: string;
  /** `--runs-root`, resolved (`:206`). */
  readonly runsRoot: string;
  /** `--cache-root`, resolved (`:207`); created before the socket binds (`:2092`). */
  readonly cacheRoot: string;
  /** 20 lowercase hex characters — see {@link gatewayIdentity}. */
  readonly identity: string;
  /** `--upstream-timeout-s`, finite and positive (`:194-200`). */
  readonly upstreamTimeoutSeconds: number;
  /** `--viewer-public-url`, normalized; absent means relative watch URLs (`:1881-1884`). */
  readonly viewerPublicUrl: Option.Option<string>;
  /** `--host`, the compressed loopback literal the socket binds (`:2081`). */
  readonly host: string;
  /** `--port`; `0` asks the kernel, and `/health` reports what it got (`:1312`). */
  readonly port: number;
  /** `--ready-file`, resolved; the 0600 record written after the socket serves (`:2146`). */
  readonly readyFile: string;
  /**
   * `--backend postgres`'s target, absent for the filesystem backend.
   *
   * TypeScript-only and additive: `_parser()` has no such flag, and `main.ts`
   * reads this field — and nothing else — to choose which `RunsRepository` and
   * which derivation layer the stack gets.  See {@link PostgresBackend}.
   */
  readonly backend?: PostgresBackend | undefined;
}

/** The gateway's configuration, as a service. */
export class GatewayConfig extends Context.Tag('@arena/harness/GatewayConfig')<
  GatewayConfig,
  GatewayConfigValues
>() {}

/** The raw, still-untrusted values `_parser()` produces. */
export interface GatewayConfigInput {
  readonly host: string;
  readonly port: bigint;
  readonly serviceUrl: string;
  readonly runsRoot: string;
  readonly cacheRoot: string;
  readonly repoRoot: string;
  readonly readyFile: string;
  readonly upstreamTimeoutSeconds: number;
  readonly viewerPublicUrl: Option.Option<string>;
  /** The TS-only backend selection, unresolved; absent is the filesystem. */
  readonly backend?: PostgresBackendInput | undefined;
}

const checkPort = (port: bigint): Either.Either<number, GatewayConfigError> =>
  port >= 0n && port <= 65535n
    ? Either.right(Number(port))
    : fail(GATEWAY_CONFIG_MESSAGES.portOutOfRange);

/**
 * Resolve the backend's paths, or refuse the pair.
 *
 * Runs **last** in {@link makeGatewayConfig}, after every Python check, so that
 * a gateway invoked exactly as `local_stack.py` invokes one cannot reach a new
 * failure — and so that an argv that is wrong in two ways still reports the
 * error the Python would have reported first.
 */
const resolveBackend = (
  backend: PostgresBackendInput | undefined,
  cacheRoot: string,
  environment: PathEnvironment,
): Effect.Effect<PostgresBackend | undefined, GatewayConfigError, FileSystem.FileSystem> =>
  backend === undefined
    ? Effect.succeed(undefined)
    : Effect.gen(function* () {
        const materializeRoot = yield* resolvePath(
          backend.materializeRoot ?? defaultMaterializeRoot(cacheRoot),
          environment,
        );
        return yield* pathsNest(materializeRoot, cacheRoot)
          ? fail(GATEWAY_CONFIG_MESSAGES.materializeRootNestsCacheRoot)
          : Either.right({
              _tag: 'Postgres' as const,
              databaseUrl: backend.databaseUrl,
              materializeRoot,
            });
      });

const checkTimeout = (seconds: number): Either.Either<number, GatewayConfigError> =>
  Number.isFinite(seconds) && seconds > 0
    ? Either.right(seconds)
    : fail(GATEWAY_CONFIG_MESSAGES.timeoutNotPositive);

/**
 * Build a {@link GatewayConfigValues} the way
 * `make_replay_gateway_server` → `gateway_config` does, reporting the *first*
 * failure in the same order they do: host, port, timeout, then the paths and
 * the two URLs.
 *
 * `backend` is carried through untouched and validated **nowhere here**: the
 * flag pair's own consistency (`--database-url` present iff
 * `--backend postgres`) is a parse-time `ValidationError` in `cli.ts`, and the
 * URL's shape is `@arena/db`'s to judge, at the one place that opens a
 * connection.  Adding a check here would put a *new* failure into the order
 * above, which is the one thing this function's contract forbids.
 */
export const makeGatewayConfig = (
  input: GatewayConfigInput,
): Effect.Effect<GatewayConfigValues, GatewayConfigError, FileSystem.FileSystem> =>
  Effect.gen(function* () {
    const host = yield* loopbackHost(input.host);
    const port = yield* checkPort(input.port);
    const upstreamTimeoutSeconds = yield* checkTimeout(input.upstreamTimeoutSeconds);
    const environment = yield* pathEnvironment;
    const repoRoot = yield* resolvePath(input.repoRoot, environment);
    const runsRoot = yield* resolvePath(input.runsRoot, environment);
    const cacheRoot = yield* resolvePath(input.cacheRoot, environment);
    const readyFile = yield* resolvePath(input.readyFile, environment);
    const upstreamServiceUrl = yield* normalizeServiceUrl(input.serviceUrl);
    const viewerPublicUrl = yield* Option.match(input.viewerPublicUrl, {
      onNone: () => Effect.succeedNone,
      onSome: (value) => Effect.asSome(normalizeServiceUrl(value)),
    });
    const backend = yield* resolveBackend(input.backend, cacheRoot, environment);
    return {
      repoRoot,
      upstreamServiceUrl,
      runsRoot,
      cacheRoot,
      identity: gatewayIdentity({
        repoRoot,
        upstreamServiceUrl,
        runsRoot,
        cacheRoot,
        viewerPublicUrl,
      }),
      upstreamTimeoutSeconds,
      viewerPublicUrl,
      host,
      port,
      readyFile,
      backend,
    };
  });

/** A `Layer` carrying one validated configuration. */
export const gatewayConfigLayer = (
  input: GatewayConfigInput,
): Layer.Layer<GatewayConfig, GatewayConfigError, FileSystem.FileSystem> =>
  Layer.effect(GatewayConfig, makeGatewayConfig(input));
