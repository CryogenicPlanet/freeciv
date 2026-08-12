/**
 * The raw-socket parity client — one request, one connection, framed by the
 * response's own headers.
 *
 * `fetch` cannot be the parity rig's client, and not for style reasons.  Four
 * things the rig has to send are unrepresentable in it:
 *
 * 1. **Request targets it normalizes.**  `//v1/games` collapses to `/v1/games`,
 *    `…/status/` keeps its slash but `…%2Fstatus` does not survive, and the
 *    whole point of those legs is what the *server* does with the bytes.
 * 2. **`GET` with a body.**  `fetch` refuses `body` on `GET`/`HEAD` outright,
 *    and that leg is how the two gateways' 400 is compared.
 * 3. **Invented verbs.**  `TRACE`, `FROB` — `fetch` forbids some and Bun's
 *    client rewrites others.
 * 4. **Deliberately malformed framing.**  `Content-Length: abc`,
 *    `Transfer-Encoding: identity`, a space inside the request target.  These
 *    are answered by the *parser*, before either gateway's router runs, and
 *    they are the only legitimate members of the divergence waiver list.
 *
 * ## The framing law this module exists to obey
 *
 * **Complete on `Content-Length`.  Never wait for EOF when the response says
 * how long it is.**
 *
 * This was measured, not guessed.  A client that reads until the socket ends —
 * which is what the first version of the gateway smoke rig did — pays a
 * *phantom 12 second* stall per request against the Bun gateway: the response
 * is complete and correct, and the connection lingers until an idle timer
 * closes it.  Multiply by ~50 legs and two phases and the rig stops being
 * runnable.  So {@link wireRequest} decides a completion rule the moment the
 * header block lands and stops reading the instant that rule is satisfied:
 *
 * | Condition | `completedBy` | Rule |
 * |---|---|---|
 * | request method was `HEAD` | `head-request` | zero body bytes, always |
 * | status 1xx, 204, 304 | `status-no-body` | zero body bytes, always |
 * | `Transfer-Encoding` mentions `chunked` | `chunked` | terminal zero-size chunk |
 * | a single, wholly-numeric `Content-Length` | `content-length` | exactly that many bytes |
 * | none of the above | `until-close` | read to EOF — the only legal wait |
 *
 * `until-close` is not a fallback to be ashamed of: an HTTP/1.0 response with
 * no `Content-Length` genuinely *is* close-delimited, and that is CPython's
 * `send_error` path.  What the law forbids is waiting for EOF when the server
 * already answered the question.
 *
 * A duplicated `Content-Length` — which the Bun adapter joins into `5, 5`, one
 * of the accepted divergences — is deliberately *not* numeric, so it degrades
 * to `until-close` rather than guessing which copy to believe.
 *
 * ## Outcomes are values
 *
 * A closed connection is a *result* here, not an error: "invented verb → the
 * server closes without answering" is one of the measured divergences, so the
 * rig has to be able to compare it between the two implementations.  Every
 * terminal state is therefore a tagged member of {@link WireOutcome}, and
 * nothing in this module throws.
 *
 * @module
 */

import { connect } from 'node:net';

// ---------------------------------------------------------------------------
// Headers
// ---------------------------------------------------------------------------

/**
 * A response's header block, indexed case-insensitively and *without* losing
 * anything.
 *
 * Header names are case-insensitive in HTTP and the two implementations
 * disagree on case in practice — CPython emits `Allow`, Bun emits `allow` — so
 * a comparison keyed on the verbatim name would report a difference that is not
 * one.  Duplicates are kept: `all('set-cookie')` is the honest answer, and
 * {@link WireHeaders.get} joins with `, ` the way a field-list is defined to
 * combine, which is exactly what makes a duplicated `Content-Length` read back
 * as the non-numeric `5, 5` the framing rule then refuses to trust.
 */
export interface WireHeaders {
  /** Every value for `name`, joined with `, `; `null` when the field is absent. */
  readonly get: (name: string) => string | null;
  /** Every value for `name`, separately, in arrival order. */
  readonly all: (name: string) => ReadonlyArray<string>;
  readonly has: (name: string) => boolean;
  /** Lower-cased field names, first-appearance order, no duplicates. */
  readonly names: () => ReadonlyArray<string>;
  /** The block verbatim: original case, original order, duplicates intact. */
  readonly raw: ReadonlyArray<readonly [string, string]>;
  /** Lower-cased name → joined value, for structural comparison in a test. */
  readonly toObject: () => Readonly<Record<string, string>>;
}

/** Build a {@link WireHeaders} over a verbatim, ordered field list. */
export const makeHeaders = (raw: ReadonlyArray<readonly [string, string]>): WireHeaders => {
  const index = raw.reduce<ReadonlyMap<string, ReadonlyArray<string>>>((map, [name, value]) => {
    const key = name.toLowerCase();
    return new Map(map).set(key, [...(map.get(key) ?? []), value]);
  }, new Map());
  const all = (name: string): ReadonlyArray<string> => index.get(name.toLowerCase()) ?? [];
  return {
    all,
    get: (name) => {
      const values = all(name);
      return values.length === 0 ? null : values.join(', ');
    },
    has: (name) => index.has(name.toLowerCase()),
    names: () => Array.from(index.keys()),
    raw,
    toObject: () =>
      Object.fromEntries(Array.from(index, ([name, values]) => [name, values.join(', ')])),
  };
};

// ---------------------------------------------------------------------------
// Outcomes
// ---------------------------------------------------------------------------

/** Which rule ended the read.  See the module doc's framing table. */
export type WireFraming =
  | 'head-request'
  | 'status-no-body'
  | 'chunked'
  | 'content-length'
  | 'until-close';

/** A complete response, as bytes plus the three things parity compares. */
export interface WireResponse {
  readonly _tag: 'Response';
  /** `200`, `405`, `501` — the three-digit code, as a number. */
  readonly status: number;
  /**
   * The reason phrase, verbatim and possibly empty.
   *
   * Named for the brief; it is the text *after* the status code, not the whole
   * line — {@link WireResponse.statusLine} is that.  It is carried because
   * `statusText` being discarded (Bun answers `501` with no reason phrase where
   * CPython writes `Unsupported method ('FROB')`) is on the waiver list, and a
   * waiver you cannot observe is a waiver you cannot police.
   */
  readonly reasonLine: string;
  /** The status line verbatim, e.g. `HTTP/1.0 405 Method Not Allowed`. */
  readonly statusLine: string;
  /** `HTTP/1.0` or `HTTP/1.1` — CPython is 1.0, Bun is 1.1. */
  readonly httpVersion: string;
  readonly headers: WireHeaders;
  /** The body, de-chunked when it arrived chunked.  Never `null`. */
  readonly bodyBytes: Uint8Array;
  readonly completedBy: WireFraming;
  /** Total bytes read off the socket, header block included. */
  readonly wireBytes: number;
}

/** Every way a request can end without a complete response, as a value. */
export type WireFailure =
  /** The origin was unusable, or the socket never connected. */
  | { readonly _tag: 'ConnectFailed'; readonly origin: string; readonly message: string }
  /**
   * The peer closed before any byte arrived.
   *
   * This is a *comparison subject*, not a rig error: an invented verb makes
   * both gateways close the connection, and the rig asserts they agree.
   */
  | { readonly _tag: 'ClosedWithoutResponse'; readonly message: string }
  /** A header block arrived but the declared body did not finish. */
  | {
      readonly _tag: 'Truncated';
      readonly status: number;
      readonly expectedBodyBytes: number;
      readonly receivedBodyBytes: number;
    }
  /** No status line, or one that is not `HTTP/x.y NNN …`. */
  | { readonly _tag: 'Malformed'; readonly reason: string; readonly preview: string }
  /** Nothing completed the read within the deadline. */
  | { readonly _tag: 'Timeout'; readonly afterMs: number; readonly bytesReceived: number };

export type WireOutcome = WireResponse | WireFailure;

/** Narrow a {@link WireOutcome} to the success arm. */
export const isWireResponse = (outcome: WireOutcome): outcome is WireResponse =>
  outcome._tag === 'Response';

/** The body as UTF-8 text.  For JSON bodies, which is most of them. */
export const bodyUtf8 = (response: WireResponse): string =>
  Buffer.from(response.bodyBytes).toString('utf8');

/**
 * The body as latin-1 — one character per byte, lossless, comparable.
 *
 * The right spelling for a *byte* comparison of two bodies that may not be
 * valid UTF-8 (a PNG frame, a truncated `replay.jsonl` tail): UTF-8 decoding
 * maps every invalid sequence onto the same replacement character, which would
 * make two different byte strings compare equal.
 */
export const bodyLatin1 = (response: WireResponse): string =>
  Buffer.from(response.bodyBytes).toString('latin1');

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

/**
 * One request, spelled at the byte level.
 *
 * Nothing here is normalized, re-encoded or reordered on the way out: the
 * request line is `{method} {target} {httpVersion}` with those three strings
 * verbatim, and the headers are emitted in the order given after the defaults.
 */
export interface WireRequest {
  /** Any token, including verbs `fetch` refuses to send. */
  readonly method: string;
  /** The literal request-target.  `//v1/games`, `/x%2Fy`, `http://host/p`. */
  readonly target: string;
  /** Default `HTTP/1.1`. */
  readonly httpVersion?: string;
  /** Sent after the defaults, verbatim, duplicates allowed. */
  readonly headers?: ReadonlyArray<readonly [string, string]>;
  /** Sent for *any* method, `GET` included. */
  readonly body?: Uint8Array | string;
  /**
   * Add `Content-Length` for {@link WireRequest.body} (default `true`).
   *
   * Suppressed automatically when the caller already supplied a
   * `Content-Length` or a `Transfer-Encoding`, so the malformed-framing legs
   * keep the exact header they mean to send.  Set `false` to send a body with
   * no framing at all.
   */
  readonly autoContentLength?: boolean;
  /** Add `Host` (default `true`). */
  readonly autoHost?: boolean;
  /**
   * Add `Connection: close` (default `true`).
   *
   * It is a courtesy, not the framing: this client stops reading when the
   * response says it is complete, whatever the server intends to do next.
   */
  readonly autoConnectionClose?: boolean;
  /** Replace the whole request line.  For a target containing a space. */
  readonly rawRequestLine?: string;
  /**
   * Replace *everything* up to and including the blank line, verbatim.
   *
   * The escape hatch for a request no structured builder can express — a bare
   * LF terminator, a header with no colon, an absent request line.  When set,
   * every other request field except {@link WireRequest.body} is ignored.
   */
  readonly rawHead?: string;
  /** Overrides {@link DEFAULT_WIRE_TIMEOUT_MS}. */
  readonly timeoutMs?: number;
}

/**
 * Generous, because a cold `replay.json` shells out to
 * `python3 -m agent_eval.replay_derive_cli` and parses savegames.
 *
 * It is a *deadlock* detector, not a performance budget — the framing law is
 * what keeps a healthy request from ever approaching it.
 */
export const DEFAULT_WIRE_TIMEOUT_MS = 60_000;

/**
 * The short deadline for a request that is *expected* to hang.
 *
 * Pair it with an RFC 5737 unroutable upstream: a connect to `192.0.2.1` never
 * refuses and never answers, which is the only honest way to exercise the
 * gateway's upstream-timeout branch.  See `boot.ts`'s down-fixture constants
 * for why a released ephemeral port must never be used instead.
 */
export const UNREACHABLE_WIRE_TIMEOUT_MS = 1_000;

const CRLF = '\r\n';
const HEAD_TERMINATOR = Buffer.from('\r\n\r\n');
const STATUS_LINE = /^(HTTP\/[0-9]\.[0-9]) (\d{3})(?: (.*))?$/;

/** Statuses defined to carry no body, whatever the headers claim. */
const bodilessStatus = (status: number): boolean =>
  (status >= 100 && status < 200) || status === 204 || status === 304;

interface WireEndpoint {
  readonly hostname: string;
  /** `host:port`, the `Host` header's value. */
  readonly authority: string;
  readonly port: number;
}

/**
 * `http://127.0.0.1:54321` → an endpoint, or `null` for anything this client
 * cannot honestly speak.
 *
 * `https` is refused rather than silently attempted: there is no TLS on a raw
 * socket, and a gateway that answered on one would be a different program.
 */
export const parseOrigin = (origin: string): WireEndpoint | null => {
  if (!URL.canParse(origin)) return null;
  const url = new URL(origin);
  if (url.protocol !== 'http:') return null;
  const port = url.port === '' ? 80 : Number(url.port);
  return Number.isInteger(port) && port > 0 && port <= 65535
    ? { hostname: url.hostname, authority: url.port === '' ? url.hostname : url.host, port }
    : null;
};

const bodyBuffer = (body: Uint8Array | string | undefined): Buffer | null =>
  body === undefined ? null : Buffer.from(typeof body === 'string' ? Buffer.from(body, 'utf8') : body);

/** The bytes before the body: request line, defaults, caller headers, blank line. */
const renderHead = (
  request: WireRequest,
  endpoint: WireEndpoint,
  body: Buffer | null,
): string => {
  if (request.rawHead !== undefined) return request.rawHead;
  const supplied = request.headers ?? [];
  const named = (name: string): boolean =>
    supplied.some(([candidate]) => candidate.toLowerCase() === name);
  const defaults: ReadonlyArray<readonly [string, string]> = [
    ...(request.autoHost === false || named('host')
      ? []
      : ([['Host', endpoint.authority]] as const)),
    ...(request.autoConnectionClose === false || named('connection')
      ? []
      : ([['Connection', 'close']] as const)),
    ...(body === null ||
    request.autoContentLength === false ||
    named('content-length') ||
    named('transfer-encoding')
      ? []
      : ([['Content-Length', String(body.byteLength)]] as const)),
  ];
  const requestLine =
    request.rawRequestLine ??
    `${request.method} ${request.target} ${request.httpVersion ?? 'HTTP/1.1'}`;
  return [
    requestLine,
    ...[...defaults, ...supplied].map(([name, value]) => `${name}: ${value}`),
    '',
    '',
  ].join(CRLF);
};

// ---------------------------------------------------------------------------
// Response framing
// ---------------------------------------------------------------------------

interface ParsedHead {
  readonly statusLine: string;
  readonly httpVersion: string;
  readonly status: number;
  readonly reasonLine: string;
  readonly headers: WireHeaders;
  /** Offset of the first body byte within the accumulated buffer. */
  readonly bodyOffset: number;
}

const parseHead = (input: Buffer, split: number): ParsedHead | null => {
  const [statusLine = '', ...fields] = input.toString('latin1', 0, split).split(CRLF);
  const match = STATUS_LINE.exec(statusLine);
  if (match === null) return null;
  return {
    statusLine,
    httpVersion: match[1] ?? '',
    status: Number(match[2]),
    reasonLine: match[3] ?? '',
    headers: makeHeaders(
      fields.flatMap((line): ReadonlyArray<readonly [string, string]> => {
        const at = line.indexOf(':');
        return at < 0 ? [] : [[line.slice(0, at).trim(), line.slice(at + 1).trim()] as const];
      }),
    ),
    bodyOffset: split + HEAD_TERMINATOR.length,
  };
};

/**
 * A `Content-Length` this client is willing to frame on.
 *
 * Wholly numeric or nothing: `abc` is the malformed-framing leg, and `5, 5` is
 * the joined-duplicate divergence.  Both degrade to `until-close`, which is
 * what a client that cannot tell how long the body is must do.
 */
const framedLength = (headers: WireHeaders): number | null => {
  const raw = headers.get('content-length');
  if (raw === null) return null;
  const text = raw.trim();
  return /^[0-9]+$/.test(text) ? Number(text) : null;
};

const isChunked = (headers: WireHeaders): boolean =>
  headers
    .all('transfer-encoding')
    .some((value) => value.toLowerCase().split(',').some((token) => token.trim() === 'chunked'));

interface ChunkScan {
  readonly complete: boolean;
  readonly body: Buffer;
}

/**
 * De-chunk what has arrived so far and say whether the terminal chunk is in it.
 *
 * Recursive rather than a loop, and re-run on each arrival rather than kept as
 * a resumable parser: the bodies here are kilobytes, and a scanner with no
 * state cannot desynchronize from the buffer it is scanning.
 */
const scanChunked = (input: Buffer, offset: number): ChunkScan => {
  const walk = (at: number, parts: ReadonlyArray<Buffer>): ChunkScan => {
    const end = input.indexOf(CRLF, at, 'latin1');
    if (end < 0) return { complete: false, body: Buffer.concat([...parts]) };
    const header = input.toString('latin1', at, end);
    const size = Number.parseInt(header.split(';')[0] ?? '', 16);
    if (!Number.isInteger(size) || size < 0) {
      return { complete: false, body: Buffer.concat([...parts]) };
    }
    if (size === 0) {
      // The trailer section — empty or not — always ends with a blank line.
      return {
        complete: input.indexOf(HEAD_TERMINATOR, at) >= 0,
        body: Buffer.concat([...parts]),
      };
    }
    const start = end + CRLF.length;
    if (input.byteLength < start + size + CRLF.length) {
      return { complete: false, body: Buffer.concat([...parts]) };
    }
    return walk(start + size + CRLF.length, [...parts, input.subarray(start, start + size)]);
  };
  return walk(offset, []);
};

interface Complete {
  readonly framing: WireFraming;
  readonly body: Buffer;
}

/**
 * The framing law, as one total function over "everything read so far".
 *
 * `null` means *keep reading*; a {@link Complete} means stop **now**, which is
 * the whole reason the phantom 12-second EOF wait does not happen here.
 */
const completion = (head: ParsedHead, input: Buffer, method: string): Complete | null => {
  const body = input.subarray(head.bodyOffset);
  if (method.toUpperCase() === 'HEAD') return { framing: 'head-request', body: Buffer.alloc(0) };
  if (bodilessStatus(head.status)) return { framing: 'status-no-body', body: Buffer.alloc(0) };
  if (isChunked(head.headers)) {
    const scan = scanChunked(input, head.bodyOffset);
    return scan.complete ? { framing: 'chunked', body: scan.body } : null;
  }
  const length = framedLength(head.headers);
  if (length === null) return null;
  return body.byteLength >= length
    ? { framing: 'content-length', body: body.subarray(0, length) }
    : null;
};

// ---------------------------------------------------------------------------
// The client
// ---------------------------------------------------------------------------

/**
 * Send one request on one connection and return its outcome.
 *
 * Never rejects and never throws: every terminal state — including a peer that
 * closes without answering — is a {@link WireOutcome} the rig can compare
 * between the two implementations.
 */
export const wireRequest = (origin: string, request: WireRequest): Promise<WireOutcome> => {
  const endpoint = parseOrigin(origin);
  if (endpoint === null) {
    return Promise.resolve({
      _tag: 'ConnectFailed',
      origin,
      message: 'origin is not a usable http:// authority',
    });
  }
  const body = bodyBuffer(request.body);
  const head = renderHead(request, endpoint, body);
  const timeoutMs = request.timeoutMs ?? DEFAULT_WIRE_TIMEOUT_MS;

  return new Promise<WireOutcome>((resolve) => {
    // A single mutable cell, not a scatter of `let`s: `settled` is the
    // once-guard every listener consults, and `chunks` is the accumulator.
    const state: { settled: boolean; chunks: ReadonlyArray<Buffer> } = {
      settled: false,
      chunks: [],
    };
    const socket = connect({ host: endpoint.hostname, port: endpoint.port });

    const settle = (outcome: WireOutcome): void => {
      if (state.settled) return;
      state.settled = true;
      // Stop reading the moment the framing rule is satisfied.  This is the
      // line that turns a 12-second idle-close into a sub-millisecond request.
      socket.destroy();
      resolve(outcome);
    };

    const received = (): Buffer => Buffer.concat([...state.chunks]);

    const finish = (framing: Complete, parsed: ParsedHead, input: Buffer): void =>
      settle({
        _tag: 'Response',
        status: parsed.status,
        reasonLine: parsed.reasonLine,
        statusLine: parsed.statusLine,
        httpVersion: parsed.httpVersion,
        headers: parsed.headers,
        bodyBytes: new Uint8Array(framing.body),
        completedBy: framing.framing,
        wireBytes: input.byteLength,
      });

    /** Called on every arrival, and once more at EOF with `atEof` set. */
    const consider = (atEof: boolean): void => {
      const input = received();
      const split = input.indexOf(HEAD_TERMINATOR);
      if (split < 0) {
        if (!atEof) return;
        settle(
          input.byteLength === 0
            ? { _tag: 'ClosedWithoutResponse', message: 'peer closed before sending any byte' }
            : {
                _tag: 'Malformed',
                reason: 'connection ended inside the header block',
                preview: input.toString('latin1', 0, 200),
              },
        );
        return;
      }
      const parsed = parseHead(input, split);
      if (parsed === null) {
        settle({
          _tag: 'Malformed',
          reason: 'first line is not an HTTP status line',
          preview: input.toString('latin1', 0, 200),
        });
        return;
      }
      const complete = completion(parsed, input, request.method);
      if (complete !== null) {
        finish(complete, parsed, input);
        return;
      }
      if (!atEof) return;
      // EOF is itself the terminator when the server never said how long the
      // body is — the HTTP/1.0 `send_error` path CPython takes.
      const declared = framedLength(parsed.headers);
      const bodySoFar = input.subarray(parsed.bodyOffset);
      settle(
        declared === null && !isChunked(parsed.headers)
          ? {
              _tag: 'Response',
              status: parsed.status,
              reasonLine: parsed.reasonLine,
              statusLine: parsed.statusLine,
              httpVersion: parsed.httpVersion,
              headers: parsed.headers,
              bodyBytes: new Uint8Array(bodySoFar),
              completedBy: 'until-close',
              wireBytes: input.byteLength,
            }
          : {
              _tag: 'Truncated',
              status: parsed.status,
              expectedBodyBytes: declared ?? -1,
              receivedBodyBytes: bodySoFar.byteLength,
            },
      );
    };

    socket.setTimeout(timeoutMs);
    socket.on('timeout', () =>
      settle({ _tag: 'Timeout', afterMs: timeoutMs, bytesReceived: received().byteLength }),
    );
    socket.on('connect', () => {
      socket.write(head, 'latin1');
      if (body !== null) socket.write(body);
    });
    socket.on('data', (chunk: Buffer) => {
      state.chunks = [...state.chunks, chunk];
      consider(false);
    });
    // An `ECONNRESET` *after* a complete response is normal, not an error: both
    // gateways reset the connection after answering a request their parser
    // refused.  With bytes in hand the arrival path decides; with none, the
    // reset is the result.
    socket.on('error', (cause: Error) =>
      state.chunks.length === 0
        ? settle({ _tag: 'ConnectFailed', origin, message: cause.message })
        : consider(true),
    );
    socket.on('end', () => consider(true));
    socket.on('close', () => consider(true));
  });
};

/** `wireRequest` for the common case. */
export const wireGet = (
  origin: string,
  target: string,
  options: Omit<WireRequest, 'method' | 'target'> = {},
): Promise<WireOutcome> => wireRequest(origin, { ...options, method: 'GET', target });

/**
 * A one-line rendering of an outcome, for a rig's evidence table.
 *
 * Deliberately lossy and deliberately not a comparison key — assertions read
 * the fields.
 */
export const describeOutcome = (outcome: WireOutcome): string =>
  isWireResponse(outcome)
    ? `${outcome.status} ${outcome.completedBy} ${outcome.bodyBytes.byteLength}B`
    : outcome._tag;
