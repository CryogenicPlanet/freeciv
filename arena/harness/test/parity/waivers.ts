/**
 * The divergence waiver list — **closed**, measured, and self-policing.
 *
 * A waiver here is not "we decided not to care".  It is a claim with three
 * parts, all of which the rig that owns the leg checks on every run:
 *
 * 1. **Python still does what we measured.**  If CPython's answer moves, the
 *    waiver's premise is gone and the test fails.
 * 2. **The TypeScript side still does what we measured.**  A waived leg is
 *    never skipped: its actual behavior is asserted just as tightly as a
 *    matching leg's, so "Bun answers a bare 400" is a pinned fact rather than a
 *    hole in the rig.
 * 3. **The two are still different.**  The moment they agree, the waiver is
 *    *dead weight* that would silently exempt a future regression — so
 *    {@link waiverStillNeeded} goes false and the run fails, telling you to
 *    delete the entry.
 *
 * ## The membership rule
 *
 * A divergence is waivable when it is **imposed by either runtime's HTTP layer
 * outside route code** — Bun's parser and URL-normalizer on the way in, the
 * adapter's response framing on the way out, or CPython's stdlib request-line
 * handling — **with a measured signature pair per entry**.  There is no code
 * either gateway could add to change one: the request never becomes a
 * `Request`, or the response never reaches the socket in the shape the handler
 * built it.
 *
 * One entry sits just outside that sentence and is called out rather than
 * smuggled in: `query-ucd-skew-11de0` is a divergence between the two
 * runtimes' shipped **Unicode tables**, not their HTTP layers.  It qualifies
 * for the same reason the rest do — route code cannot override the UCD its
 * runtime was built with — and it closes itself when CPython's table catches
 * up, which {@link waiverStillNeeded} will notice.
 *
 * Everything else that diverges is a **bug**: `diff.test.ts` reports it as one,
 * and a defect the port *can* close is pinned in that file's open-findings
 * table, never here.  The list cannot grow by argument — a new member has to be
 * a new *runtime* behavior, measured on both sides, with a leg that replays it.
 *
 * ## Two arenas, one list
 *
 * Every entry names the rig that replays and polices it ({@link WaiverArena}):
 *
 * - **`matrix`** — `diff.test.ts`, which replays the leg in all nine upstream
 *   scenarios.  Most matrix entries target `/health` or fail before routing
 *   (`/health` never contacts the upstream, `dispatch.ts:504`), so they are
 *   scenario-independent and say nothing about the upstream fixture.  The one
 *   exception is `binary-disk-fallback-chunked`, which is *only* reachable when
 *   the archive is read from disk and therefore carries an explicit
 *   {@link ParityWaiver.scenarios} list; in the other four scenarios the same
 *   legs are compared in full, with no exemption at all.
 * - **`query-fuzz`** — `hunt-query-fuzz.test.ts`, whose legs are request
 *   targets rather than routes.  Four entries live there because the behavior
 *   they name is a property of a *byte sequence in the request line*, which the
 *   matrix's route table does not vary.
 *
 * ## The signature
 *
 * {@link waiverSignature} is deliberately coarse — status, reason phrase,
 * content type, and whether the body is empty.  It is *not* a body hash,
 * because two of these legs answer with `/health`'s identity payload whose
 * length moves with the scratch directory's name.  What it captures is exactly
 * the shape of each measured divergence, and each entry's expected strings were
 * transcribed from a run, not predicted.
 *
 * An entry whose divergence is invisible in those four fields sets
 * {@link ParityWaiver.framing}, which appends the rule that ended the read off
 * the socket (`content-length`, `chunked`, `until-close`).  Exactly one entry
 * needs it, and needs it twice over: a PNG served from disk has the same
 * status, phrase, type and non-empty body on both sides, so without the
 * framing field the waiver would look dead the moment it was written, and the
 * divergence it exists for would be unobservable.
 *
 * @module
 */

import { bodyLatin1, isWireResponse, type WireOutcome } from './wire-client.ts';

// ---------------------------------------------------------------------------
// Signatures
// ---------------------------------------------------------------------------

/**
 * A response's divergence-relevant shape, as one comparable string.
 *
 * `body:empty` / `body:nonempty` rather than the bytes: the two `/health` legs
 * below carry a payload containing an `mkdtemp` path, so a length or a digest
 * would change per run while the *divergence* would not.  Body **content** is
 * still compared byte-for-byte by the matrix for every non-waived leg, and for
 * the waived ones the byte comparison is precisely what this list exempts.
 *
 * `withFraming` appends the framing rule that ended the read — a fifth field
 * for the one entry whose divergence is *only* framing.  It is off by default
 * so the other entries' recorded strings stay the four fields they were
 * measured as.
 */
export const waiverSignature = (outcome: WireOutcome, withFraming = false): string =>
  isWireResponse(outcome)
    ? [
        String(outcome.status),
        JSON.stringify(outcome.reasonLine),
        outcome.headers.get('content-type') ?? '-',
        bodyLatin1(outcome).length === 0 ? 'body:empty' : 'body:nonempty',
        ...(withFraming ? [`framing:${outcome.completedBy}`] : []),
      ].join(' | ')
    : outcome._tag;

// ---------------------------------------------------------------------------
// The list
// ---------------------------------------------------------------------------

/** Which parts of a comparison a waiver exempts. */
export type WaivedAspect = 'status' | 'reason' | 'headers' | 'body';

/** The rig that replays a waived leg and polices its entry. */
export type WaiverArena = 'matrix' | 'query-fuzz';

/** One measured, accepted divergence. */
export interface ParityWaiver {
  /** Stable identifier, for prose that needs to name one. */
  readonly id: string;
  /** The rig whose leg names below belong to it. */
  readonly arena: WaiverArena;
  /** Every leg this applies to.  All of them diverge the same measured way. */
  readonly legs: ReadonlyArray<string>;
  /**
   * `matrix` only: the upstream scenarios in which the divergence exists.
   *
   * Omitted means *every* scenario, which is true of every entry that never
   * reaches the upstream.  Naming a subset is a claim in both directions — the
   * listed scenarios must diverge exactly as measured, and the unlisted ones
   * are compared with no exemption whatever.
   */
  readonly scenarios?: ReadonlyArray<string>;
  /** What the rig must not fail on for these legs. */
  readonly aspects: ReadonlyArray<WaivedAspect>;
  /**
   * Restricts the `headers` aspect to these header names, lower-case.
   *
   * Omitted waives the whole compared header set.  Present, it waives exactly
   * these and leaves every other compared header asserted — which is what makes
   * "framing only" a checkable claim rather than a sentence in a comment.
   */
  readonly headers?: ReadonlyArray<string>;
  /** Extend the signature with the framing rule.  See {@link waiverSignature}. */
  readonly framing?: true;
  /** Why neither port can close it. */
  readonly reason: string;
  /** Where it was measured, so the claim is checkable against a second rig. */
  readonly measuredIn: string;
  /** {@link waiverSignature} of CPython's answer, as measured. */
  readonly python: string;
  /** {@link waiverSignature} of Bun's answer, as measured. */
  readonly typescript: string;
}

const JSON_UTF8 = 'application/json; charset=utf-8';
const STDLIB_HTML = 'text/html;charset=utf-8';

/** The five scenarios in which the archive is read from disk rather than relayed. */
const DISK_FALLBACK_SCENARIOS: ReadonlyArray<string> = [
  'upstream-down',
  'stub-not-found-404',
  'stub-method-405',
  'stub-portless',
  'stub-hang',
];

/**
 * Twelve entries.  Adding a thirteenth requires a measurement, not an argument.
 */
export const PARITY_WAIVERS: ReadonlyArray<ParityWaiver> = [
  {
    id: 'bad-content-length',
    arena: 'matrix',
    legs: ['bad-content-length'],
    aspects: ['headers', 'body'],
    reason:
      "Bun's parser refuses a `Content-Length` it cannot read and writes its own bodiless `400` before the handler exists; CPython reaches `_reject_body` (`:1390`) and produces the `invalid Content-Length` problem body. The status agrees; the body and our three security headers cannot.",
    measuredIn:
      'test/gateway/smoke-live.test.ts "an unparseable Content-Length is answered by Bun, before any handler"',
    python: `400 | "Bad Request" | ${JSON_UTF8} | body:nonempty`,
    typescript: '400 | "Bad Request" | - | body:empty',
  },
  {
    id: 'transfer-encoding-identity',
    arena: 'matrix',
    legs: ['transfer-encoding-identity'],
    aspects: ['headers', 'body'],
    reason:
      '`Transfer-Encoding: identity` is a legal field value CPython treats as "a body is present" (`:1394` → `GET request bodies are not accepted`); Bun rejects the framing outright. `Transfer-Encoding: chunked` does reach the handler and matches, which is what makes this a parser boundary and not a routing difference.',
    measuredIn:
      'test/gateway/smoke-live.test.ts "a Transfer-Encoding on a GET is likewise answered by Bun"',
    python: `400 | "Bad Request" | ${JSON_UTF8} | body:nonempty`,
    typescript: '400 | "Bad Request" | - | body:empty',
  },
  {
    id: 'duplicate-content-length',
    arena: 'matrix',
    legs: ['duplicate-content-length'],
    aspects: ['status', 'reason', 'body'],
    reason:
      'Two `Content-Length: 0` fields, and the two runtimes disagree about what a repeated framing header *is*. CPython reads it through `email.message.Message.get`, which returns the **first** occurrence, so `_reject_body` sees `int("0") == 0`, concludes there is no body, and serves `/health` with a `200`. Bun hands the handler a `Headers` object that has **joined** them into `0, 0`; the port\'s Python-`int` parse refuses that and answers its own `400 invalid Content-Length` — which is the correct answer to the value it was given, and unreachable from the port, because the two fields no longer exist separately by the time any code owned here can look. The **headers** match (both are the gateway\'s own JSON problem shape); only the status, the reason and the body differ. It is also why the parity client treats a joined `Content-Length` as non-numeric (`wire-client.ts` `framedLength`).',
    measuredIn:
      'test/parity/waiver-oracles.test.ts — three measurements, none of them this rig: a bare `Bun.serve` is handed `"0, 0"` (so the two fields are gone before any Effect runs), `http.client.parse_headers` returns `\'0\'` out of two occurrences in the same `python3` that owns `agent_eval`, and `server.ts#bodySignal` maps `"0, 0"` to `invalid-content-length`. The signatures below are from a matrix run, which pins the pair in all nine scenarios every time.',
    python: `200 | "OK" | ${JSON_UTF8} | body:nonempty`,
    typescript: `400 | "Bad Request" | ${JSON_UTF8} | body:nonempty`,
  },
  {
    id: 'space-in-request-target',
    arena: 'matrix',
    legs: ['space-in-request-target'],
    aspects: ['status', 'reason', 'headers', 'body'],
    reason:
      "`GET /v1/games?a= b HTTP/1.1` — CPython's `parse_request` cannot split three tokens out of four and sends the stdlib `400` page naming the whole line; Bun reads the trailing `b HTTP/1.1` as the protocol version and refuses *that* with a `505`. Neither request ever reaches a router.",
    measuredIn:
      'test/gateway/smoke-live.test.ts "a space in the request target: CPython 400-HTML, Bun 505"',
    python: `400 | "Bad request syntax ('GET /v1/games?a= b HTTP/1.1')" | ${STDLIB_HTML} | body:nonempty`,
    typescript: '505 | "HTTP Version Not Supported" | - | body:empty',
  },
  {
    id: 'invented-verb',
    arena: 'matrix',
    legs: ['invented-verb'],
    aspects: ['status', 'reason', 'headers', 'body'],
    reason:
      "CPython answers any unmapped verb with `501 Unsupported method ('FROB')` and the stdlib HTML page. Bun's parser accepts only known methods and drops the connection with **no response bytes at all**, so there is nothing for the port to answer with. Recorded rather than commented so a future Bun that starts delivering `FROB` fails this list.",
    measuredIn:
      'test/gateway/server.test.ts "an invented verb never reaches us at all — Bun closes the connection"',
    python: `501 | "Unsupported method ('FROB')" | ${STDLIB_HTML} | body:nonempty`,
    typescript: 'ClosedWithoutResponse',
  },
  {
    id: 'trace-501',
    arena: 'matrix',
    legs: ['trace-501'],
    aspects: ['reason'],
    reason:
      "`Response.statusText` is discarded by Bun's adapter, which writes the canonical phrase for the status. CPython's reason phrase carries the verb. The **body** — the 483-byte stdlib page that also names the verb — is byte-identical on both sides, which is why only `reason` is waived here.",
    measuredIn:
      'test/gateway/server.test.ts "TRACE is the stdlib HTML 501, with none of our headers" (the `response.reason` expectation)',
    python: `501 | "Unsupported method ('TRACE')" | ${STDLIB_HTML} | body:nonempty`,
    typescript: `501 | "Not Implemented" | ${STDLIB_HTML} | body:nonempty`,
  },
  {
    id: 'head-health-body-visible',
    arena: 'matrix',
    legs: ['head-health-body-visible'],
    aspects: ['status', 'reason', 'headers', 'body'],
    reason:
      "Bun's adapter strips the body of a `HEAD` response; CPython's `do_HEAD = _method_not_allowed` writes the 30-byte problem body anyway. A plain `HEAD` leg cannot see it — an honest HTTP client stops at the header block — so this leg spells the request line by hand and reads with `Content-Length` framing, which makes the stripped body visible as a `Truncated` outcome. The headers, `Content-Length: 30` included, do match; that is the `head-health` leg, and it is not waived.",
    measuredIn:
      'test/gateway/smoke-live.test.ts "HEAD: headers match including Content-Length; the adapter strips the body"',
    python: `405 | "Method Not Allowed" | ${JSON_UTF8} | body:nonempty`,
    typescript: 'Truncated',
  },
  {
    id: 'binary-disk-fallback-chunked',
    arena: 'matrix',
    legs: ['frame-zero', 'frame-last', 'frame-latest', 'frame-unpaired-archive'],
    scenarios: DISK_FALLBACK_SCENARIOS,
    aspects: ['headers'],
    headers: ['content-length'],
    framing: true,
    reason:
      // The dossier, consolidated here from `OPEN_FINDINGS` and from
      // `archive.ts#sendLocalFile`, because this is now the one place that
      // decides it.
      'A PNG served from the **disk archive** goes out `Transfer-Encoding: chunked` with no `Content-Length`; CPython sends `Content-Length: st_size` and no chunking. The bytes are byte-identical, `Range` is ignored by both (`200`, never a `206`), and every other compared header matches — only the framing differs. Cause, measured: Bun drops a `Content-Length` from any `Response` whose body is a `ReadableStream` that does not resolve synchronously, which every Effect `Stream` is. Four candidate bodies were built and measured, and each is worse than the divergence: `Bun.file(path)` (a `Blob`) keeps the length but makes **Bun** answer `Range` itself — `Range: bytes=0-9` gets a `206 Partial Content` with `Content-Range` where CPython ignores the header and sends the whole `200`, on `video.mp4`, the one route a browser seeks in on every scrub; `Bun.file(fd)` keeps the length but does not take ownership of the descriptor, so leaving it open leaks one per request (**209 open after 200 requests, measured**) and closing it at scope exit truncates the body (also measured); a `ReadableStream` with `type: "direct"` is still chunked; a `Uint8Array` keeps the length and buffers an 18 MiB video per request. This is therefore a **decision**, not a defect being tolerated: a framing divergence with byte-identical bodies is strictly better than a `200`-versus-`206` status divergence on the video route. It closes when Bun can stream a body of known length, or when the port stops going through `Response` — and this entry dies with it, because the four legs would then match.',
    measuredIn:
      'test/gateway/routes-archive.test.ts (the `contentLength` on the response *value*), and the matrix itself: the four legs below, in the five scenarios that read the archive from disk. The `206` and the descriptor-leak numbers were measured against a standalone `Bun.serve` in the same Bun version, not inferred.',
    python: '200 | "OK" | image/png | body:nonempty | framing:content-length',
    typescript: '200 | "OK" | image/png | body:nonempty | framing:chunked',
  },
  {
    id: 'query-raw-bytes-utf8-vs-latin1',
    arena: 'query-fuzz',
    legs: ['disk-boundary-raw-arabic-indic-digit'],
    aspects: ['status', 'body'],
    reason:
      "Raw, non-percent-encoded bytes in the request target are decoded by the two runtimes with **different codecs before either gateway is called**. `?turn=<D9 A1>` is UTF-8 for `U+0661 ARABIC-INDIC DIGIT ONE`, which is what Bun hands the handler, and `int()` accepts any `\\p{Nd}`: the TypeScript side reads `turn=1`. CPython's `BaseHTTPRequestHandler` decodes the request line as **latin-1**, so `self.path` carries `Ù¡` and `int()` raises. Neither gateway can recover the original bytes — a raw byte that is *invalid* UTF-8 reaches Bun's handler as `U+FFFD`, so the information is gone below the port.",
    measuredIn:
      'test/parity/hunt-query-fuzz.test.ts, `disk` phase — CPython 400 `turn must be an integer`, TypeScript 404 `game not found` (the query parsed, so the request went on to the absent game).',
    python: `400 | "Bad Request" | ${JSON_UTF8} | body:nonempty`,
    typescript: `404 | "Not Found" | ${JSON_UTF8} | body:nonempty`,
  },
  {
    id: 'request-line-length-cap',
    arena: 'query-fuzz',
    legs: ['disk-boundary-request-line-16k'],
    aspects: ['status', 'reason', 'headers', 'body'],
    reason:
      "Bun's parser refuses a request line past ~16 KiB with a bodiless `431` and never builds a `Request`; CPython's `http.server` caps the line at 64 KiB (`_read_request_line`'s 65536), so a 16 KiB target reaches `do_GET` and is answered by the query parser — `400 replay query accepts one after_turn and one limit`. The cap is the runtime's, not the gateway's, and the gateway is not consulted on either side.",
    measuredIn:
      'test/parity/hunt-query-fuzz.test.ts, `disk` phase — the TypeScript answer is also read `until-close`, since a `431` with no body and no `Content-Length` is framed by the close.',
    python: `400 | "Bad Request" | ${JSON_UTF8} | body:nonempty`,
    typescript: '431 | "Request Header Fields Too Large" | - | body:empty',
  },
  {
    id: 'cpython-request-line-nel-split',
    arena: 'query-fuzz',
    legs: ['disk-boundary-cpython-nel-split'],
    aspects: ['reason', 'headers', 'body'],
    reason:
      "The mirror image of a Bun-parser waiver, and the reason this list is not \"Bun's fault\" by construction. A raw `85` byte in the target is `U+0085` once CPython latin-1-decodes the request line, and `str.split()` treats it as whitespace — so `parse_request` sees four tokens where it wants three and answers the stdlib `400` page *before* `do_GET`, with the whole request line in the reason phrase. Bun's parser splits on ASCII space only, so the byte reaches the handler, fails UTF-8 decoding into `U+FFFD`, and gets the gateway's own `400 turn must be an integer`. The **status** agrees; nothing else can.",
    measuredIn:
      'test/parity/hunt-query-fuzz.test.ts, `disk` phase. CPython escapes the byte into its own message, so the reason phrase below contains the four literal characters `\\`, `x`, `8`, `5`.',
    python: `400 | "Bad request syntax ('GET /v1/games/game_parity_absent_wellformed_id/board.json?turn=\\\\x855 HTTP/1.1')" | ${STDLIB_HTML} | body:nonempty`,
    typescript: `400 | "Bad Request" | ${JSON_UTF8} | body:nonempty`,
  },
  {
    id: 'query-ucd-skew-11de0',
    arena: 'query-fuzz',
    legs: ['disk-boundary-ucd-skew-11de0', 'disk-boundary-ucd-skew-11de9'],
    aspects: ['status', 'body'],
    reason:
      "`U+11DE0`–`U+11DE9` (Tulu-Tigalari digits) are `\\p{Nd}` in the Unicode database **Bun** ships and *unassigned* in the one CPython 3.14.6 ships. `int()` therefore raises where the port's `parsePythonInt` reads a digit: `?turn=<U+11DE0>5` is `400 turn must be an integer` on CPython and `turn=5` here. This is the one entry not imposed by an HTTP layer — it is a version skew between two shipped UCD tables, and it is waivable for the same reason the rest are: `services/upstream.ts` cannot override the table its runtime was built with, and the differential that found it agrees with CPython on all 2 228 224 code points but these ten. It dies when the interpreter's table catches up, and this list will say so.",
    measuredIn:
      'test/parity/hunt-query-fuzz.test.ts, `disk` phase, at both ends of the block (`U+11DE0`, the block zero, and `U+11DE9`, its nine). The 2 228 224-probe differential is `src/gateway/services/upstream.ts#digitRunOffset`.',
    python: `400 | "Bad Request" | ${JSON_UTF8} | body:nonempty`,
    typescript: `404 | "Not Found" | ${JSON_UTF8} | body:nonempty`,
  },
];

/**
 * True when this waiver's *measured* signature for `side` is "no response at
 * all" rather than a status line.
 *
 * {@link waiverSignature} renders a response as `|`-joined fields and a
 * non-response as the bare outcome tag, so the absence of the separator is the
 * discriminator — and it is a property of the recorded measurement, not of a
 * hand-kept second list.  Two entries qualify today: Bun's silence on an
 * invented verb, and the `Truncated` read of an adapter-stripped `HEAD` body.
 *
 * `diff.test.ts` reads this to build the *only* set of legs allowed to answer
 * nothing; every other leg, on both sides, must produce a response.
 */
export const waiverExpectsNoResponse = (
  waiver: ParityWaiver,
  side: 'python' | 'typescript',
): boolean => !waiver[side].includes(' | ');

/** Every waiver policed by one rig. */
export const waiversIn = (arena: WaiverArena): ReadonlyArray<ParityWaiver> =>
  PARITY_WAIVERS.filter((waiver) => waiver.arena === arena);

/**
 * The waiver covering `leg` in `scenario`, or `undefined`.
 *
 * An entry with no `scenarios` covers every one of them; an entry with a list
 * covers exactly those, and the leg is compared in full everywhere else.
 * `scenario` is omitted only by an arena that has no scenario axis.
 */
export const waiverFor = (
  arena: WaiverArena,
  leg: string,
  scenario?: string,
): ParityWaiver | undefined =>
  PARITY_WAIVERS.find(
    (waiver) =>
      waiver.arena === arena &&
      waiver.legs.includes(leg) &&
      (waiver.scenarios === undefined ||
        scenario === undefined ||
        waiver.scenarios.includes(scenario)),
  );

/**
 * True when `aspect` is exempt for a matrix `leg` in `scenario`.
 *
 * `detail` is the header name for a `headers` divergence and is ignored for
 * every other aspect: an entry that names {@link ParityWaiver.headers} waives
 * only those, so a *new* header divergence on a waived leg is still a failure.
 */
export const isWaived = (
  scenario: string,
  leg: string,
  aspect: WaivedAspect,
  detail: string,
): boolean => {
  const waiver = waiverFor('matrix', leg, scenario);
  if (waiver === undefined || !waiver.aspects.includes(aspect)) return false;
  return aspect !== 'headers' || waiver.headers === undefined || waiver.headers.includes(detail);
};

/**
 * A waiver is still *needed* only while the two measured signatures differ.
 *
 * Checked at runtime rather than trusted: someone updating `typescript` to
 * match a newly-fixed implementation, and forgetting to delete the entry, is
 * exactly the drift this list must not tolerate.
 */
export const waiverStillNeeded = (waiver: ParityWaiver): boolean =>
  waiver.python !== waiver.typescript;

// ---------------------------------------------------------------------------
// Checking one
// ---------------------------------------------------------------------------

/** What a waiver check concluded.  Only `Honored` is a passing result. */
export type WaiverVerdict =
  /** Both sides behave exactly as measured, and they still differ. */
  | { readonly _tag: 'Honored'; readonly leg: string }
  /**
   * The two sides now agree — the divergence is gone.  **Delete the entry**;
   * the leg belongs in the ordinary comparison from now on.
   */
  | { readonly _tag: 'NoLongerNeeded'; readonly leg: string; readonly signature: string }
  /** A side moved.  A waiver covers a *measured* behavior, not any behavior. */
  | {
      readonly _tag: 'Drifted';
      readonly leg: string;
      readonly side: 'python' | 'typescript';
      readonly expected: string;
      readonly actual: string;
    };

/**
 * Check one waived leg's two outcomes against what the list claims.
 *
 * Never throws; the verdict is the value a test asserts on.
 */
export const checkWaiver = (
  waiver: ParityWaiver,
  python: WireOutcome,
  typescript: WireOutcome,
  leg: string = waiver.legs[0] ?? waiver.id,
): WaiverVerdict => {
  const framing = waiver.framing === true;
  const pythonSignature = waiverSignature(python, framing);
  const typescriptSignature = waiverSignature(typescript, framing);
  if (pythonSignature === typescriptSignature) {
    return { _tag: 'NoLongerNeeded', leg, signature: pythonSignature };
  }
  if (pythonSignature !== waiver.python) {
    return { _tag: 'Drifted', leg, side: 'python', expected: waiver.python, actual: pythonSignature };
  }
  if (typescriptSignature !== waiver.typescript) {
    return {
      _tag: 'Drifted',
      leg,
      side: 'typescript',
      expected: waiver.typescript,
      actual: typescriptSignature,
    };
  }
  return { _tag: 'Honored', leg };
};
