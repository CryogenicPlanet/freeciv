/**
 * The waiver list's **second** oracle.
 *
 * `waivers.ts` makes a three-part claim about each accepted divergence — Python
 * still does X, Bun still does Y, and X ≠ Y — and `diff.test.ts` checks all
 * three on every run.  That is enough to *police* a waiver and not enough to
 * *justify* one, because the rig that consumes the entry is then the only thing
 * that has ever measured it.  Six of the seven entries name a source outside
 * this directory (`test/gateway/smoke-live.test.ts`, `test/gateway/server.test.ts`);
 * `duplicate-content-length` named `diff.test.ts` itself, and nothing anywhere
 * asserted the joined-`Content-Length` behavior at all — `dispatch.test.ts:484`
 * covers only the *unparseable* value, never `0, 0`.
 *
 * This file is that missing source, and it deliberately uses **neither
 * gateway**: three measurements, three different oracles, no parity rig.
 *
 * 1. **Bun's parser joins the two fields** before a handler exists — measured
 *    against a bare `Bun.serve` that only reports what it was given.  This is
 *    what makes the divergence unreachable from any code the port owns: by the
 *    time an `Effect` runs, the two header fields are one string.
 * 2. **CPython's parser returns the first** — measured by running the very
 *    class `BaseHTTPRequestHandler` stores in `self.headers`
 *    (`email.message.Message`, through `email.parser`), in the same `python3`
 *    that owns `agent_eval`.
 * 3. **The gateway's answer to what Bun hands it is correct** — `bodySignal`
 *    (`src/gateway/server.ts`) is a pure function of the headers, and it refuses
 *    `"0, 0"` because Python's `int()` refuses it.  So the `400` is not a port
 *    defect that a waiver excuses; it is the right answer to a different input.
 *
 * Together those three say exactly what the waiver claims: same request, two
 * parsers, two different values delivered to two identical decision procedures.
 *
 * No `describe.if` here.  These oracles need Bun and `python3` and nothing else,
 * so the corroboration exists on every platform the suite runs on — including
 * the ones where the matrix itself is skipped.
 *
 * @module
 */

import { describe, expect, test } from 'bun:test';
import { Headers } from '@effect/platform';
import { connect } from 'node:net';
import { PYTHON_BIN } from './boot.ts';
import { bodySignal } from '../../src/gateway/server.ts';
import { waiverFor } from './waivers.ts';

/** The leg whose waiver this file exists to corroborate. */
const LEG = 'duplicate-content-length';

/**
 * The request, spelled on the socket, because no HTTP client will send it.
 *
 * `fetch` and `Headers` both collapse a repeated field before it reaches a
 * connection, which is the whole reason `wire-client.ts` exists and the reason
 * this oracle writes bytes.
 */
const DUPLICATE_LENGTH_REQUEST =
  'GET /probe HTTP/1.1\r\nHost: probe.invalid\r\nContent-Length: 0\r\nContent-Length: 0\r\nConnection: close\r\n\r\n';

/** Everything the stub handler saw, as one line it writes back. */
const askBunParser = async (): Promise<string> => {
  const server = Bun.serve({
    port: 0,
    hostname: '127.0.0.1',
    fetch: (request) =>
      new Response(JSON.stringify(request.headers.get('content-length')), {
        headers: { 'content-type': 'application/json' },
      }),
  });
  // `Bun.serve`'s `port` is optional in the type (a unix socket has none); a
  // `--port 0` TCP listener always has one, and asserting it here keeps the
  // socket call total.
  const port = server.port ?? 0;
  const received = await new Promise<string>((resolve) => {
    const chunks: string[] = [];
    // `node:net`, the same socket `wire-client.ts` uses, and for the same
    // reason: this request has to reach the wire byte for byte.
    const socket = connect({ host: '127.0.0.1', port }, () => {
      socket.write(DUPLICATE_LENGTH_REQUEST);
    });
    socket.on('data', (data: Buffer) => chunks.push(data.toString('latin1')));
    socket.on('close', () => {
      resolve(chunks.join(''));
    });
  });
  await server.stop(true);
  return received;
};

/**
 * `self.headers.get("Content-Length")` in CPython, without a gateway.
 *
 * `BaseHTTPRequestHandler` builds `self.headers` with
 * `http.client.parse_headers`, which is `email.parser.Parser(_class=HTTPMessage)`
 * — so this is the same `Message.get` that `_reject_body` (`:1390`) calls, and
 * `get` documents itself as returning the first matching field.
 */
const askCPythonParser = (): string => {
  const program = [
    'import http.client, io',
    'raw = b"Content-Length: 0\\r\\nContent-Length: 0\\r\\n\\r\\n"',
    'message = http.client.parse_headers(io.BufferedReader(io.BytesIO(raw)))',
    'print(repr(message.get("Content-Length")), len(message.get_all("Content-Length")))',
  ].join('\n');
  const probe = Bun.spawnSync([PYTHON_BIN, '-c', program]);
  return probe.stdout.toString().trim();
};

describe('the duplicate-Content-Length waiver, corroborated outside the parity rig', () => {
  test('the waiver still exists and still claims what this file measures', () => {
    const waiver = waiverFor('matrix', LEG);
    expect(waiver?.aspects.toSorted()).toEqual(['body', 'reason', 'status']);
    // The measured signatures the matrix pins: a served `/health` against our
    // own 400.  Named here so this file fails too if the entry is reworded into
    // a different claim.
    expect([waiver?.python.startsWith('200 '), waiver?.typescript.startsWith('400 ')]).toEqual([
      true,
      true,
    ]);
  });

  test("Bun joins the two fields into `0, 0` before any handler exists", async () => {
    const response = await askBunParser();
    const body = response.slice(response.indexOf('\r\n\r\n') + 4);
    // The handler *ran* — this is not a parser refusal — and what it was handed
    // is one comma-joined string.  Nothing the port could do at any layer it
    // owns can recover the two fields from that.
    expect(response.startsWith('HTTP/1.1 200')).toBe(true);
    expect(JSON.parse(body)).toBe('0, 0');
  });

  test('CPython hands its handler the first field, and knows there were two', () => {
    // `repr(...)` then the count: the value is `'0'`, from two occurrences.
    expect(askCPythonParser()).toBe("'0' 2");
  });

  test("the gateway's 400 is the correct answer to the value Bun delivers", () => {
    // `int("0, 0")` raises in Python, so `_reject_body` would answer exactly
    // this if it were ever handed the joined value.  The divergence is in what
    // the two runtimes deliver, not in what either gateway decides.
    expect(bodySignal(Headers.fromInput({ 'content-length': '0, 0' }))).toBe(
      'invalid-content-length',
    );
    expect(bodySignal(Headers.fromInput({ 'content-length': '0' }))).toBe('absent');
  });
});
