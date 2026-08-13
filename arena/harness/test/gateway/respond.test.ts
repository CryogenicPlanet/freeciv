/** Exact error status, body, headers, 405, and private-detail coverage. */

import { describe, expect, test } from 'bun:test';
import { Gateway } from '@arena/wire';
import { HttpServerResponse } from '@effect/platform';
import { Effect, Either, Exit, Logger, LogLevel } from 'effect';
import {
  ARCHIVE_UNAVAILABLE_PROBLEMS,
  ArchiveUnavailable,
  BAD_REQUEST_PROBLEMS,
  BadRequest,
  type GatewayError,
  gatewayProblem,
  InternalError,
  MethodNotAllowed,
  NOT_FOUND_PROBLEMS,
  NotFound,
  UpstreamHttpError,
  UpstreamInvalid,
  UpstreamTooLarge,
  UpstreamUnavailable,
} from 'src/gateway/errors';
import {
  catchGatewayErrors,
  GATEWAY_SECURITY_HEADERS,
  gatewayJsonResponse,
  renderProblemBody,
  respondGateway,
  toResponse,
  withSecurityHeaders,
} from 'src/gateway/http/respond';

// ---------------------------------------------------------------------------
// Instruments
// ---------------------------------------------------------------------------

interface Observed {
  readonly status: number;
  readonly headers: ReadonlyMap<string, string>;
  readonly bytes: Uint8Array;
  readonly text: string;
}

/** Everything that would reach the socket, read back off a real `Response`. */
const observe = async (
  response: HttpServerResponse.HttpServerResponse,
): Promise<Observed> => {
  const web = HttpServerResponse.toWeb(response);
  const bytes = new Uint8Array(await web.arrayBuffer());
  return {
    status: web.status,
    headers: new Map(web.headers.entries()),
    bytes,
    text: new TextDecoder().decode(bytes),
  };
};

/** `_canonical({"error": message})` — the expected body, from wire, never hand-written. */
const canonicalProblemBytes = (message: string): Uint8Array =>
  Either.getOrElse(Gateway.gatewayProblemBytes(message), () => new Uint8Array());

const silently = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
  Logger.withMinimumLogLevel(effect, LogLevel.None);

/** One error value, with the label its test is named after. */
interface Case {
  readonly label: string;
  readonly error: GatewayError;
}

const badRequests: readonly Case[] = BAD_REQUEST_PROBLEMS.map((problem) => ({
  label: `BadRequest.${problem}`,
  error: new BadRequest({ problem }),
}));

const notFounds: readonly Case[] = NOT_FOUND_PROBLEMS.map((problem) => ({
  label: `NotFound.${problem}`,
  error: new NotFound({ problem }),
}));

const archiveUnavailables: readonly Case[] = ARCHIVE_UNAVAILABLE_PROBLEMS.map((problem) => ({
  label: `ArchiveUnavailable.${problem}`,
  error: new ArchiveUnavailable({ problem }),
}));

/**
 * Exactly one case per fixed catalogue message, plus the 3xx form of
 * {@link UpstreamHttpError} — which is the only class that can produce
 * `upstream redirects are not allowed`.
 */
const catalogueCases: readonly Case[] = [
  ...badRequests,
  ...notFounds,
  { label: 'MethodNotAllowed', error: new MethodNotAllowed() },
  { label: 'InternalError', error: new InternalError({ cause: 'defect' }) },
  {
    label: 'UpstreamUnavailable.portless',
    error: new UpstreamUnavailable({ reason: 'portless' }),
  },
  { label: 'UpstreamTooLarge', error: new UpstreamTooLarge() },
  { label: 'UpstreamInvalid', error: new UpstreamInvalid() },
  { label: 'UpstreamHttpError.302', error: new UpstreamHttpError({ upstreamStatus: 302 }) },
  ...archiveUnavailables,
];

/** Every case whose response is rendered: the catalogue plus the open forms. */
const renderedCases: readonly Case[] = [
  ...catalogueCases,
  {
    label: 'UpstreamUnavailable.transport',
    error: new UpstreamUnavailable({ reason: 'transport', cause: new Error('ECONNREFUSED') }),
  },
  { label: 'UpstreamHttpError.500', error: new UpstreamHttpError({ upstreamStatus: 500 }) },
  { label: 'UpstreamHttpError.409', error: new UpstreamHttpError({ upstreamStatus: 409 }) },
];

// ---------------------------------------------------------------------------
// 1. The taxonomy covers the wire catalogue, exactly once, at the right status
// ---------------------------------------------------------------------------

describe('the taxonomy is a partition of the wire catalogue', () => {
  test('every fixed message is owned by exactly one class', () => {
    const emitted = catalogueCases.map((entry) => entry.error.message);
    const catalogue = Object.values(Gateway.GATEWAY_PROBLEM_MESSAGES);

    // No duplicates: one case per message, and one message per case.
    expect(new Set(emitted).size).toBe(emitted.length);
    expect(emitted.toSorted()).toEqual(catalogue.toSorted());
  });

  test('every class agrees with GATEWAY_PROBLEM_STATUS about its status', () => {
    const disagreements = catalogueCases.filter(
      (entry) =>
        !Gateway.isKnownGatewayProblemMessage(entry.error.message) ||
        Gateway.GATEWAY_PROBLEM_STATUS[entry.error.message] !== entry.error.status,
    );
    expect(disagreements.map((entry) => entry.label)).toEqual([]);
  });

  test('the message on the error IS the Error message, so logs cannot drift', () => {
    const error = new NotFound({ problem: 'terminalArchiveNotFound' });
    expect(error.message).toBe(Gateway.GATEWAY_PROBLEM_MESSAGES.terminalArchiveNotFound);
    expect(error).toBeInstanceOf(Error);
    expect(String(error)).toBe(`NotFound: ${Gateway.GATEWAY_PROBLEM_MESSAGES.terminalArchiveNotFound}`);
    expect(error._tag).toBe('NotFound');
  });

  test('gatewayProblem is the (status, message) pair Python raises', () => {
    expect(gatewayProblem(new BadRequest({ problem: 'healthQuery' }))).toEqual({
      status: 400,
      message: Gateway.GATEWAY_PROBLEM_MESSAGES.healthQuery,
    });
  });
});

// ---------------------------------------------------------------------------
// 2. Every tag renders exact status, headers and body bytes
// ---------------------------------------------------------------------------

describe('toResponse renders exact bytes', () => {
  renderedCases.forEach((entry) => {
    test(`${entry.label} → status, headers and body`, async () => {
      const observed = await observe(toResponse(entry.error));
      const expectedBody = canonicalProblemBytes(entry.error.message);

      expect(observed.status).toBe(entry.error.status);
      expect(observed.bytes).toEqual(expectedBody);
      expect(observed.text).toBe(`{"error":${JSON.stringify(entry.error.message)}}`);

      expect(observed.headers.get('content-type')).toBe(Gateway.GATEWAY_PROBLEM_CONTENT_TYPE);
      expect(observed.headers.get('content-length')).toBe(String(expectedBody.length));
      expect(observed.headers.get('cache-control')).toBe(Gateway.GATEWAY_PROBLEM_CACHE_CONTROL);
      expect(observed.headers.get('x-content-type-options')).toBe('nosniff');
      expect(observed.headers.get('referrer-policy')).toBe('no-referrer');

      // Nothing else. In particular: no CORS, no Vary, no Set-Cookie.
      const expectedKeys =
        entry.error._tag === 'MethodNotAllowed'
          ? ['allow', 'cache-control', 'content-length', 'content-type', 'referrer-policy', 'x-content-type-options']
          : ['cache-control', 'content-length', 'content-type', 'referrer-policy', 'x-content-type-options'];
      expect([...observed.headers.keys()].toSorted()).toEqual(expectedKeys);
    });
  });

  test('the 405 carries Allow: GET and the 30 bytes Python puts on the wire', async () => {
    const observed = await observe(toResponse(new MethodNotAllowed()));
    expect(observed.status).toBe(405);
    expect(observed.headers.get('allow')).toBe(Gateway.GATEWAY_METHOD_NOT_ALLOWED_ALLOW);
    expect(observed.headers.get('allow')).toBe('GET');
    // Observed on the Python wire: BODY[30] b'{"error":"method not allowed"}'.
    expect(observed.text).toBe('{"error":"method not allowed"}');
    expect(observed.bytes.length).toBe(30);
  });

  test('no other class carries Allow', async () => {
    const observed = await Promise.all(
      renderedCases
        .filter((entry) => entry.error._tag !== 'MethodNotAllowed')
        .map((entry) => observe(toResponse(entry.error))),
    );
    expect(observed.filter((response) => response.headers.has('allow'))).toEqual([]);
  });

  test('the body is canonical JSON, not JSON.stringify with spaces', async () => {
    const observed = await observe(toResponse(new NotFound({ problem: 'notFound' })));
    expect(observed.text).toBe('{"error":"not found"}');
    expect(observed.text).not.toContain(': ');
  });
});

// ---------------------------------------------------------------------------
// 3. UpstreamHttpError: 3xx is refused, everything else is relayed
// ---------------------------------------------------------------------------

describe('UpstreamHttpError follows wire upstreamProblem', () => {
  const redirects = [300, 301, 302, 307, 399];
  const relayed = [400, 404, 405, 409, 418, 500, 503];

  redirects.forEach((upstreamStatus) => {
    test(`upstream ${String(upstreamStatus)} → 502 upstream redirects are not allowed`, async () => {
      const observed = await observe(toResponse(new UpstreamHttpError({ upstreamStatus })));
      expect(observed.status).toBe(502);
      expect(observed.text).toBe('{"error":"upstream redirects are not allowed"}');
    });
  });

  relayed.forEach((upstreamStatus) => {
    test(`upstream ${String(upstreamStatus)} → itself, templated`, async () => {
      const observed = await observe(toResponse(new UpstreamHttpError({ upstreamStatus })));
      expect(observed.status).toBe(upstreamStatus);
      expect(observed.text).toBe(
        `{"error":"upstream returned HTTP ${String(upstreamStatus)}"}`,
      );
    });
  });

  test('a 2xx never reaches here as a problem, but if it did it is relayed verbatim', async () => {
    // Guards the boundary arithmetic: 299 is not a redirect, 300 is.
    const observed = await observe(toResponse(new UpstreamHttpError({ upstreamStatus: 299 })));
    expect(observed.status).toBe(299);
    expect(observed.text).toBe('{"error":"upstream returned HTTP 299"}');
  });
});

// ---------------------------------------------------------------------------
// 4. Nothing leaks
// ---------------------------------------------------------------------------

describe('private detail never reaches a body', () => {
  const privatePath = '/private/runs/game_secret/decisions.jsonl';

  test('a defect renders the generic 500 with no stack, message or path', async () => {
    const exit = await Effect.runPromiseExit(
      silently(respondGateway(Effect.die(new Error(`corrupt: ${privatePath}`)))),
    );
    expect(Exit.isSuccess(exit)).toBe(true);
    const observed = await observe(
      Exit.isSuccess(exit) ? exit.value : HttpServerResponse.empty({ status: 0 }),
    );

    expect(observed.status).toBe(500);
    expect(observed.text).toBe(
      '{"error":"the replay gateway encountered an internal error"}',
    );
    expect(observed.text).toBe(
      `{"error":${JSON.stringify(Gateway.GATEWAY_PROBLEM_MESSAGES.internalError)}}`,
    );
    expect(observed.text).not.toContain('private');
    expect(observed.text).not.toContain('decisions');
    expect(observed.text).not.toContain('corrupt');
    expect(observed.text).not.toContain('Error');
    expect(observed.text).not.toContain('at ');
  });

  test('a thrown non-Error defect is just as generic', async () => {
    const exit = await Effect.runPromiseExit(
      silently(
        respondGateway(
          Effect.sync(() => {
            throw { token: 'super-secret-owner-token' };
          }),
        ),
      ),
    );
    expect(Exit.isSuccess(exit)).toBe(true);
    const observed = await observe(
      Exit.isSuccess(exit) ? exit.value : HttpServerResponse.empty({ status: 0 }),
    );
    expect(observed.status).toBe(500);
    expect(observed.text).not.toContain('token');
  });

  test('a declared error carrying a private cause renders only its catalogue message', async () => {
    const observed = await observe(
      toResponse(
        new ArchiveUnavailable({
          problem: 'replayTelemetryUnavailable',
          cause: new Error(`private corrupt details at ${privatePath}`),
        }),
      ),
    );
    expect(observed.status).toBe(503);
    expect(observed.text).toBe('{"error":"replay telemetry is temporarily unavailable"}');
    expect(observed.text).not.toContain('private');
  });

  test('an unencodable message downgrades to the 500 body instead of escaping', () => {
    // Unreachable from the taxonomy — every catalogue message is ASCII — but the
    // renderer must be total. Python's `.encode("utf-8")` raises here, and the
    // request ends as a 500 (problem.ts trap T1).
    const rendered = renderProblemBody({ status: 404, message: '\uD800' });
    expect(rendered.status).toBe(500);
    expect(new TextDecoder().decode(rendered.body)).toBe(
      '{"error":"the replay gateway encountered an internal error"}',
    );
  });

  test('an encodable non-ASCII message is NOT escaped (ensure_ascii=False)', () => {
    const rendered = renderProblemBody({ status: 400, message: 'héllo → ok' });
    expect(rendered.status).toBe(400);
    expect(new TextDecoder().decode(rendered.body)).toBe('{"error":"héllo → ok"}');
  });
});

// ---------------------------------------------------------------------------
// 5. The router edge
// ---------------------------------------------------------------------------

describe('the catch pair is the only response site', () => {
  test('a declared failure becomes a response, not a failed effect', async () => {
    const exit = await Effect.runPromiseExit(
      catchGatewayErrors(Effect.fail(new NotFound({ problem: 'gameNotFound' }))),
    );
    expect(Exit.isSuccess(exit)).toBe(true);
    const observed = await observe(
      Exit.isSuccess(exit) ? exit.value : HttpServerResponse.empty({ status: 0 }),
    );
    expect(observed.status).toBe(404);
    expect(observed.text).toBe('{"error":"game not found"}');
  });

  test('a success passes through untouched', async () => {
    const response = gatewayJsonResponse({
      status: 200,
      body: new TextEncoder().encode('{"ok":true}'),
      cacheControl: Gateway.GATEWAY_PROBLEM_CACHE_CONTROL,
    });
    const exit = await Effect.runPromiseExit(
      silently(respondGateway(Effect.succeed(response))),
    );
    expect(Exit.isSuccess(exit)).toBe(true);
    const observed = await observe(
      Exit.isSuccess(exit) ? exit.value : HttpServerResponse.empty({ status: 0 }),
    );
    expect(observed.status).toBe(200);
    expect(observed.text).toBe('{"ok":true}');
    expect(observed.headers.get('content-type')).toBe(Gateway.GATEWAY_PROBLEM_CONTENT_TYPE);
    expect(observed.headers.get('cache-control')).toBe('no-store');
    expect(observed.headers.get('x-content-type-options')).toBe('nosniff');
    expect(observed.headers.get('referrer-policy')).toBe('no-referrer');
  });

  test('interruption stays interruption — a cancelled fiber is not a 500', async () => {
    const exit = await Effect.runPromiseExit(silently(respondGateway(Effect.interrupt)));
    expect(Exit.isInterrupted(exit)).toBe(true);
  });

  test('a defect raised while rendering is still caught by the outer clause', async () => {
    const exit = await Effect.runPromiseExit(
      silently(
        respondGateway(
          Effect.flatMap(Effect.fail(new UpstreamTooLarge()), () => Effect.void).pipe(
            Effect.flatMap(() => Effect.die('unreachable')),
          ),
        ),
      ),
    );
    expect(Exit.isSuccess(exit)).toBe(true);
    const observed = await observe(
      Exit.isSuccess(exit) ? exit.value : HttpServerResponse.empty({ status: 0 }),
    );
    // The failure wins: it is caught first, exactly as `except GatewayProblem`
    // precedes `except Exception`.
    expect(observed.status).toBe(502);
    expect(observed.text).toBe('{"error":"the upstream JSON response is too large"}');
  });
});

// ---------------------------------------------------------------------------
// 6. The security pair is available to the paths this module does not build
// ---------------------------------------------------------------------------

describe('withSecurityHeaders', () => {
  test('adds nosniff and no-referrer to a response built elsewhere', async () => {
    const observed = await observe(
      withSecurityHeaders(
        HttpServerResponse.uint8Array(new Uint8Array([137, 80, 78, 71]), {
          contentType: Gateway.ARCHIVE_FRAME_CONTENT_TYPE,
          headers: { 'cache-control': Gateway.ARCHIVE_BINARY_CACHE_CONTROL },
        }),
      ),
    );
    expect(observed.headers.get('content-type')).toBe('image/png');
    expect(observed.headers.get('cache-control')).toBe(
      'public, max-age=31536000, immutable',
    );
    // The literals, not the constant.  Asserting the observed header against
    // the very binding under test proves plumbing and not value: setting
    // `constants.ts`'s pair to `'sniff'` / `'unsafe-url'` would leave this
    // green.  The constant is compared too, so the two cannot part company.
    expect(observed.headers.get('x-content-type-options')).toBe('nosniff');
    expect(observed.headers.get('referrer-policy')).toBe('no-referrer');
    expect(GATEWAY_SECURITY_HEADERS).toEqual({
      'x-content-type-options': 'nosniff',
      'referrer-policy': 'no-referrer',
    });
  });
});
