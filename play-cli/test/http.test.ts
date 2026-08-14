/**
 * The HTTP floor.
 *
 * `serviceUrl` is the credential boundary — everything it accepts is an origin
 * a bearer token will be sent to — so its refusals get more attention than its
 * successes.
 */
import { describe, expect, test } from 'bun:test';
import { Effect, Either } from 'effect';
import { encodeRequestBody, httpFor, serviceUrl, v1ErrorMessage } from 'src/services/http';
import { fakeFetch, recordingFetch } from 'test/_fixtures';
import { awaitTest } from 'test/_effect-test';

const run = <A, E>(effect: Effect.Effect<A, E>): Either.Either<A, E> =>
  Effect.runSync(Effect.either(effect));

const runPromise = <A, E>(effect: Effect.Effect<A, E>): Promise<Either.Either<A, E>> =>
  Effect.runPromise(Effect.either(effect));

describe('serviceUrl', () => {
  test('defaults to the loopback supervisor', () => {
    expect(run(serviceUrl(undefined, {}))).toEqual(Either.right('http://127.0.0.1:8765'));
  });

  test('lower-cases the origin and strips a trailing slash', () => {
    expect(run(serviceUrl('HTTP://Example.COM:8080/base/', {}))).toEqual(
      Either.right('http://example.com:8080/base')
    );
  });

  test('the environment supplies it when the argument does not', () => {
    expect(run(serviceUrl(undefined, { AGENT_EVAL_SERVICE_URL: 'https://host:443' }))).toEqual(
      Either.right('https://host:443')
    );
  });

  test.each([
    ['credentials', 'http://user:pass@host:8765'],
    ['a query', 'http://host:8765/?a=1'],
    ['a fragment', 'http://host:8765/#f'],
    ['a non-http scheme', 'ftp://host:8765'],
    ['a bad port', 'http://host:99999'],
  ])('refuses %s', (_label, url) => {
    const either = run(serviceUrl(url, {}));
    expect(Either.isLeft(either)).toBe(true);
  });
});

describe('request encoding', () => {
  test('bodies are sorted and compact, as urllib sent them', () => {
    expect(encodeRequestBody({ b: 1, a: 2 })).toBe('{"a":2,"b":1}');
  });

  awaitTest('the bearer token and content type ride on the request', function* (wait) {
    const recorder = recordingFetch([{ body: { ok: true } }]);
    const http = httpFor(recorder.fetch);
    yield* wait(
      runPromise(http.requestJson('POST', 'http://host/v1/x', { token: 'shhh', body: { a: 1 } }))
    );
    const sent = recorder.requests[0];
    expect(sent?.headers['authorization']).toBe('Bearer shhh');
    expect(sent?.headers['content-type']).toBe('application/json');
    expect(sent?.body).toBe('{"a":1}');
  });

  awaitTest('two bodies at once is a refusal, not a silent pick', function* (wait) {
    const http = httpFor(fakeFetch([{ body: {} }]));
    const either = yield* wait(
      runPromise(http.requestJsonResponse('POST', 'http://host/x', { body: {}, encodedBody: '{}' }))
    );
    expect(Either.isLeft(either)).toBe(true);
  });
});

describe('responses', () => {
  awaitTest('requestJsonResponse hands back the body of a 4xx', function* (wait) {
    const http = httpFor(fakeFetch([{ status: 409, body: { error: { code: 'conflict' } } }]));
    const either = yield* wait(runPromise(http.requestJsonResponse('GET', 'http://host/x')));
    expect(Either.isRight(either)).toBe(true);
    if (Either.isRight(either)) {
      expect(either.right.status).toBe(409);
      expect(either.right.value).toEqual({ error: { code: 'conflict' } });
    }
  });

  awaitTest('requestJson raises on a 4xx with the Python message shape', function* (wait) {
    const http = httpFor(
      fakeFetch([{ status: 400, body: { error: { message: 'bad turn', code: 'invalid_request' } } }])
    );
    const either = yield* wait(runPromise(http.requestJson('GET', 'http://host/x')));
    expect(Either.isLeft(either)).toBe(true);
    if (Either.isLeft(either)) expect(either.left.message).toBe('HTTP 400: bad turn');
  });

  awaitTest('a non-object JSON response is refused', function* (wait) {
    const http = httpFor(fakeFetch([{ body: [1, 2, 3] }]));
    const either = yield* wait(runPromise(http.requestJsonResponse('GET', 'http://host/x')));
    expect(Either.isLeft(either)).toBe(true);
    if (Either.isLeft(either)) {
      expect(either.left.message).toBe('the supervisor returned a non-object JSON response');
    }
  });

  test('v1ErrorMessage falls back to the code, then to the status', () => {
    expect(v1ErrorMessage(500, { error: { code: 'internal_error' } })).toBe(
      'HTTP 500: internal_error'
    );
    expect(v1ErrorMessage(503, {})).toBe('HTTP 503: HTTP 503');
  });
});
