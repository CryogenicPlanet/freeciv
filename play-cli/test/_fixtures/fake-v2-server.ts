import { Effect } from 'effect';
import type { JsonValue } from 'src/schema/primitives';

/**
 * The fake supervisor every unit's tests talk to.
 *
 * It is a `fetch` implementation, not an HTTP listener: the port's only network
 * surface is `services/http.ts`, so injecting `fetch` gives byte-level control
 * over statuses, headers and bodies without binding a port — which also means a
 * test suite never races another agent's live server.
 */
export interface RecordedRequest {
  readonly method: string;
  readonly url: string;
  readonly headers: Readonly<Record<string, string>>;
  readonly body: string | null;
}

export interface FakeRoute {
  readonly status?: number;
  readonly body: JsonValue;
  readonly headers?: Readonly<Record<string, string>>;
  /** Seconds of simulated latency, for the concurrency-determinism tests. */
  readonly delayS?: number;
}

export interface RecordingServer {
  readonly fetch: typeof fetch;
  readonly requests: ReadonlyArray<RecordedRequest>;
}

type FakePlan = ReadonlyArray<FakeRoute> | ReadonlyMap<string, FakeRoute>;
type FetchInput = Parameters<typeof fetch>[0];
type FetchArguments = Parameters<typeof fetch>;

const completeFetch = (
  handler: (...args: FetchArguments) => ReturnType<typeof fetch>
): typeof fetch => Object.assign(handler, { preconnect: fetch.preconnect });

export const jsonResponse = (body: JsonValue, status = 200): Response =>
  new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  });

const urlOf = (input: FetchInput): string =>
  input instanceof Request ? input.url : new URL(input).href;

const requestBody = (init: RequestInit | undefined): Effect.Effect<string | null> => {
  const body = init?.body;
  return body === undefined || body === null
    ? Effect.succeed(null)
    : Effect.promise(() => new Response(body).text());
};

const headerRecord = (init: RequestInit | undefined) => {
  const out: Record<string, string> = {};
  const headers = init?.headers;
  if (headers === undefined) return out;
  new Headers(headers).forEach((value, name) => {
    out[name.toLowerCase()] = value;
  });
  return out;
};

const isRouteQueue = (plan: FakePlan): plan is ReadonlyArray<FakeRoute> => Array.isArray(plan);

const responseFor = (route: FakeRoute): Response =>
  new Response(JSON.stringify(route.body), {
    status: route.status ?? 200,
    headers: { 'content-type': 'application/json', ...route.headers },
  });

/**
 * Serve a queue of responses, or one response per matching URL substring.
 *
 * A queue drains in order and is how a busy-retry test proves the second GET
 * happened; a map is how a multi-route command test stays readable.
 */
export const fakeFetch = (plan: FakePlan): typeof fetch => {
  const queue: Array<FakeRoute> = isRouteQueue(plan) ? [...plan] : [];
  const routes: ReadonlyMap<string, FakeRoute> = isRouteQueue(plan) ? new Map() : plan;
  return completeFetch((input, _init) =>
    Effect.runPromise(
      Effect.gen(function* () {
        const url = urlOf(input);
        const route = isRouteQueue(plan)
          ? queue.shift()
          : [...routes].find(([match]) => url.includes(match))?.[1];
        if (route === undefined) {
          return jsonResponse(
            { error: { code: 'not_implemented', message: `no route for ${url}` } },
            404
          );
        }
        const delayS = route.delayS ?? 0;
        if (delayS > 0) yield* Effect.sleep(`${delayS} seconds`);
        return responseFor(route);
      })
    )
  );
};

/** The same, plus a log of everything that was sent. */
export const recordingFetch = (plan: FakePlan): RecordingServer => {
  const requests: RecordedRequest[] = [];
  const inner = fakeFetch(plan);
  const wrapped = completeFetch((input, init) =>
    Effect.runPromise(
      Effect.gen(function* () {
        const url = urlOf(input);
        requests.push({
          method: init?.method ?? 'GET',
          url,
          headers: headerRecord(init),
          body: yield* requestBody(init),
        });
        return yield* Effect.promise(() => inner(input, init));
      })
    )
  );
  return { fetch: wrapped, requests };
};
