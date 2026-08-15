import type { ZodType } from 'zod'
import {
  BOARD_RESPONSE_SCHEMA,
  ERROR_PAYLOAD_SCHEMA,
  GAME_EVENTS_RESPONSE_SCHEMA,
  GAMES_INDEX_RESPONSE_SCHEMA,
  REPLAY_RESPONSE_SCHEMA,
  WATCH_RESPONSE_SCHEMA,
} from './api-schema'
import { apiUrl, arenaApiUrl } from './route'
import type {
  ArenaRouteContext,
  BoardResponse,
  GameEventsResponse,
  GamesIndexResponse,
  ReplaySnapshot,
  ReplayWarning,
  RouteContext,
  TechnologyCatalog,
  WatchResponse,
} from './types'

export class HTTPError extends Error {
  constructor(readonly status: number, message: string) {
    super(message)
    this.name = 'HTTPError'
  }
}

async function fetchJson<T>(
  url: string,
  schema: ZodType<T>,
  signal?: AbortSignal,
): Promise<T> {
  const request: RequestInit = { cache: 'no-store' }
  if (signal) request.signal = signal
  const response = await fetch(url, request)
  if (!response.ok) {
    let detail = `${response.status} ${response.statusText}`
    try {
      const payload = ERROR_PAYLOAD_SCHEMA.safeParse(await response.json())
      if (payload.success) detail = payload.data.error
    } catch {
      // Keep the stable HTTP status when a proxy returns non-JSON.
    }
    throw new HTTPError(response.status, detail)
  }
  return schema.parse(await response.json())
}

export function fetchGames(
  route: ArenaRouteContext,
  signal?: AbortSignal,
): Promise<GamesIndexResponse> {
  return fetchJson(
    arenaApiUrl(route, '/v1/games'), GAMES_INDEX_RESPONSE_SCHEMA, signal,
  )
}

export function fetchWatch(
  route: RouteContext,
  signal?: AbortSignal,
): Promise<WatchResponse> {
  return fetchJson(
    apiUrl(route, `/v1/games/${encodeURIComponent(route.gameId)}/watch.json`),
    WATCH_RESPONSE_SCHEMA,
    signal,
  ).then((payload) => ({
    ...payload,
    frames: (payload.frames ?? []).map((frame) => ({
      ...frame,
      turn: legacyFrameTurn(frame),
    })),
  }))
}

export function fetchEvents(
  route: RouteContext,
  signal?: AbortSignal,
): Promise<GameEventsResponse> {
  return fetchJson(
    apiUrl(route, `/v1/games/${encodeURIComponent(route.gameId)}/events.json`),
    GAME_EVENTS_RESPONSE_SCHEMA,
    signal,
  )
}

export function fetchBoard(
  route: RouteContext,
  turn: number,
  signal?: AbortSignal,
): Promise<BoardResponse> {
  const query = new URLSearchParams({ turn: String(Math.max(0, Math.trunc(turn))) })
  return fetchJson(
    apiUrl(
      route,
      `/v1/games/${encodeURIComponent(route.gameId)}/board.json?${query}`,
    ),
    BOARD_RESPONSE_SCHEMA,
    signal,
  )
}

export function legacyFrameTurn(frame: {
  turn?: number | null | undefined
  source_name: string
}): number | null {
  if (frame.turn != null && Number.isFinite(frame.turn) && frame.turn >= 0) {
    return Math.trunc(frame.turn)
  }
  const match = frame.source_name.match(/^turn-(\d+)(?:-|\.|$)/)
  const encodedTurn = match?.[1]
  return encodedTurn === undefined ? null : Number(encodedTurn)
}

export interface ReplayBatch {
  snapshots: ReplaySnapshot[]
  catalog: TechnologyCatalog | undefined
  warnings: ReplayWarning[]
  available: boolean
  complete: boolean
}

export async function fetchReplaySince(
  route: RouteContext,
  afterTurn: number,
  signal?: AbortSignal,
): Promise<ReplayBatch> {
  const snapshots: ReplaySnapshot[] = []
  const warnings: ReplayWarning[] = []
  let catalog: TechnologyCatalog | undefined
  let cursor = afterTurn
  let available = false
  let complete = false

  for (let page = 0; page < 1000; page += 1) {
    const query = new URLSearchParams({
      after_turn: String(cursor),
      limit: '250',
    })
    const payload = await fetchJson(
      apiUrl(
        route,
        `/v1/games/${encodeURIComponent(route.gameId)}/replay.json?${query}`,
      ),
      REPLAY_RESPONSE_SCHEMA,
      signal,
    )
    available ||= payload.available
    complete ||= Boolean(payload.complete)
    catalog = payload.catalog ?? catalog
    snapshots.push(...payload.snapshots)
    warnings.push(...(payload.replay_warnings ?? payload.warnings ?? []))
    if (!payload.has_more) break
    if (payload.next_after_turn <= cursor) {
      throw new Error('Replay pagination did not advance')
    }
    cursor = payload.next_after_turn
  }

  return { snapshots, catalog, warnings, available, complete }
}

export interface OptionalReplayLoad {
  watch: WatchResponse
  replay: ReplayBatch | null
  replayError: string | null
  replayUnavailable: boolean
}

export async function fetchWatchWithOptionalReplay(
  route: RouteContext,
  afterTurn: number,
  signal?: AbortSignal,
): Promise<OptionalReplayLoad> {
  const [watch, replay] = await Promise.allSettled([
    fetchWatch(route, signal),
    fetchReplaySince(route, afterTurn, signal),
  ])
  if (watch.status === 'rejected') throw watch.reason
  if (replay.status === 'fulfilled') {
    return {
      watch: watch.value,
      replay: replay.value,
      replayError: null,
      replayUnavailable: false,
    }
  }
  if (signal?.aborted) throw replay.reason
  return {
    watch: watch.value,
    replay: null,
    replayError: replay.reason instanceof Error
      ? replay.reason.message
      : 'Replay telemetry is unavailable',
    replayUnavailable: replay.reason instanceof HTTPError && replay.reason.status === 404,
  }
}
