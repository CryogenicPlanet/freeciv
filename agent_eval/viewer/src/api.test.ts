import { afterEach, describe, expect, it, vi } from 'vitest'
import { fetchBoard, fetchEvents, fetchGames, fetchWatchWithOptionalReplay } from './api'
import { mockWatch } from './mock'
import { requiredValue } from './test-support'

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('arena game index', () => {
  it('loads the same-origin public index under a proxy prefix', async () => {
    const payload = { schema_version: 1, games: [] }
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(new Response(JSON.stringify(payload), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    }))
    vi.stubGlobal('fetch', fetchMock)

    await expect(fetchGames({ prefix: '/freeciv' })).resolves.toEqual(payload)
    expect(fetchMock).toHaveBeenCalledWith('/freeciv/v1/games', {
      cache: 'no-store',
    })
  })

  it('rejects malformed successful payloads at the JSON boundary', async () => {
    vi.stubGlobal('fetch', vi.fn<typeof fetch>().mockResolvedValue(new Response(
      JSON.stringify({ schema_version: 1, games: [{ state: 'running' }] }),
      { status: 200 },
    )))

    await expect(fetchGames({ prefix: '' })).rejects.toThrow('game_id')
  })

  it('surfaces an index error while the picker can retain manual entry', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(
      JSON.stringify({ error: 'game index unavailable' }),
      { status: 404, statusText: 'Not Found' },
    )))

    await expect(fetchGames({ prefix: '' })).rejects.toThrow('game index unavailable')
  })
})

describe('semantic board endpoint', () => {
  it('loads exactly one selected turn through the same-origin prefix', async () => {
    const payload = {
      schema_version: 1,
      game_id: 'game_abcdefghijklmnop',
      turn: 42,
      width: 1,
      height: 1,
      topology: 'ISO|HEX',
      wrap: '',
      terrain_catalog: [{ code: 'g', name: 'Grassland' }],
      terrain_rows: ['g'],
      altitude_rows: ['0'],
      owner_rows: ['-:1'],
      extras_catalog: [],
      extra_layers: [],
      cities: [],
      unit_stacks: [],
      players: [],
    }
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(new Response(JSON.stringify(payload), { status: 200 }))
    vi.stubGlobal('fetch', fetchMock)

    await expect(fetchBoard({
      prefix: '/freeciv', gameId: 'game_abcdefghijklmnop',
    }, 42)).resolves.toEqual(payload)
    expect(fetchMock).toHaveBeenCalledWith(
      '/freeciv/v1/games/game_abcdefghijklmnop/board.json?turn=42',
      { cache: 'no-store' },
    )
  })
})

describe('derived game event log endpoint', () => {
  it('loads the whole log in one same-origin request', async () => {
    const payload = {
      schema_version: 1,
      game_id: 'game_abcdefghijklmnop',
      available: true,
      events: [],
      event_counts: {},
      total_events: 0,
      truncated: false,
      omitted_counts: {},
      min_included_weight: 0,
      last_turn: 0,
    }
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(new Response(JSON.stringify(payload), { status: 200 }))
    vi.stubGlobal('fetch', fetchMock)

    await expect(fetchEvents({
      prefix: '/freeciv', gameId: 'game_abcdefghijklmnop',
    })).resolves.toEqual(payload)
    expect(fetchMock).toHaveBeenCalledWith(
      '/freeciv/v1/games/game_abcdefghijklmnop/events.json',
      { cache: 'no-store' },
    )
  })

  it('rejects so the panel can keep its turn timeline on an older gateway', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(
      JSON.stringify({ error: 'not found' }), { status: 404, statusText: 'Not Found' },
    )))

    await expect(fetchEvents({ prefix: '', gameId: 'game_abcdefghijklmnop' }))
      .rejects.toThrow('not found')
  })
})

describe('legacy watch compatibility', () => {
  it('keeps watch data when replay is missing and derives frame turns', async () => {
    const legacyWatch = {
      ...mockWatch,
      frames: mockWatch.frames.map(({ turn: _turn, ...frame }) => frame),
    }
    vi.stubGlobal('fetch', vi.fn<(input: string) => Promise<Response>>().mockImplementation((input) => {
      if (input.includes('/watch.json')) {
        return Promise.resolve(new Response(JSON.stringify(legacyWatch), { status: 200 }))
      }
      return Promise.resolve(new Response(
        JSON.stringify({ error: 'not found' }),
        { status: 404, statusText: 'Not Found' },
      ))
    }))

    const load = await fetchWatchWithOptionalReplay(
      { prefix: '', gameId: mockWatch.game.game_id }, 0,
    )
    expect(load.watch.game.game_id).toBe(mockWatch.game.game_id)
    expect(requiredValue(load.watch.frames[0], 'loaded watch frame').turn).toBe(3)
    expect(load.replay).toBeNull()
    expect(load.replayError).toBe('not found')
    expect(load.replayUnavailable).toBe(true)
  })
})
