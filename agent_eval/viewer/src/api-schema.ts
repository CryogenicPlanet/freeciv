import { z } from 'zod'
import type {
  BoardResponse,
  GameEventsResponse,
  GamesIndexResponse,
  ReplayResponse,
  WatchResponse,
} from './types'

const GAME_PLACE_SCHEMA = z.object({
  place: z.number(),
  seat_id: z.string(),
  player_name: z.string(),
  controller: z.string(),
  joined: z.boolean(),
  controller_label: z.string().nullable().optional(),
  controller_type: z.string().nullable().optional(),
  model: z.string().nullable().optional(),
  ai_difficulty: z.string().nullable().optional(),
  player_color: z.string(),
})

const LEADERBOARD_ENTRY_SCHEMA = z.object({
  rank: z.number(),
  score: z.number(),
  score_turn: z.number().nullable().optional(),
  place: z.number(),
  seat_id: z.string(),
  player_name: z.string(),
  player_color: z.string(),
  controller_label: z.string(),
  controller_type: z.string(),
  model: z.string().nullable().optional(),
})

const MATCH_VICTORY_SCHEMA = z.object({
  code: z.string(),
  label: z.string(),
  winners: z.array(z.string()),
  turn: z.number().nullable().optional(),
  year: z.number().nullable().optional(),
})

const MATCH_OUTCOME_SCHEMA = z.object({
  status: z.string(),
  summary: z.string(),
  leaders: z.array(z.string()).optional(),
  margin: z.number().nullable().optional(),
  score_turn: z.number().nullable().optional(),
  victory: MATCH_VICTORY_SCHEMA.nullable().optional(),
})

const GAME_STATUS_SCHEMA = z.object({
  schema_version: z.number(),
  game_id: z.string(),
  state: z.string(),
  benchmark_valid: z.boolean().nullable(),
  mode: z.string(),
  control_protocol: z.string().nullable().optional(),
  places: z.number(),
  max_agents: z.number(),
  joined_agents: z.number(),
  turns: z.number(),
  current_turn: z.number().nullable(),
  objective: z.string(),
  timing_mode: z.string().nullable().optional(),
  action_timeout_s: z.number().nullable().optional(),
  error: z.string().nullable(),
  invalid_reasons: z.array(z.string()),
  resolved_places: z.array(GAME_PLACE_SCHEMA),
  leaderboard: z.array(LEADERBOARD_ENTRY_SCHEMA),
  outcome: MATCH_OUTCOME_SCHEMA,
  replay_url: z.string().optional(),
  created_at: z.number().nullable().optional(),
  finished_at: z.number().nullable().optional(),
})

const GAME_SUMMARY_SCHEMA = z.object({
  game_id: z.string(),
  state: z.string(),
  benchmark_valid: z.boolean().nullable(),
  mode: z.string(),
  control_protocol: z.string().nullable().optional(),
  places: z.number(),
  max_agents: z.number(),
  joined_agents: z.number(),
  turns: z.number(),
  current_turn: z.number().nullable(),
  resolved_places: z.array(GAME_PLACE_SCHEMA),
  leaderboard: z.array(LEADERBOARD_ENTRY_SCHEMA),
  outcome: MATCH_OUTCOME_SCHEMA,
  timing_mode: z.string().nullable().optional(),
  action_timeout_s: z.number().nullable().optional(),
  created_at: z.number().nullable().optional(),
  finished_at: z.number().nullable().optional(),
  watch_path: z.string().optional(),
})

export const GAMES_INDEX_RESPONSE_SCHEMA: z.ZodType<GamesIndexResponse> = z.object({
  schema_version: z.number(),
  games: z.array(GAME_SUMMARY_SCHEMA),
})

const TIMELINE_ENTRY_SCHEMA = z.object({
  turn: z.number(),
  year: z.number().nullable().optional(),
  responded_seats: z.array(z.string()).optional(),
  timed_out_seats: z.array(z.string()).optional(),
  resolved_at: z.number().optional(),
})

const MAP_PLAYER_SCHEMA = z.object({
  player_id: z.number(),
  player_name: z.string(),
  player_color: z.string(),
  seat_id: z.string().nullable().optional(),
  place: z.number().nullable().optional(),
  controller_label: z.string().nullable().optional(),
  controller_type: z.string().nullable().optional(),
  nation: z.string().nullable().optional(),
  scored: z.boolean().optional(),
})

const REPLAY_FRAME_SCHEMA = z.object({
  index: z.number(),
  turn: z.number().nullable().optional(),
  source_name: z.string(),
  png_url: z.string(),
  map_players: z.array(MAP_PLAYER_SCHEMA).optional(),
})

export const WATCH_RESPONSE_SCHEMA: z.ZodType<WatchResponse> = z.object({
  schema_version: z.number(),
  label: z.string(),
  game: GAME_STATUS_SCHEMA,
  timeline: z.array(TIMELINE_ENTRY_SCHEMA),
  frames: z.array(REPLAY_FRAME_SCHEMA).optional().default([]),
  replay: z.object({ url: z.string(), available: z.boolean() }).optional(),
  video: z.object({
    available: z.boolean(),
    url: z.string(),
    kind: z.string(),
  }).optional(),
})

const TECHNOLOGY_SCHEMA = z.object({
  id: z.number(),
  rule_name: z.string(),
  name: z.string(),
  cost_base: z.number(),
  requires: z.array(z.number()).optional(),
  depth: z.number().optional(),
})

const TECHNOLOGY_CATALOG_SCHEMA = z.object({
  schema_version: z.number().optional(),
  technologies: z.array(TECHNOLOGY_SCHEMA),
})

const RESEARCH_STATE_SCHEMA = z.object({
  tech_id: z.number().nullable(),
  name: z.string(),
  bulbs: z.number(),
  cost: z.number(),
})

const REPLAY_PLAYER_SCHEMA = z.object({
  seat_id: z.string(),
  place: z.number().nullable().optional(),
  player_id: z.number(),
  player_name: z.string(),
  player_color: z.string().nullable().optional(),
  controller_label: z.string().nullable().optional(),
  controller_type: z.string().nullable().optional(),
  model: z.string().nullable().optional(),
  ai_difficulty: z.string().nullable().optional(),
  nation: z.string(),
  government: z.string(),
  alive: z.boolean(),
  score: z.number(),
  cities: z.number(),
  citizens: z.number().optional(),
  population: z.number().optional(),
  units: z.number(),
  gold: z.number(),
  culture: z.number(),
  known_tech_ids: z.array(z.number()),
  gained_tech_ids: z.array(z.number()),
  lost_tech_ids: z.array(z.number()),
  research: RESEARCH_STATE_SCHEMA,
  future_techs: z.number(),
  scored: z.boolean().optional(),
})

const REPLAY_SNAPSHOT_SCHEMA = z.object({
  schema_version: z.number().optional(),
  game_id: z.string().optional(),
  turn: z.number(),
  year: z.number(),
  players: z.array(REPLAY_PLAYER_SCHEMA),
})

const REPLAY_WARNING_SCHEMA = z.object({
  turn: z.number().nullable().optional(),
  message: z.string(),
})

export const REPLAY_RESPONSE_SCHEMA: z.ZodType<ReplayResponse> = z.object({
  schema_version: z.number(),
  game_id: z.string(),
  available: z.boolean(),
  catalog: TECHNOLOGY_CATALOG_SCHEMA.nullable().optional(),
  snapshots: z.array(REPLAY_SNAPSHOT_SCHEMA),
  next_after_turn: z.number(),
  has_more: z.boolean(),
  complete: z.boolean().optional(),
  replay_warnings: z.array(REPLAY_WARNING_SCHEMA).optional(),
  warnings: z.array(REPLAY_WARNING_SCHEMA).optional(),
})

const GAME_EVENT_SCHEMA = z.object({
  turn: z.number(),
  kind: z.string(),
  summary: z.string(),
  actors: z.array(z.string()),
  weight: z.number(),
  data: z.record(z.string(), z.json()),
})

export const GAME_EVENTS_RESPONSE_SCHEMA: z.ZodType<GameEventsResponse> = z.object({
  schema_version: z.number(),
  game_id: z.string(),
  available: z.boolean(),
  events: z.array(GAME_EVENT_SCHEMA),
  event_counts: z.record(z.string(), z.number()),
  total_events: z.number(),
  truncated: z.boolean(),
  omitted_counts: z.record(z.string(), z.number()),
  min_included_weight: z.number(),
  last_turn: z.number(),
  complete: z.boolean().optional(),
  event_warnings: z.array(REPLAY_WARNING_SCHEMA).optional(),
})

const BOARD_PLAYER_SCHEMA = z.object({
  player_id: z.number(),
  player_name: z.string(),
  player_color: z.string().nullable().optional(),
  seat_id: z.string().nullable().optional(),
  place: z.number().nullable().optional(),
  controller_label: z.string().nullable().optional(),
  controller_type: z.string().nullable().optional(),
  model: z.string().nullable().optional(),
  nation: z.string().nullable().optional(),
  scored: z.boolean().optional(),
})

export const BOARD_RESPONSE_SCHEMA: z.ZodType<BoardResponse> = z.object({
  schema_version: z.number(),
  game_id: z.string(),
  turn: z.number(),
  width: z.number(),
  height: z.number(),
  topology: z.string(),
  wrap: z.string(),
  terrain_catalog: z.array(z.object({ code: z.string(), name: z.string() })),
  terrain_rows: z.array(z.string()),
  altitude_rows: z.array(z.string()),
  owner_rows: z.array(z.string()),
  extras_catalog: z.array(z.object({ id: z.number(), name: z.string() })),
  extra_layers: z.array(z.array(z.string())),
  cities: z.array(z.object({
    id: z.number(),
    x: z.number(),
    y: z.number(),
    player_id: z.number(),
    name: z.string(),
    size: z.number(),
    capital: z.boolean(),
  })),
  unit_stacks: z.array(z.object({
    x: z.number(),
    y: z.number(),
    player_id: z.number(),
    count: z.number(),
    types: z.array(z.object({ name: z.string(), count: z.number() })),
  })),
  players: z.array(BOARD_PLAYER_SCHEMA),
})

export const ERROR_PAYLOAD_SCHEMA = z.object({ error: z.string() })
