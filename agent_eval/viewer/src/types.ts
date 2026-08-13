export type BenchmarkValidity = boolean | null

export interface GamePlace {
  place: number
  seat_id: string
  player_name: string
  controller: string
  joined: boolean
  controller_label?: string | null | undefined
  controller_type?: string | null | undefined
  model?: string | null | undefined
  /** The server AI level driving this place; absent on older payloads. */
  ai_difficulty?: string | null | undefined
  player_color: string
}

export interface LeaderboardEntry {
  rank: number
  score: number
  score_turn?: number | null | undefined
  place: number
  seat_id: string
  player_name: string
  player_color: string
  controller_label: string
  controller_type: string
  model?: string | null | undefined
}

export interface MatchVictory {
  code: string
  label: string
  winners: string[]
  turn?: number | null | undefined
  year?: number | null | undefined
}

export interface MatchOutcome {
  status: string
  summary: string
  leaders?: string[] | undefined
  margin?: number | null | undefined
  score_turn?: number | null | undefined
  victory?: MatchVictory | null | undefined
}

export interface GameStatus {
  schema_version: number
  game_id: string
  state: string
  benchmark_valid: BenchmarkValidity
  mode: string
  /** Absent on runs recorded before the field existed; those are strategic-v1. */
  control_protocol?: string | null | undefined
  places: number
  max_agents: number
  joined_agents: number
  turns: number
  current_turn: number | null
  objective: string
  timing_mode?: string | null | undefined
  action_timeout_s?: number | null | undefined
  error: string | null
  invalid_reasons: string[]
  resolved_places: GamePlace[]
  leaderboard: LeaderboardEntry[]
  outcome: MatchOutcome
  replay_url?: string | undefined
  created_at?: number | null | undefined
  finished_at?: number | null | undefined
}

export type GameSummary = Pick<GameStatus,
  | 'game_id'
  | 'state'
  | 'benchmark_valid'
  | 'mode'
  | 'control_protocol'
  | 'places'
  | 'max_agents'
  | 'joined_agents'
  | 'turns'
  | 'current_turn'
  | 'resolved_places'
  | 'leaderboard'
  | 'outcome'
  | 'timing_mode'
  | 'action_timeout_s'
> & {
  created_at?: number | null | undefined
  finished_at?: number | null | undefined
  watch_path?: string | undefined
}

export interface GamesIndexResponse {
  schema_version: number
  games: GameSummary[]
}

export interface TurnTimelineEntry {
  turn: number
  year?: number | null | undefined
  responded_seats?: string[] | undefined
  timed_out_seats?: string[] | undefined
  resolved_at?: number | undefined
}

export interface MapPlayer {
  player_id: number
  player_name: string
  player_color: string
  seat_id?: string | null | undefined
  place?: number | null | undefined
  controller_label?: string | null | undefined
  controller_type?: string | null | undefined
  nation?: string | null | undefined
  scored?: boolean | undefined
}

export interface ReplayFrame {
  index: number
  turn?: number | null | undefined
  source_name: string
  png_url: string
  map_players?: MapPlayer[] | undefined
}

export interface WatchResponse {
  schema_version: number
  label: string
  game: GameStatus
  timeline: TurnTimelineEntry[]
  frames: ReplayFrame[]
  replay?: { url: string; available: boolean } | undefined
  video?: { available: boolean; url: string; kind: string } | undefined
}

export interface Technology {
  id: number
  rule_name: string
  name: string
  cost_base: number
  requires?: number[] | undefined
  depth?: number | undefined
}

export interface TechnologyCatalog {
  schema_version?: number | undefined
  technologies: Technology[]
}

export interface ResearchState {
  tech_id: number | null
  name: string
  bulbs: number
  cost: number
}

export interface ReplayPlayer {
  seat_id: string
  place?: number | null | undefined
  player_id: number
  player_name: string
  player_color?: string | null | undefined
  controller_label?: string | null | undefined
  controller_type?: string | null | undefined
  model?: string | null | undefined
  /** The server AI level driving this player; absent on older payloads. */
  ai_difficulty?: string | null | undefined
  nation: string
  government: string
  alive: boolean
  score: number
  cities: number
  citizens?: number | undefined
  population?: number | undefined
  units: number
  gold: number
  culture: number
  known_tech_ids: number[]
  gained_tech_ids: number[]
  lost_tech_ids: number[]
  research: ResearchState
  future_techs: number
  scored?: boolean | undefined
}

export interface ReplaySnapshot {
  schema_version?: number | undefined
  game_id?: string | undefined
  turn: number
  year: number
  players: ReplayPlayer[]
}

export interface ReplayWarning {
  turn?: number | null | undefined
  message: string
}

export interface ReplayResponse {
  schema_version: number
  game_id: string
  available: boolean
  catalog?: TechnologyCatalog | null | undefined
  snapshots: ReplaySnapshot[]
  next_after_turn: number
  has_more: boolean
  complete?: boolean | undefined
  replay_warnings?: ReplayWarning[] | undefined
  warnings?: ReplayWarning[] | undefined
}

/** One derived match event: what happened, on which turn, to whom. */
export type JsonValue = boolean | JsonObject | JsonValue[] | null | number | string

export interface JsonObject {
  [key: string]: JsonValue
}

export interface GameEvent {
  turn: number
  kind: string
  summary: string
  /** Seat ids where the seat resolved, otherwise recorded player names. */
  actors: string[]
  /** 1-100. How much of the match's story this event carries. */
  weight: number
  data: JsonObject
}

export interface GameEventsResponse {
  schema_version: number
  game_id: string
  available: boolean
  events: GameEvent[]
  event_counts: Record<string, number>
  total_events: number
  truncated: boolean
  omitted_counts: Record<string, number>
  /** The lightest weight that survived the response cap. */
  min_included_weight: number
  last_turn: number
  complete?: boolean | undefined
  event_warnings?: ReplayWarning[] | undefined
}

export interface BoardTerrain {
  code: string
  name: string
}

export interface BoardExtra {
  id: number
  name: string
}

export interface BoardCity {
  id: number
  x: number
  y: number
  player_id: number
  name: string
  size: number
  capital: boolean
}

export interface BoardUnitType {
  name: string
  count: number
}

export interface BoardUnitStack {
  x: number
  y: number
  player_id: number
  count: number
  types: BoardUnitType[]
}

export interface BoardPlayer {
  player_id: number
  player_name: string
  player_color?: string | null | undefined
  seat_id?: string | null | undefined
  place?: number | null | undefined
  controller_label?: string | null | undefined
  controller_type?: string | null | undefined
  model?: string | null | undefined
  nation?: string | null | undefined
  scored?: boolean | undefined
}

export interface BoardResponse {
  schema_version: number
  game_id: string
  turn: number
  width: number
  height: number
  topology: string
  wrap: string
  terrain_catalog: BoardTerrain[]
  terrain_rows: string[]
  altitude_rows: string[]
  owner_rows: string[]
  extras_catalog: BoardExtra[]
  extra_layers: string[][]
  cities: BoardCity[]
  unit_stacks: BoardUnitStack[]
  players: BoardPlayer[]
}

export interface RouteContext {
  gameId: string
  prefix: string
}

export interface ArenaRouteContext {
  prefix: string
}
