/**
 * The health envelope, the phase-end event and the recovery event.
 *
 * Ports `_validate_phase_end_event` (client.py:2308-2388),
 * `_validate_recovery_event` (2000-2016) and `_validate_health` (2019-2288).
 *
 * Every "optional-if-present" field widens the expected key set *before*
 * `exact` sees it — that is the whole mechanism by which an additive server
 * field does not take the play surface down, and it is why new fields must be
 * added here rather than by loosening `exact`.
 */
import { Effect } from 'effect';
import { type DriftError, drifted, invalid } from 'src/errors';
import {
  FULL_CONTROL_V2,
  PLAYER_COLOR_RE,
  TERMINAL_STATES,
  V2_EVALUATION_FIELDS,
  V2_PHASE_STATES,
  V2_RECOVERY_FIELDS,
  V2_RECOVERY_KINDS,
  V2_RECOVERY_OUTCOMES,
  V2_SIDECAR_FIELDS,
  V2_TERMINAL_RECEIPTS,
} from 'src/constants';
import {
  decodeEvaluationContext,
  exact,
  field,
  hasField,
  isJsonBoolean,
  isJsonNumber,
  isJsonObject,
  isJsonScalar,
  isJsonString,
  isWholeNumber,
  jsonObject,
  jsonValue,
  safeNumber,
  sortedNames,
  type EvaluationContext,
  type JsonObject,
  type JsonValue,
  type JsonValueInput,
  type SessionIdentity,
} from 'src/schema/primitives';
import { decodeV2Header } from 'src/schema/error';
import { compactJson } from 'src/services/json-output';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface HealthSeat {
  readonly place: number;
  readonly seat_id: string;
  readonly player_name: string;
  readonly standing?: string;
}

export interface PhaseTiming {
  readonly mode: string;
  readonly timeout_s: number | null;
  readonly deadline_started_at: number | null;
  readonly deadline_at: number | null;
  readonly elapsed_s: number | null;
  readonly remaining_s: number | null;
}

export interface WaitingOnSeat {
  readonly place: JsonValue;
  readonly seat_id: JsonValue;
  readonly player_name: JsonValue;
  readonly controller_label: JsonValue;
  readonly standing: JsonValue;
  readonly is_self: JsonValue;
}

export interface WaitingOn {
  readonly kind: string;
  readonly summary: string;
  readonly waiting_s: number | null;
  readonly seats: ReadonlyArray<WaitingOnSeat>;
}

export interface AutoEnd {
  readonly enabled: boolean;
  readonly armed: boolean;
  readonly grace_s: number | null;
  readonly remaining_s: number | null;
}

export interface PriorEnd {
  readonly place: number;
  readonly seat_id: string;
  readonly player_name: string;
  readonly controller_label: string | null;
  readonly turn: number;
  readonly phase: number;
  readonly source: string;
  readonly receipt_state: string;
  readonly resolution: string;
  readonly elapsed_s: number;
  readonly orders_submitted: number | null;
}

export interface PhaseBlock {
  readonly state: string;
  readonly turn: number | null;
  readonly phase: number | null;
  readonly active: boolean;
  readonly timing: PhaseTiming;
  readonly waiting_on?: WaitingOn | null;
  readonly auto_end?: AutoEnd | null;
  readonly prior_end?: PriorEnd | null;
}

export interface PhaseEndEvent extends JsonObject {
  readonly sequence: number;
  readonly turn: number;
  readonly phase: number;
  readonly place: number;
  readonly seat_id: string;
  readonly player_name: string;
  readonly player_color: string;
  readonly controller_label: string;
  readonly controller_type: string;
  readonly source: string;
  readonly receipt_state: string;
  readonly resolution: string;
  readonly deadline_started_at: number;
  readonly ended_at: number;
  readonly elapsed_s: number;
}

export type RecoveryEvent = JsonObject;

interface PhaseEndEventDraft extends PhaseEndEvent {
  incarnation?: number;
  orders_submitted?: number | null;
}

interface PhaseBlockDraft extends PhaseBlock {
  waiting_on?: WaitingOn | null;
  auto_end?: AutoEnd | null;
  prior_end?: PriorEnd | null;
}

export interface HealthEnvelope {
  readonly schema_version: 2;
  readonly control_protocol: typeof FULL_CONTROL_V2;
  readonly game_id: string;
  readonly agent: { readonly agent_id: string; readonly controller_label: string };
  readonly game_state: string;
  readonly seat: HealthSeat;
  readonly sidecar: JsonObject;
  readonly observation_available: boolean;
  readonly legal_actions_available: boolean;
  readonly phase: PhaseBlock | null;
  readonly last_phase_end: PhaseEndEvent | null;
  readonly last_recovery?: RecoveryEvent | null;
  readonly objective?: string;
  readonly max_turns?: number;
  readonly turns_remaining?: number | null;
}

// ---------------------------------------------------------------------------
// _validate_phase_end_event
// ---------------------------------------------------------------------------

const PHASE_END_BASE = [
  'sequence',
  'turn',
  'phase',
  'place',
  'seat_id',
  'player_name',
  'player_color',
  'controller_label',
  'controller_type',
  'source',
  'receipt_state',
  'resolution',
  'deadline_started_at',
  'ended_at',
  'elapsed_s',
] as const;

const PHASE_END_SOURCES: ReadonlySet<string> = new Set(['agent', 'timeout', 'auto_idle']);
const PHASE_END_RESOLUTIONS: ReadonlySet<string> = new Set(['advanced', 'terminal', 'failed']);

export const decodePhaseEndEvent = (
  value: JsonValue,
  session: SessionIdentity,
  seat: HealthSeat
): Effect.Effect<PhaseEndEvent, DriftError> =>
  Effect.gen(function* () {
    const fields = new Set<string>(PHASE_END_BASE);
    // Optional-if-present, like seat.standing: the recovery-era supervisor
    // stamps which receipt incarnation ended the phase.
    if (isJsonObject(value) && hasField(value, 'incarnation')) fields.add('incarnation');
    // How many batches this seat submitted in the phase that ended.  `null`
    // means the supervisor was not watching, which is not the same as zero.
    if (isJsonObject(value) && hasField(value, 'orders_submitted')) {
      fields.add('orders_submitted');
    }
    const raw = yield* exact(value, fields, 'health last phase end');

    const orders = field(raw, 'orders_submitted');
    if (hasField(raw, 'orders_submitted') && orders !== null) {
      if (!isWholeNumber(orders) || orders < 0) {
        return yield*invalid('v2 health last phase end orders_submitted');
      }
    }
    if (hasField(raw, 'incarnation')) {
      const incarnation = field(raw, 'incarnation');
      if (!isWholeNumber(incarnation) || incarnation < 0) {
        return yield*invalid('v2 health last phase end incarnation');
      }
    }

    const counters: ReadonlyArray<readonly [string, number]> = [
      ['sequence', 1],
      ['turn', 0],
      ['phase', 0],
      ['place', 1],
    ];
    const badCounter = counters.some(([name, minimum]) => {
      const own = field(raw, name);
      return !isWholeNumber(own) || own < minimum;
    });
    const sequence = field(raw, 'sequence');
    const turn = field(raw, 'turn');
    const phase = field(raw, 'phase');
    const place = field(raw, 'place');
    const seatId = field(raw, 'seat_id');
    const playerName = field(raw, 'player_name');
    const color = field(raw, 'player_color');
    const controllerLabel = field(raw, 'controller_label');
    const controllerType = field(raw, 'controller_type');
    const receiptState = field(raw, 'receipt_state');
    const resolution = field(raw, 'resolution');
    const source = field(raw, 'source');
    if (
      badCounter ||
      !isWholeNumber(sequence) ||
      !isWholeNumber(turn) ||
      !isWholeNumber(phase) ||
      !isWholeNumber(place) ||
      place !== seat.place ||
      !isJsonString(seatId) ||
      seatId !== seat.seat_id ||
      !isJsonString(playerName) ||
      playerName !== seat.player_name ||
      !isJsonString(color) ||
      !PLAYER_COLOR_RE.test(color) ||
      !isJsonString(controllerLabel) ||
      controllerLabel !== session.controllerLabel ||
      controllerType !== 'external' ||
      !isJsonString(source) ||
      !PHASE_END_SOURCES.has(source) ||
      !isJsonString(receiptState) ||
      !V2_TERMINAL_RECEIPTS.has(receiptState) ||
      !isJsonString(resolution) ||
      !PHASE_END_RESOLUTIONS.has(resolution) ||
      (receiptState === 'rejected' && resolution !== 'failed')
    ) {
      return yield*invalid('v2 health last phase end');
    }

    const deadlineStartedAt = yield* safeNumber(
      field(raw, 'deadline_started_at'),
      'health last phase end deadline_started_at'
    );
    const endedAt = yield* safeNumber(field(raw, 'ended_at'), 'health last phase end ended_at');
    const elapsed = yield* safeNumber(field(raw, 'elapsed_s'), 'health last phase end elapsed_s');
    if (endedAt < deadlineStartedAt) {
      return yield*invalid('v2 health last phase end timing');
    }
    const decoded: PhaseEndEventDraft = {
      sequence,
      turn,
      phase,
      place,
      seat_id: seatId,
      player_name: playerName,
      player_color: color,
      controller_label: controllerLabel,
      controller_type: controllerType,
      source,
      receipt_state: receiptState,
      resolution,
      deadline_started_at: deadlineStartedAt,
      ended_at: endedAt,
      elapsed_s: elapsed,
    };
    if (hasField(raw, 'incarnation')) {
      const incarnation = field(raw, 'incarnation');
      if (isWholeNumber(incarnation)) decoded.incarnation = incarnation;
    }
    if (hasField(raw, 'orders_submitted')) {
      const submitted = field(raw, 'orders_submitted');
      if (submitted === null || isWholeNumber(submitted)) decoded.orders_submitted = submitted;
    }
    return decoded;
  });

// ---------------------------------------------------------------------------
// _validate_recovery_event
// ---------------------------------------------------------------------------

export const decodeRecoveryEvent = (
  value: JsonValue,
  session: SessionIdentity,
  seat: HealthSeat
): Effect.Effect<RecoveryEvent, DriftError> =>
  Effect.gen(function* () {
    const raw = yield* exact(value, V2_RECOVERY_FIELDS, 'v2 health last recovery');
    if (
      field(raw, 'game_id') !== session.gameId ||
      field(raw, 'seat_id') !== seat.seat_id ||
      field(raw, 'place') !== seat.place
    ) {
      return yield*invalid('v2 health last recovery seat');
    }
    const kind = field(raw, 'kind');
    const outcome = field(raw, 'outcome');
    const trigger = field(raw, 'trigger');
    const rewound = field(raw, 'rewound_applied_actions');
    const timestamp = field(raw, 'timestamp');
    if (
      !isJsonString(kind) ||
      !V2_RECOVERY_KINDS.has(kind) ||
      !isJsonString(outcome) ||
      !V2_RECOVERY_OUTCOMES.has(outcome) ||
      !isJsonString(trigger) ||
      trigger.length === 0 ||
      !isJsonBoolean(rewound) ||
      !isJsonString(timestamp) ||
      timestamp.length === 0
    ) {
      return yield*invalid('v2 health last recovery');
    }
    return yield* jsonObject(raw, 'v2 health last recovery');
  });

// ---------------------------------------------------------------------------
// _validate_health
// ---------------------------------------------------------------------------

const HEALTH_BASE = [
  'schema_version',
  'control_protocol',
  'game_id',
  'agent',
  'game_state',
  'seat',
  'sidecar',
  'observation_available',
  'legal_actions_available',
  'phase',
  'last_phase_end',
] as const;

const AGENT_FIELDS: ReadonlySet<string> = new Set(['agent_id', 'controller_label']);
const SEAT_BASE = ['place', 'seat_id', 'player_name'] as const;
const SEAT_STANDINGS: ReadonlySet<string> = new Set([
  'active',
  'surrendered',
  'eliminated',
  'termination_pending',
]);
const GAME_STATES: ReadonlySet<string> = new Set([
  'lobby',
  'starting',
  'running',
  ...TERMINAL_STATES,
]);
const PHASE_BASE = ['state', 'turn', 'phase', 'active', 'timing'] as const;
const AUTO_END_FIELDS: ReadonlySet<string> = new Set([
  'enabled',
  'armed',
  'grace_s',
  'remaining_s',
]);
const PRIOR_END_FIELDS: ReadonlySet<string> = new Set([
  'place',
  'seat_id',
  'player_name',
  'controller_label',
  'turn',
  'phase',
  'source',
  'receipt_state',
  'resolution',
  'elapsed_s',
  'orders_submitted',
]);
const WAITING_ON_FIELDS: ReadonlySet<string> = new Set([
  'kind',
  'summary',
  'seats',
  'waiting_s',
]);
const WAITING_ON_SEAT_FIELDS: ReadonlySet<string> = new Set([
  'place',
  'seat_id',
  'player_name',
  'controller_label',
  'standing',
  'is_self',
]);
const WAITING_ON_KINDS: ReadonlySet<string> = new Set([
  'seat_not_ready',
  'seat_surrendered',
  'seat_inactive',
  'other_seat',
  'native_phase',
  'phase_synchronization',
  'phase_end',
  'termination',
  'boundary_recovery',
]);
const TIMING_FIELDS: ReadonlySet<string> = new Set([
  'mode',
  'timeout_s',
  'deadline_started_at',
  'deadline_at',
  'elapsed_s',
  'remaining_s',
]);
const TIMING_MODES: ReadonlySet<string> = new Set(['default', 'blitz', 'infinite', 'custom']);

/**
 * The sentence both stale-client refusals end with, verbatim from
 * client.py:2189-2193 and 2265-2270.  The dash is an em dash; every apostrophe
 * is ASCII.
 */
const STALE_CLIENT_TAIL =
  " — this workspace's client predates the server; re-materialize it with the " +
  "repository root's play launcher or refresh client.py";

/** CPython's `repr()` for the scalars a drifted `waiting_on.kind` can hold. */
const pyRepr = (value: JsonValue): string => {
  if (value === null) return 'None';
  if (isJsonBoolean(value)) return value ? 'True' : 'False';
  if (isJsonNumber(value)) return String(value);
  if (!isJsonString(value)) return compactJson(value);
  const quote = value.includes("'") && !value.includes('"') ? '"' : "'";
  const escaped = value
    .replace(/\\/g, '\\\\')
    .replace(/\n/g, '\\n')
    .replace(/\r/g, '\\r')
    .replace(/\t/g, '\\t');
  return quote + (quote === "'" ? escaped.replace(/'/g, "\\'") : escaped) + quote;
};

const decodeAutoEnd = (value: JsonValue): Effect.Effect<AutoEnd, DriftError> =>
  Effect.gen(function* () {
    const raw = yield* exact(value, AUTO_END_FIELDS, 'health phase auto_end');
    const enabled = field(raw, 'enabled');
    const armed = field(raw, 'armed');
    if (!isJsonBoolean(enabled) || !isJsonBoolean(armed)) {
      return yield*invalid('v2 health phase auto_end');
    }
    const grace = yield* safeNumber(field(raw, 'grace_s'), 'health phase auto_end grace_s', {
      nullable: true,
    });
    const remaining = yield* safeNumber(
      field(raw, 'remaining_s'),
      'health phase auto_end remaining_s',
      { nullable: true }
    );
    return { enabled, armed, grace_s: grace, remaining_s: remaining };
  });

const decodePriorEnd = (
  value: JsonValue,
  seat: HealthSeat
): Effect.Effect<PriorEnd, DriftError> =>
  Effect.gen(function* () {
    const raw = yield* exact(value, PRIOR_END_FIELDS, 'health phase prior_end');
    const source = field(raw, 'source');
    const receiptState = field(raw, 'receipt_state');
    const resolution = field(raw, 'resolution');
    const counters: ReadonlyArray<readonly [string, number]> = [
      ['place', 1],
      ['turn', 0],
      ['phase', 0],
    ];
    const badCounter = counters.some(([name, low]) => {
      const own = field(raw, name);
      return !isWholeNumber(own) || own < low;
    });
    const playerName = field(raw, 'player_name');
    const seatId = field(raw, 'seat_id');
    const controllerLabel = field(raw, 'controller_label');
    const orders = field(raw, 'orders_submitted');
    if (
      !isJsonString(source) ||
      !PHASE_END_SOURCES.has(source) ||
      !isJsonString(receiptState) ||
      !V2_TERMINAL_RECEIPTS.has(receiptState) ||
      !isJsonString(resolution) ||
      !PHASE_END_RESOLUTIONS.has(resolution) ||
      badCounter ||
      field(raw, 'place') === seat.place ||
      !isJsonString(playerName) ||
      !isJsonString(seatId) ||
      (controllerLabel !== null && !isJsonString(controllerLabel)) ||
      (orders !== null && (!isWholeNumber(orders) || orders < 0))
    ) {
      return yield*invalid('v2 health phase prior_end');
    }
    const place = field(raw, 'place');
    const turn = field(raw, 'turn');
    const phase = field(raw, 'phase');
    if (!isWholeNumber(place) || !isWholeNumber(turn) || !isWholeNumber(phase)) {
      return yield* invalid('v2 health phase prior_end');
    }
    const elapsed = yield* safeNumber(field(raw, 'elapsed_s'), 'health prior_end elapsed_s');
    return {
      place,
      seat_id: seatId,
      player_name: playerName,
      controller_label: controllerLabel,
      turn,
      phase,
      source,
      receipt_state: receiptState,
      resolution,
      elapsed_s: elapsed,
      orders_submitted: orders,
    };
  });

const decodeWaitingOn = (value: JsonValue): Effect.Effect<WaitingOn, DriftError> =>
  Effect.gen(function* () {
    const raw = yield* exact(value, WAITING_ON_FIELDS, 'health phase waiting_on');
    const kind = field(raw, 'kind');
    if (!isJsonString(kind) || !WAITING_ON_KINDS.has(kind)) {
      return yield*
        drifted(
          'v2 health',
          `invalid v2 health: unknown waiting_on kind ${pyRepr(kind)}${STALE_CLIENT_TAIL}`
        );
    }
    const summary = field(raw, 'summary');
    if (!isJsonString(summary) || summary.length === 0) {
      return yield*invalid('v2 health phase waiting_on');
    }
    const waitingS = yield* safeNumber(
      field(raw, 'waiting_s'),
      'health waiting_on waiting_s',
      { nullable: true }
    );
    const seats = field(raw, 'seats');
    if (!Array.isArray(seats)) {
      return yield*invalid('v2 health waiting_on seats');
    }
    const rows: WaitingOnSeat[] = [];
    for (const row of seats) {
      const decoded = yield* exact(row, WAITING_ON_SEAT_FIELDS, 'health waiting_on seat');
      rows.push({
        place: field(decoded, 'place'),
        seat_id: field(decoded, 'seat_id'),
        player_name: field(decoded, 'player_name'),
        controller_label: field(decoded, 'controller_label'),
        standing: field(decoded, 'standing'),
        is_self: field(decoded, 'is_self'),
      });
    }
    return { kind, summary, waiting_s: waitingS, seats: rows };
  });

const decodePhaseBlock = (
  phaseValue: JsonValue,
  seat: HealthSeat
): Effect.Effect<PhaseBlock, DriftError> =>
  Effect.gen(function* () {
    const fields = new Set<string>(PHASE_BASE);
    const carries = (name: string): boolean =>
      isJsonObject(phaseValue) && hasField(phaseValue, name);
    if (carries('waiting_on')) fields.add('waiting_on');
    // A supervisor that can end a provably idle phase for this seat says so
    // here, in time for the seat to act instead if it would rather.
    if (carries('auto_end')) fields.add('auto_end');
    // How the phase before this one ended, and whether the seat that held it
    // played it at all.
    if (carries('prior_end')) fields.add('prior_end');
    const phase = yield* exact(phaseValue, fields, 'health phase');

    const autoEndRaw = field(phase, 'auto_end');
    const autoEnd = hasField(phase, 'auto_end') && autoEndRaw !== null
      ? yield* decodeAutoEnd(autoEndRaw)
      : null;
    const priorEndRaw = field(phase, 'prior_end');
    const priorEnd = hasField(phase, 'prior_end') && priorEndRaw !== null
      ? yield* decodePriorEnd(priorEndRaw, seat)
      : null;
    const waitingOnRaw = field(phase, 'waiting_on');
    const waitingOn = hasField(phase, 'waiting_on') && waitingOnRaw !== null
      ? yield* decodeWaitingOn(waitingOnRaw)
      : null;

    const state = field(phase, 'state');
    const turn = field(phase, 'turn');
    const phaseNumber = field(phase, 'phase');
    const active = field(phase, 'active');
    if (
      !isJsonString(state) ||
      !V2_PHASE_STATES.has(state) ||
      (turn !== null && (!isWholeNumber(turn) || turn < 0)) ||
      (phaseNumber !== null && (!isWholeNumber(phaseNumber) || phaseNumber < 0)) ||
      !isJsonBoolean(active)
    ) {
      return yield*invalid('v2 health phase');
    }

    const timing = yield* exact(field(phase, 'timing'), TIMING_FIELDS, 'health phase timing');
    const mode = field(timing, 'mode');
    if (!isJsonString(mode) || !TIMING_MODES.has(mode)) {
      return yield*invalid('v2 health timing mode');
    }
    const timeouts: Record<string, number | null> = {};
    for (const name of [
      'timeout_s',
      'deadline_started_at',
      'deadline_at',
      'elapsed_s',
      'remaining_s',
    ] as const) {
      timeouts[name] = yield* safeNumber(field(timing, name), `health timing ${name}`, {
        nullable: true,
      });
    }

    const block: PhaseBlockDraft = {
      state,
      turn: turn,
      phase: phaseNumber,
      active,
      timing: {
        mode,
        timeout_s: timeouts['timeout_s'] ?? null,
        deadline_started_at: timeouts['deadline_started_at'] ?? null,
        deadline_at: timeouts['deadline_at'] ?? null,
        elapsed_s: timeouts['elapsed_s'] ?? null,
        remaining_s: timeouts['remaining_s'] ?? null,
      },
    };
    if (hasField(phase, 'waiting_on')) block.waiting_on = waitingOn;
    if (hasField(phase, 'auto_end')) block.auto_end = autoEnd;
    if (hasField(phase, 'prior_end')) block.prior_end = priorEnd;
    return block;
  });

interface HealthEnvelopeDraft extends HealthEnvelope {
  last_recovery?: RecoveryEvent | null;
  objective?: string;
  max_turns?: number;
  turns_remaining?: number | null;
}

export const decodeHealth = (
  value: JsonValueInput,
  session: SessionIdentity
): Effect.Effect<HealthEnvelope, DriftError> =>
  Effect.gen(function* () {
    const payload = yield* jsonValue(value, 'v2 health');
    const baseFields = new Set<string>(HEALTH_BASE);
    // A supervisor that reports rollbacks and one that does not are both valid
    // peers, and an additive server field must never take the play surface down.
    if (isJsonObject(payload) && hasField(payload, 'last_recovery')) {
      baseFields.add('last_recovery');
    }
    const present = isJsonObject(payload)
      ? Object.keys(payload).filter((key) => V2_EVALUATION_FIELDS.has(key))
      : [];
    const sessionContext: EvaluationContext | null = session.evaluation;
    if (present.length > 0 && present.length !== V2_EVALUATION_FIELDS.size) {
      return yield*invalid('v2 health', 'evaluation context is incomplete');
    }
    if (sessionContext !== null && present.length === 0) {
      return yield*invalid('v2 health', 'evaluation context is missing');
    }
    const fields = new Set<string>(baseFields);
    if (present.length > 0) {
      for (const name of V2_EVALUATION_FIELDS) fields.add(name);
    }
    const raw = yield* decodeV2Header(payload, session, fields, 'v2 health');
    const evaluation = yield* decodeEvaluationContext(raw, 'v2 health', {
      expected: sessionContext,
      required: sessionContext !== null,
    });

    const agent = yield* exact(field(raw, 'agent'), AGENT_FIELDS, 'health agent');
    const controllerLabel = field(agent, 'controller_label');
    if (
      field(agent, 'agent_id') !== session.agentId ||
      !isJsonString(controllerLabel) ||
      controllerLabel.length === 0 ||
      controllerLabel !== session.controllerLabel
    ) {
      return yield*invalid('v2 health agent identity');
    }

    const seatFields = new Set<string>(SEAT_BASE);
    const rawSeat = field(raw, 'seat');
    if (isJsonObject(rawSeat) && hasField(rawSeat, 'standing')) seatFields.add('standing');
    const seatRaw = yield* exact(rawSeat, seatFields, 'health seat');
    const standing = field(seatRaw, 'standing');
    const cleanStanding = hasField(seatRaw, 'standing') && isJsonString(standing)
      ? standing
      : undefined;
    if (
      hasField(seatRaw, 'standing') &&
      (cleanStanding === undefined || !SEAT_STANDINGS.has(cleanStanding))
    ) {
      return yield* invalid('v2 health seat standing');
    }
    const expectations: ReadonlyArray<readonly [string, string | number | null]> = [
      ['place', session.place],
      ['seat_id', session.seatId],
      ['player_name', session.playerName],
    ];
    for (const [name, expected] of expectations) {
      if (expected !== null && field(seatRaw, name) !== expected) {
        return yield*invalid(`v2 health seat ${name}`);
      }
    }
    const place = field(seatRaw, 'place');
    const seatId = field(seatRaw, 'seat_id');
    const playerName = field(seatRaw, 'player_name');
    const gameState = field(raw, 'game_state');
    const sidecar = field(raw, 'sidecar');
    const observationAvailable = field(raw, 'observation_available');
    const legalActionsAvailable = field(raw, 'legal_actions_available');
    if (
      !isWholeNumber(place) ||
      place < 1 ||
      !isJsonString(seatId) ||
      seatId.length === 0 ||
      !isJsonString(playerName) ||
      playerName.length === 0 ||
      !isJsonString(gameState) ||
      !GAME_STATES.has(gameState) ||
      !isJsonObject(sidecar) ||
      !hasField(sidecar, 'state') ||
      !hasField(sidecar, 'generation') ||
      Object.values(sidecar).some((item) => !isJsonScalar(item)) ||
      !isJsonBoolean(observationAvailable) ||
      !isJsonBoolean(legalActionsAvailable)
    ) {
      return yield*invalid('v2 health response');
    }
    const unexpectedSidecar = sortedNames(
      Object.keys(sidecar).filter((key) => !V2_SIDECAR_FIELDS.has(key))
    );
    if (unexpectedSidecar.length > 0) {
      return yield*
        drifted(
          'v2 health',
          `invalid v2 health: unexpected sidecar field(s) ${unexpectedSidecar.join(', ')}` +
            STALE_CLIENT_TAIL
        );
    }

    const seat: HealthSeat = cleanStanding === undefined
      ? { place, seat_id: seatId, player_name: playerName }
      : { place, seat_id: seatId, player_name: playerName, standing: cleanStanding };

    const rawPhase = field(raw, 'phase');
    let cleanPhase: PhaseBlock | null = null;
    if (rawPhase === null) {
      // A clean native server exit can be observed just before the supervisor
      // classifies its terminal result.  No stale actionable phase may survive.
      cleanPhase = null;
    } else if (TERMINAL_STATES.has(gameState)) {
      return yield*drifted('v2 health', 'terminal v2 health retained stale phase state');
    } else {
      cleanPhase = yield* decodePhaseBlock(rawPhase, seat);
    }

    if (
      evaluation !== null &&
      cleanPhase !== null &&
      cleanPhase.turn !== null &&
      evaluation.turns_remaining !== Math.max(0, evaluation.max_turns - cleanPhase.turn)
    ) {
      return yield*invalid('v2 health', 'turns_remaining is inconsistent');
    }

    const rawLastPhaseEnd = field(raw, 'last_phase_end');
    const lastPhaseEnd =
      rawLastPhaseEnd === null ? null : yield* decodePhaseEndEvent(rawLastPhaseEnd, session, seat);

    const result: HealthEnvelopeDraft = {
      schema_version: 2,
      control_protocol: FULL_CONTROL_V2,
      game_id: session.gameId,
      agent: { agent_id: session.agentId, controller_label: controllerLabel },
      game_state: gameState,
      seat,
      sidecar: yield* jsonObject(sidecar, 'sidecar health'),
      observation_available: observationAvailable,
      legal_actions_available: legalActionsAvailable,
      phase: cleanPhase,
      last_phase_end: lastPhaseEnd,
    };
    if (hasField(raw, 'last_recovery')) {
      const rawRecovery = field(raw, 'last_recovery');
      result.last_recovery =
        rawRecovery === null ? null : yield* decodeRecoveryEvent(rawRecovery, session, seat);
    }
    if (evaluation !== null) {
      result.objective = evaluation.objective;
      result.max_turns = evaluation.max_turns;
      result.turns_remaining = evaluation.turns_remaining;
    }
    return result;
  });
