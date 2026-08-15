/**
 * Command receipts and the city-investigation observation they may carry.
 *
 * Ports `_validate_investigation_observation` (client.py:1713-1848) and
 * `_validate_receipt` (1851-1937).
 *
 * The receipt-state invariants are safety-critical and are enforced here, not
 * in U14: an `ambiguous` receipt must carry `action_outcome_ambiguous` and must
 * not be retryable, and a `rejected` receipt must not carry that code at all.
 */
import { Effect } from 'effect';
import { type DriftError, invalid } from 'src/errors';
import { FULL_CONTROL_V2, V2_RECEIPT_STATES } from 'src/constants';
import {
  exact,
  field,
  isJsonBoolean,
  isJsonString,
  isWholeNumber,
  opaque,
  type JsonObject,
  type JsonValue,
  type SessionIdentity,
} from 'src/schema/primitives';
import { decodeError, decodeV2Header, type StructuredError } from 'src/schema/error';
import { decodeRevision, revisionsEqual, type Revision } from 'src/schema/revision';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type ReceiptState = 'accepted' | 'applied' | 'rejected' | 'ambiguous';

export interface CityFeeling {
  readonly stage: string;
  readonly happy: number;
  readonly content: number;
  readonly unhappy: number;
  readonly angry: number;
}

export interface CityInvestigationObservation {
  readonly id: string;
  readonly type: 'city_investigation';
  readonly source: 'human_client_city_info';
  readonly freshness: 'captured_at_receipt_revision';
  readonly state_revision: Revision;
  readonly city: {
    readonly id: string;
    readonly name: string;
    readonly size: number;
    readonly production: { readonly id: string; readonly kind: string; readonly name: string };
    readonly shields: { readonly stock: number; readonly surplus: number };
    readonly improvements: ReadonlyArray<{ readonly id: string; readonly name: string }>;
    readonly citizens: {
      readonly feelings: ReadonlyArray<CityFeeling>;
      readonly specialists: ReadonlyArray<{
        readonly id: string;
        readonly name: string;
        readonly count: number;
      }>;
    };
  };
}

export interface CommandReceipt {
  readonly schema_version: 2;
  readonly control_protocol: typeof FULL_CONTROL_V2;
  readonly game_id: string;
  readonly agent_id: string;
  readonly batch_id: string;
  readonly receipt_state: ReceiptState;
  readonly idempotent: boolean;
  readonly state_revision: Revision;
  readonly error: StructuredError | null;
  readonly observation: CityInvestigationObservation | null;
}

// ---------------------------------------------------------------------------
// _validate_investigation_observation
// ---------------------------------------------------------------------------

const OBSERVATION_FIELDS: ReadonlySet<string> = new Set([
  'id',
  'type',
  'source',
  'freshness',
  'state_revision',
  'city',
]);
const CITY_FIELDS: ReadonlySet<string> = new Set([
  'id',
  'name',
  'size',
  'production',
  'shields',
  'improvements',
  'citizens',
]);
const PRODUCTION_FIELDS: ReadonlySet<string> = new Set(['id', 'kind', 'name']);
const SHIELD_FIELDS: ReadonlySet<string> = new Set(['stock', 'surplus']);
const IMPROVEMENT_FIELDS: ReadonlySet<string> = new Set(['id', 'name']);
const CITIZEN_FIELDS: ReadonlySet<string> = new Set(['feelings', 'specialists']);
const FEELING_FIELDS: ReadonlySet<string> = new Set([
  'stage',
  'happy',
  'content',
  'unhappy',
  'angry',
]);
const SPECIALIST_FIELDS: ReadonlySet<string> = new Set(['id', 'name', 'count']);

const FEELING_STAGES = [
  'base',
  'luxury',
  'effects',
  'nationality',
  'martial_law',
  'final',
] as const;

const MOOD_KEYS = ['happy', 'content', 'unhappy', 'angry'] as const;

const nonNegativeInt = (value: JsonValue): value is number =>
  isWholeNumber(value) && value >= 0;

const distinct = (values: ReadonlyArray<string>): boolean => new Set(values).size === values.length;
const isReceiptState = (value: JsonValue): value is ReceiptState =>
  isJsonString(value) && V2_RECEIPT_STATES.has(value);

export const decodeInvestigation = (
  value: JsonValue,
  revision: Revision
): Effect.Effect<CityInvestigationObservation, DriftError> =>
  Effect.gen(function* () {
    const raw = yield* exact(value, OBSERVATION_FIELDS, 'city investigation observation');
    const own = yield* decodeRevision(field(raw, 'state_revision'));
    if (
      field(raw, 'type') !== 'city_investigation' ||
      field(raw, 'source') !== 'human_client_city_info' ||
      field(raw, 'freshness') !== 'captured_at_receipt_revision' ||
      !revisionsEqual(own, revision)
    ) {
      return yield*invalid('city investigation observation provenance');
    }
    const city = yield* exact(field(raw, 'city'), CITY_FIELDS, 'city investigation observation city');
    const name = field(city, 'name');
    const size = field(city, 'size');
    if (!isJsonString(name) || name.length === 0 || !isWholeNumber(size) || size < 1) {
      return yield*invalid('city investigation observation city');
    }

    const production = yield* exact(
      field(city, 'production'),
      PRODUCTION_FIELDS,
      'city investigation production'
    );
    const productionKind = field(production, 'kind');
    const productionName = field(production, 'name');
    if (
      (productionKind !== 'unit' && productionKind !== 'improvement') ||
      !isJsonString(productionName) ||
      productionName.length === 0
    ) {
      return yield*invalid('city investigation production');
    }

    const shields = yield* exact(
      field(city, 'shields'),
      SHIELD_FIELDS,
      'city investigation shields'
    );
    const stock = field(shields, 'stock');
    const surplus = field(shields, 'surplus');
    if (!nonNegativeInt(stock) || !isWholeNumber(surplus)) {
      return yield*invalid('city investigation shields');
    }

    const improvementItems = field(city, 'improvements');
    if (!Array.isArray(improvementItems) || improvementItems.length > 1024) {
      return yield*invalid('city investigation improvements');
    }
    const improvements: Array<{ readonly id: string; readonly name: string }> = [];
    for (const item of improvementItems) {
      const improvement = yield* exact(
        item,
        IMPROVEMENT_FIELDS,
        'city investigation improvement'
      );
      const improvementName = field(improvement, 'name');
      if (!isJsonString(improvementName) || improvementName.length === 0) {
        return yield*invalid('city investigation improvement');
      }
      improvements.push({
        id: yield* opaque(field(improvement, 'id'), 'improvement ID'),
        name: improvementName,
      });
    }
    if (
      !distinct(improvements.map((item) => item.id)) ||
      !distinct(improvements.map((item) => item.name))
    ) {
      return yield*invalid('city investigation improvements');
    }

    const citizens = yield* exact(
      field(city, 'citizens'),
      CITIZEN_FIELDS,
      'city investigation citizens'
    );

    const feelingItems = field(citizens, 'feelings');
    if (!Array.isArray(feelingItems) || feelingItems.length !== FEELING_STAGES.length) {
      return yield*invalid('city investigation feelings');
    }
    const feelings: CityFeeling[] = [];
    for (const [index, item] of feelingItems.entries()) {
      const feeling = yield* exact(item, FEELING_FIELDS, 'city investigation feeling');
      const stage = FEELING_STAGES.at(index);
      const happy = field(feeling, MOOD_KEYS[0]);
      const content = field(feeling, MOOD_KEYS[1]);
      const unhappy = field(feeling, MOOD_KEYS[2]);
      const angry = field(feeling, MOOD_KEYS[3]);
      if (
        stage === undefined ||
        field(feeling, 'stage') !== stage ||
        !nonNegativeInt(happy) ||
        !nonNegativeInt(content) ||
        !nonNegativeInt(unhappy) ||
        !nonNegativeInt(angry)
      ) {
        return yield*invalid('city investigation feeling');
      }
      feelings.push({ stage, happy, content, unhappy, angry });
    }

    const specialistItems = field(citizens, 'specialists');
    if (!Array.isArray(specialistItems) || specialistItems.length > 256) {
      return yield*invalid('city investigation specialists');
    }
    const specialists: Array<{ readonly id: string; readonly name: string; readonly count: number }> =
      [];
    for (const item of specialistItems) {
      const specialist = yield* exact(item, SPECIALIST_FIELDS, 'city investigation specialist');
      const specialistName = field(specialist, 'name');
      const count = field(specialist, 'count');
      if (
        !isJsonString(specialistName) ||
        specialistName.length === 0 ||
        !nonNegativeInt(count)
      ) {
        return yield*invalid('city investigation specialist');
      }
      specialists.push({
        id: yield* opaque(field(specialist, 'id'), 'specialist ID'),
        name: specialistName,
        count,
      });
    }
    if (
      !distinct(specialists.map((item) => item.id)) ||
      !distinct(specialists.map((item) => item.name))
    ) {
      return yield*invalid('city investigation specialists');
    }

    const specialistPopulation = specialists.reduce((total, item) => total + item.count, 0);
    const inconsistent = feelings.some(
      (feeling) =>
        feeling.happy +
          feeling.content +
          feeling.unhappy +
          feeling.angry +
          specialistPopulation !==
        size
    );
    if (inconsistent) {
      return yield*invalid('city investigation population');
    }

    return {
      id: yield* opaque(field(raw, 'id'), 'observation ID'),
      type: 'city_investigation',
      source: 'human_client_city_info',
      freshness: 'captured_at_receipt_revision',
      state_revision: revision,
      city: {
        id: yield* opaque(field(city, 'id'), 'investigated city ID'),
        name,
        size,
        production: {
          id: yield* opaque(field(production, 'id'), 'production ID'),
          kind: productionKind,
          name: productionName,
        },
        shields: { stock, surplus },
        improvements,
        citizens: { feelings, specialists },
      },
    } as const;
  });

// ---------------------------------------------------------------------------
// _validate_receipt
// ---------------------------------------------------------------------------

const RECEIPT_FIELDS: ReadonlySet<string> = new Set([
  'schema_version',
  'control_protocol',
  'game_id',
  'agent_id',
  'batch_id',
  'receipt_state',
  'idempotent',
  'state_revision',
  'error',
  'observation',
]);

export interface ReceiptOptions {
  /** When set, the receipt must name this batch. */
  readonly batchId?: string;
}

export const decodeReceipt = (
  value: JsonValue,
  session: SessionIdentity,
  options: ReceiptOptions = {}
): Effect.Effect<CommandReceipt, DriftError> =>
  Effect.gen(function* () {
    const raw: JsonObject = yield* decodeV2Header(value, session, RECEIPT_FIELDS, 'v2 receipt');
    const receiptBatch = yield* opaque(field(raw, 'batch_id'), 'receipt batch ID');
    const revision = yield* decodeRevision(field(raw, 'state_revision'));
    const state = field(raw, 'receipt_state');
    const idempotent = field(raw, 'idempotent');
    if (
      (options.batchId !== undefined && receiptBatch !== options.batchId) ||
      !isReceiptState(state) ||
      !isJsonBoolean(idempotent)
    ) {
      return yield*invalid('v2 receipt');
    }
    const receiptState = state;

    const terminalError = receiptState === 'rejected' || receiptState === 'ambiguous';
    let error: StructuredError | null = null;
    if (terminalError) {
      const decoded = yield* decodeError(field(raw, 'error'));
      if (decoded.state_revision === null || !revisionsEqual(decoded.state_revision, revision)) {
        return yield*invalid('v2 receipt error revision');
      }
      if (
        receiptState === 'ambiguous' &&
        (decoded.error.code !== 'action_outcome_ambiguous' || decoded.error.retryable)
      ) {
        return yield*invalid('ambiguous receipt');
      }
      if (receiptState === 'rejected' && decoded.error.code === 'action_outcome_ambiguous') {
        return yield*invalid('rejected receipt');
      }
      error = decoded;
    } else if (field(raw, 'error') !== null) {
      return yield*invalid('v2 receipt error');
    }

    const rawObservation = field(raw, 'observation');
    let observation: CityInvestigationObservation | null = null;
    if (rawObservation !== null) {
      if (receiptState !== 'applied') {
        return yield*invalid('v2 receipt observation');
      }
      observation = yield* decodeInvestigation(rawObservation, revision);
    }

    return {
      schema_version: 2,
      control_protocol: FULL_CONTROL_V2,
      game_id: session.gameId,
      agent_id: session.agentId,
      batch_id: receiptBatch,
      receipt_state: receiptState,
      idempotent,
      state_revision: revision,
      error,
      observation,
    } as const;
  });
