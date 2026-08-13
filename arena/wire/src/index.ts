/** Canonical JSON and versioned schemas for the Arena gateway. */

export {
  CANON_ASCII,
  CANON_MAX_DEPTH,
  CANON_UTF8,
  type CanonError,
  canonicalBytes,
  canonicalText,
  type CanonOptions,
  type CanonPath,
  type CanonPathSegment,
  type CanonRecord,
  type CanonValue,
  compareCodePoints,
  encodeString,
  encodeUtf8,
  findLoneSurrogate,
  formatCanonPath,
  formatFloat,
  LoneSurrogateError,
  MaxDepthExceededError,
  NonFiniteFloatError,
  UnsupportedValueError,
} from './canon.ts';

export {
  decodeWire,
  encodeWire,
  isWire,
  type WireDecoder,
  type WireEncoder,
  WireDecodeError,
  WireEncodeError,
  type WireIssue,
} from './codec.ts';

export {
  decodeJsonArray,
  decodeJsonObject,
  decodeJsonValue,
  decodeJsonValueFromString,
  isJsonArray,
  isJsonObject,
  isJsonValue,
  JsonArray,
  jsonField,
  JsonObject,
  JsonValue,
  JsonValueFromString,
} from './json.ts';

export {
  WireFloat,
  WireInt,
  WireNonNegativeInt,
  WireNumber,
} from './numeric.ts';

export {
  CONTROL_PROTOCOLS,
  ControlProtocol,
  FULL_CONTROL_V2,
  isControlProtocol,
  STRATEGIC_V1,
} from './control-protocol.ts';

export {
  decodeFrameIndex,
  decodeFrameIndexFromPngName,
  decodeFrameIndexFromString,
  decodeGameId,
  decodeRunState,
  DERIVED_RUN_STATES,
  FRAME_INDEX_DIGITS_RE,
  FRAME_INDEX_RE,
  FrameIndex,
  FrameIndexFromPngName,
  FrameIndexFromString,
  GAME_ID_RE,
  GameId,
  isGameId,
  isKnownRunState,
  isTerminalRunState,
  LIVE_RUN_STATES,
  RunState,
  TERMINAL_RUN_STATES,
} from './ids.ts';

export * as Gateway from './gateway/index.ts';

/** Identity of this package, used by the harness to report its stack. */
export const WIRE_PACKAGE = '@arena/wire' as const;

/** The wire format revision this build speaks.  Bumped when packets change. */
export const WIRE_REVISION = 0 as const;
