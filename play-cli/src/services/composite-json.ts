/**
 * The one-call composite payload.
 *
 * Ports `_composite_json` (client.py:6939-6945).
 *
 * `turn --end --await --brief` and `do … --end --await --brief` each perform
 * three logical steps and print one JSON object.  The object is the parts, each
 * exactly as it was — no field is re-derived, renamed or flattened, so a
 * consumer that already parses `just wait --json` or `just turn --json` parses
 * the composite's members unchanged.
 */
import type { TurnResult } from 'src/render/turn';
import type { BatchDisposition } from 'src/schema/batch';
import type { WaitEnvelope } from 'src/schema/wait';

/** Anything a composite part can be — each member is already a validated envelope. */
export type CompositePart =
  | BatchDisposition
  | WaitEnvelope
  | TurnResult
  | string
  | null;

export interface CompositeJson {
  readonly schema_version: 1;
  readonly command: string;
  readonly status: 'briefed';
  readonly [part: string]: CompositePart | 1 | string;
}

/** Shape the one-call composite: the parts, each exactly as it was. */
export const compositeJson = (
  command: string,
  parts: Readonly<Record<string, CompositePart>>
): CompositeJson => ({
  schema_version: 1,
  command,
  status: 'briefed',
  ...parts,
});
