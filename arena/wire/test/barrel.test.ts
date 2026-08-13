import { describe, expect, test } from 'bun:test';
import * as Wire from 'src/index';
import * as Canon from 'src/canon';
import * as Codec from 'src/codec';
import * as ControlProtocol from 'src/control-protocol';
import * as Ids from 'src/ids';
import * as Json from 'src/json';
import * as Numeric from 'src/numeric';
import * as GatewayArchive from 'src/gateway/archive';
import * as GatewayGames from 'src/gateway/games';
import * as GatewayIdentity from 'src/gateway/identity';
import * as GatewayManifest from 'src/gateway/manifest';
import * as GatewayProblem from 'src/gateway/problem';
import * as GatewayReplay from 'src/gateway/replay';
import * as GatewayTechnology from 'src/gateway/technology';

const exportsOf = (module: object): readonly string[] =>
  Object.keys(module).filter((name) => name !== 'default');

type Family = readonly (readonly [string, object])[];
const CORE: Family = [
  ['canon', Canon],
  ['codec', Codec],
  ['json', Json],
  ['numeric', Numeric],
  ['control-protocol', ControlProtocol],
  ['ids', Ids],
];
const GATEWAY: Family = [
  ['identity', GatewayIdentity],
  ['games', GatewayGames],
  ['manifest', GatewayManifest],
  ['archive', GatewayArchive],
  ['replay', GatewayReplay],
  ['technology', GatewayTechnology],
  ['problem', GatewayProblem],
];

const missingFrom = (barrel: object, family: Family): readonly string[] =>
  family.flatMap(([label, module]) =>
    exportsOf(module)
      .filter((name) => !Object.hasOwn(barrel, name))
      .map((name) => `${label}:${name}`),
  );

const surfaceOf = (family: Family): ReadonlySet<string> =>
  new Set(family.flatMap(([, module]) => exportsOf(module)));

describe('public barrels', () => {
  test('the root includes every core runtime export', () => {
    expect(missingFrom(Wire, CORE)).toEqual([]);
  });

  test('the Gateway namespace includes every gateway runtime export', () => {
    expect(missingFrom(Wire.Gateway, GATEWAY)).toEqual([]);
  });

  test('the root and Gateway barrels expose no stale runtime names', () => {
    const core = surfaceOf(CORE);
    expect(
      exportsOf(Wire).filter(
        (name) => !core.has(name) && !['WIRE_PACKAGE', 'WIRE_REVISION', 'Gateway'].includes(name),
      ),
    ).toEqual([]);

    const gateway = surfaceOf(GATEWAY);
    expect(exportsOf(Wire.Gateway).filter((name) => !gateway.has(name))).toEqual([]);
  });

  test('package identity and namespace are usable through the public entry point', () => {
    expect(Wire.WIRE_PACKAGE).toBe('@arena/wire');
    expect(Wire.WIRE_REVISION).toBe(0);
    expect(Wire.Gateway.PUBLIC_CONTROL_PROTOCOLS).toBe(Wire.CONTROL_PROTOCOLS);
    expect(Wire.decodeGameId('AbC-012345678901234_')._tag).toBe('Right');
  });
});
