/**
 * The invitation loader (`_invite`, client.py:6010-6126).
 *
 * This is the port's only credential-reading surface, so the tests are written
 * as a rejection matrix: one row per way a hostile or rotten invitation can
 * reach the loader, each asserting the *exact* stderr sentence, because those
 * sentences are the agent's only route back to a working game — it cannot run
 * `just invite` itself and has to quote the remedy to the game owner verbatim.
 *
 * Ports `test_invite_is_game_scoped_and_token_is_not_returned_publicly`,
 * `test_missing_or_broken_invite_names_owner_recovery_command`,
 * `test_invite_root_symlink_cannot_escape_player_workspace`,
 * `test_explicit_token_ignores_bad_implicit_invite_and_uses_env_url` and the
 * invite half of `test_session_and_invite_paths_cannot_escape_workspace`.
 */
import type { FileSystem } from '@effect/platform';
import { afterEach, describe, expect } from 'bun:test';
import { Effect, Either } from 'effect';
import {
  INVITE_ESCAPES,
  INVITE_ROOT_NOT_REAL,
  loadInvitation,
  type InviteRequest,
  type Invitation,
} from 'src/services/invites';
import type { JsonObject, JsonValue } from 'src/schema/primitives';
import type { WorkspacePaths } from 'src/services/private-fs';
import { effectTest } from 'test/_effect-test';
import { path, withTestFileSystem } from 'test/_test-platform';

const GAME_ID = 'game_Hsit9YEuBjKdJPPouFoGVYlk';
const OTHER_ID = 'game_9OtherGameIdentifier000';

const roots: string[] = [];

const withInviteFiles = <A, E>(
  body: (files: FileSystem.FileSystem) => Effect.Effect<A, E>
): Effect.Effect<A> => withTestFileSystem(body).pipe(Effect.orDie);

/** Run one invitation test against a single Bun FileSystem scope. */
const inviteTest = (
  name: string,
  body: (files: FileSystem.FileSystem) => Effect.Effect<void>
): void => effectTest(name, () => withInviteFiles(body));

const makeDirectory = (
  files: FileSystem.FileSystem,
  target: string,
  recursive = false
): Effect.Effect<void> => files.makeDirectory(target, { recursive }).pipe(Effect.orDie);

const symlink = (
  files: FileSystem.FileSystem,
  from: string,
  to: string
): Effect.Effect<void> => files.symlink(from, to).pipe(Effect.orDie);

const writeFixtureFile = (
  files: FileSystem.FileSystem,
  target: string,
  text: string,
  mode = 0o600
): Effect.Effect<void> =>
  Effect.gen(function* () {
    yield* files.writeFileString(target, text);
    yield* files.chmod(target, mode);
  }).pipe(Effect.orDie);

const writeBytes = (
  files: FileSystem.FileSystem,
  fixture: Bench,
  name: string,
  bytes: Uint8Array
): Effect.Effect<string> =>
  Effect.gen(function* () {
    const target = path.join(fixture.invites, name);
    yield* files.writeFile(target, bytes);
    yield* files.chmod(target, 0o600);
    return target;
  }).pipe(Effect.orDie);

afterEach(() =>
  Effect.runPromise(
    withInviteFiles((files) =>
      Effect.gen(function* () {
        while (roots.length > 0) {
          const root = roots.pop();
          if (root !== undefined) {
            yield* files.remove(root, { recursive: true, force: true }).pipe(Effect.orDie);
          }
        }
      })
    )
  )
);

interface Bench {
  readonly files: FileSystem.FileSystem;
  readonly root: string;
  readonly workspace: WorkspacePaths;
  readonly invites: string;
  readonly write: (name: string, body: JsonValue, mode?: number) => Effect.Effect<string>;
  readonly writeRaw: (name: string, body: string, mode?: number) => Effect.Effect<string>;
}

/** A workspace with a real `.invites/` directory, unless `invites` is false. */
const bench = (
  files: FileSystem.FileSystem,
  options: { readonly invites?: boolean } = {}
): Effect.Effect<Bench> =>
  Effect.gen(function* () {
    const root = yield* Effect.flatMap(
      files.makeTempDirectory({ prefix: 'play-cli-u02-' }),
      files.realPath
    );
    roots.push(root);
    const invites = path.join(root, '.invites');
    if (options.invites !== false) {
      yield* files.makeDirectory(invites, { mode: 0o700 });
    }
    const writeText = (name: string, text: string, mode = 0o600): Effect.Effect<string> =>
      Effect.gen(function* () {
        const target = path.join(invites, name);
        yield* files.writeFileString(target, text);
        yield* files.chmod(target, mode);
        return target;
      }).pipe(Effect.orDie);
    return {
      files,
      root,
      workspace: { root, stateRoot: path.join(root, '.sessions') },
      invites,
      write: (name: string, body: JsonValue, mode?: number) =>
        writeText(name, JSON.stringify(body), mode),
      writeRaw: writeText,
    };
  }).pipe(Effect.orDie);

const validInvite = (overrides: JsonObject = {}): JsonObject => ({
  schema_version: 1,
  game_id: GAME_ID,
  service_url: 'http://127.0.0.1:8765',
  join_token: 'join-secret',
  ...overrides,
});

const inviteJson = (overrides: JsonObject = {}): string => JSON.stringify(validInvite(overrides));

const request = (overrides: Partial<InviteRequest> = {}): InviteRequest => ({
  gameId: GAME_ID,
  invite: '',
  joinToken: '',
  ...overrides,
});

const load = (
  workspace: WorkspacePaths,
  overrides: Partial<InviteRequest> = {},
  environment: Record<string, string | undefined> = {}
): Effect.Effect<Either.Either<Invitation, { readonly message: string }>> =>
  Effect.either(loadInvitation(workspace, request(overrides), environment));

const refusal = <A>(either: Either.Either<A, { readonly message: string }>): string => {
  expect(Either.isLeft(either)).toBe(true);
  return Either.isLeft(either) ? either.left.message : '';
};

const accepted = <A, E>(either: Either.Either<A, E>): A => {
  expect(Either.isRight(either)).toBe(true);
  if (Either.isLeft(either)) throw new Error('expected an accepted invitation');
  return either.right;
};

/** The one remediation sentence every refusal ends with. */
const REMEDY =
  `Ask the game owner to run \`just invite ${GAME_ID}\` from the ` +
  'repository root, then retry once.';

// ---------------------------------------------------------------------------

describe('a well-formed invitation', () => {
  inviteTest('the default file is read by game ID and carries token and origin', (files) =>
    Effect.gen(function* () {
      const fixture = yield* bench(files);
      yield* fixture.write(`${GAME_ID}.json`, validInvite());
      expect(accepted(yield* load(fixture.workspace))).toEqual({
        token: 'join-secret',
        base: 'http://127.0.0.1:8765',
      });
    })
  );

  inviteTest('an explicitly configured file wins over the default one', (files) =>
    Effect.gen(function* () {
      const fixture = yield* bench(files);
      yield* fixture.write(`${GAME_ID}.json`, validInvite({ join_token: 'default-secret' }));
      const configured = yield* fixture.write(
        'explicit.json',
        validInvite({ join_token: 'explicit-secret', service_url: 'https://elsewhere.test' })
      );
      expect(accepted(yield* load(fixture.workspace, { invite: configured }))).toEqual({
        token: 'explicit-secret',
        base: 'https://elsewhere.test',
      });
    })
  );

  inviteTest('PLAY_INVITE configures the same path as --invite', (files) =>
    Effect.gen(function* () {
      const fixture = yield* bench(files);
      const configured = yield* fixture.write('env.json', validInvite({ join_token: 'env-file-secret' }));
      expect(
        accepted(yield* load(fixture.workspace, {}, { PLAY_INVITE: `  ${configured}  ` })).token
      ).toBe('env-file-secret');
    })
  );

  inviteTest('a relative --invite is read against the workspace root', (files) =>
    Effect.gen(function* () {
      const fixture = yield* bench(files);
      yield* fixture.write('relative.json', validInvite({ join_token: 'relative-secret' }));
      expect(
        accepted(yield* load(fixture.workspace, { invite: '.invites/relative.json' })).token
      ).toBe('relative-secret');
    })
  );
});

// ---------------------------------------------------------------------------

describe('the credential override', () => {
  inviteTest('an environment token skips the default file and takes the env origin', (files) =>
    Effect.gen(function* () {
      // The default invitation is unparseable JSON: a stale local file must not
      // block documented recovery, nor redirect the join to its old origin.
      const fixture = yield* bench(files);
      yield* fixture.writeRaw(`${GAME_ID}.json`, '{');
      expect(
        accepted(
          yield* load(
            fixture.workspace,
            {},
            {
              AGENT_EVAL_JOIN_TOKEN: 'explicit-secret',
              AGENT_EVAL_SERVICE_URL: 'http://127.0.0.1:9999',
            }
          )
        )
      ).toEqual({ token: 'explicit-secret', base: 'http://127.0.0.1:9999' });
    })
  );

  inviteTest('--join-token skips a `.invites` that is not even a directory', (files) =>
    Effect.gen(function* () {
      const fixture = yield* bench(files, { invites: false });
      yield* writeFixtureFile(files, path.join(fixture.root, '.invites'), 'not a directory');
      expect(accepted(yield* load(fixture.workspace, { joinToken: 'flag-secret' })).token).toBe(
        'flag-secret'
      );
    })
  );

  inviteTest('it does NOT skip an explicitly configured invitation', (files) =>
    Effect.gen(function* () {
      // The override is about the *implicit default*: an operator who named a
      // file still gets that file's origin, and its refusals.
      const fixture = yield* bench(files);
      const configured = yield* fixture.write('configured.json', validInvite({ join_token: ' padded ' }));
      const loaded = accepted(
        yield* load(
          fixture.workspace,
          { invite: configured },
          { AGENT_EVAL_JOIN_TOKEN: 'explicit-secret' }
        )
      );
      // A rotten stored token is tolerated only because it is never used …
      expect(loaded.token).toBe('explicit-secret');
      // … while the configured file's service URL still decides the origin.
      expect(loaded.base).toBe('http://127.0.0.1:8765');
    })
  );

  inviteTest('an explicitly configured file that does not exist is still a refusal', (files) =>
    Effect.gen(function* () {
      const fixture = yield* bench(files);
      expect(
        refusal(
          yield* load(
            fixture.workspace,
            { invite: path.join(fixture.invites, 'absent.json') },
            { AGENT_EVAL_JOIN_TOKEN: 'explicit-secret' }
          )
        )
      ).toBe(`the configured invitation for ${GAME_ID} does not exist. ${REMEDY}`);
    })
  );
});

// ---------------------------------------------------------------------------

describe('the rejection matrix', () => {
  interface Row {
    readonly name: string;
    /** Prepare the workspace and return the `--invite` value to pass. */
    readonly stage: (fixture: Bench) => Effect.Effect<Partial<InviteRequest>>;
    readonly message: string;
    readonly invites?: boolean;
  }

  const rows: ReadonlyArray<Row> = [
    {
      name: 'a symlinked .invites cannot escape the player workspace',
      invites: false,
      stage: (fixture) =>
        Effect.gen(function* () {
          const outside = path.join(fixture.root, '..', path.basename(fixture.root) + '-outside');
          yield* makeDirectory(fixture.files, outside, true);
          roots.push(outside);
          yield* symlink(fixture.files, outside, path.join(fixture.root, '.invites'));
          return {};
        }),
      message: INVITE_ROOT_NOT_REAL,
    },
    {
      name: 'a missing .invites names the owner recovery command',
      invites: false,
      stage: () => Effect.succeed({}),
      message: `the invitation directory is unavailable. ${REMEDY}`,
    },
    {
      name: 'an absolute path outside .invites is refused by path',
      stage: (fixture) =>
        Effect.gen(function* () {
          const outside = path.join(fixture.root, 'outside-invite.json');
          yield* writeFixtureFile(fixture.files, outside, inviteJson());
          return { invite: outside };
        }),
      message: INVITE_ESCAPES,
    },
    {
      name: 'a traversal out of .invites is refused by path',
      stage: (fixture) =>
        Effect.gen(function* () {
          const outside = path.join(fixture.root, 'escaped.json');
          yield* writeFixtureFile(fixture.files, outside, inviteJson());
          return { invite: '.invites/../escaped.json' };
        }),
      message: INVITE_ESCAPES,
    },
    {
      name: 'a symlinked invite file resolving outside .invites is refused',
      stage: (fixture) =>
        Effect.gen(function* () {
          const outside = path.join(fixture.root, 'linked.json');
          yield* writeFixtureFile(fixture.files, outside, inviteJson());
          const link = path.join(fixture.invites, 'link.json');
          yield* symlink(fixture.files, outside, link);
          return { invite: link };
        }),
      message: INVITE_ESCAPES,
    },
    {
      name: 'mode 0644 is not a credential',
      stage: (fixture) =>
        Effect.map(fixture.write('loose.json', validInvite(), 0o644), (invite) => ({ invite })),
      message: `the invitation for ${GAME_ID} is not mode 0600. ${REMEDY}`,
    },
    {
      name: 'unparseable JSON is unreadable, not a stack trace',
      stage: (fixture) =>
        Effect.map(fixture.writeRaw('broken.json', '{'), (invite) => ({ invite })),
      message: `the invitation for ${GAME_ID} is unreadable. ${REMEDY}`,
    },
    {
      name: 'a JSON array is unreadable too',
      stage: (fixture) =>
        Effect.map(fixture.write('array.json', [1, 2, 3]), (invite) => ({ invite })),
      message: `the invitation for ${GAME_ID} is unreadable. ${REMEDY}`,
    },
    {
      name: 'schema_version 2 is an unsupported schema',
      stage: (fixture) =>
        Effect.map(fixture.write('v2.json', validInvite({ schema_version: 2 })), (invite) => ({
          invite,
        })),
      message: `the invitation for ${GAME_ID} has an unsupported schema. ${REMEDY}`,
    },
    {
      name: 'a missing schema_version is an unsupported schema',
      stage: (fixture) => {
        const { schema_version: _schemaVersion, ...body } = validInvite();
        return Effect.map(fixture.write('noschema.json', body), (invite) => ({ invite }));
      },
      message: `the invitation for ${GAME_ID} has an unsupported schema. ${REMEDY}`,
    },
    {
      name: "another game's invitation is refused by game ID",
      stage: (fixture) =>
        Effect.map(fixture.write('other.json', validInvite({ game_id: OTHER_ID })), (invite) => ({
          invite,
        })),
      message: `the invitation belongs to a different game. ${REMEDY}`,
    },
    {
      name: 'an invitation with no game_id at all belongs to a different game',
      stage: (fixture) => {
        const { game_id: _gameId, ...body } = validInvite();
        return Effect.map(fixture.write('nogame.json', body), (invite) => ({ invite }));
      },
      message: `the invitation belongs to a different game. ${REMEDY}`,
    },
    {
      name: 'a blank join token is invalid',
      stage: (fixture) =>
        Effect.map(fixture.write('blank.json', validInvite({ join_token: '   ' })), (invite) => ({
          invite,
        })),
      message: `the invitation for ${GAME_ID} has an invalid join token. ${REMEDY}`,
    },
    {
      name: 'an untrimmed join token is invalid',
      stage: (fixture) =>
        Effect.map(fixture.write('padded.json', validInvite({ join_token: ' secret ' })), (invite) => ({
          invite,
        })),
      message: `the invitation for ${GAME_ID} has an invalid join token. ${REMEDY}`,
    },
    {
      name: 'a non-string join token is invalid',
      stage: (fixture) =>
        Effect.map(fixture.write('numeric.json', validInvite({ join_token: 12345 })), (invite) => ({
          invite,
        })),
      message: `the invitation for ${GAME_ID} has an invalid join token. ${REMEDY}`,
    },
    {
      name: 'a non-string service URL is invalid',
      stage: (fixture) =>
        Effect.map(fixture.write('nullurl.json', validInvite({ service_url: null })), (invite) => ({
          invite,
        })),
      message: `the invitation for ${GAME_ID} has an invalid service URL. ${REMEDY}`,
    },
    {
      name: 'a service URL carrying credentials is invalid',
      stage: (fixture) =>
        Effect.map(
          fixture.write(
            'creds.json',
            validInvite({ service_url: 'http://user:pass@127.0.0.1:8765' })
          ),
          (invite) => ({ invite })
        ),
      message: `the invitation for ${GAME_ID} has an invalid service URL. ${REMEDY}`,
    },
    {
      name: 'no invitation at all names the owner recovery command',
      stage: () => Effect.succeed({}),
      message: `no join invitation for ${GAME_ID}. ${REMEDY}`,
    },
  ];

  for (const row of rows) {
    inviteTest(row.name, (files) =>
      Effect.gen(function* () {
        const fixture = yield* bench(files, row.invites === false ? { invites: false } : {});
        const overrides = yield* row.stage(fixture);
        expect(refusal(yield* load(fixture.workspace, overrides))).toBe(row.message);
      })
    );
  }

  inviteTest('every refusal names `just invite {game_id}` verbatim', (files) =>
    Effect.gen(function* () {
      for (const row of rows) {
        const fixture = yield* bench(files, row.invites === false ? { invites: false } : {});
        const overrides = yield* row.stage(fixture);
        const message = refusal(yield* load(fixture.workspace, overrides));
        const remediable = message !== INVITE_ROOT_NOT_REAL && message !== INVITE_ESCAPES;
        expect(remediable ? message.includes(`just invite ${GAME_ID}`) : true).toBe(true);
      }
    })
  );
});

// ---------------------------------------------------------------------------

/**
 * `Path.resolve()` expands each symlink as it walks, so a `..` pops a component
 * off the *resolved* prefix.  Normalizing `..` lexically first — which is what
 * `path.resolve` / `path.join` do — silently re-points every one of these paths
 * at a different file, and the escape refusal never fires.  Each expectation
 * below was taken from CPython (`Path(p).resolve()`), not from the port.
 */
describe('containment across a symlinked component', () => {
  /** A real invitation the loader must never read, planted where the escape lands. */
  const plant = (
    files: FileSystem.FileSystem,
    target: string,
    token: string
  ): Effect.Effect<void> => writeFixtureFile(files, target, inviteJson({ join_token: token }));

  inviteTest('`..` through a directory symlink escapes .invites/ and is refused', (files) =>
    Effect.gen(function* () {
      // `.invites/d` -> `<root>/outside`, so `.invites/d/../x.json` is
      // `<root>/x.json`.  Lexically it is `.invites/x.json`, a file that exists
      // and holds a different token — accepting it is a credential swap.
      const fixture = yield* bench(files);
      yield* makeDirectory(files, path.join(fixture.root, 'outside'));
      yield* symlink(files, path.join(fixture.root, 'outside'), path.join(fixture.invites, 'd'));
      yield* plant(files, path.join(fixture.root, 'x.json'), 'escaped-secret');
      yield* fixture.write('x.json', validInvite({ join_token: 'inside-secret' }));
      expect(refusal(yield* load(fixture.workspace, { invite: '.invites/d/../x.json' }))).toBe(
        INVITE_ESCAPES
      );
    })
  );

  inviteTest('it is refused by path, not by the absence of the lexical twin', (files) =>
    Effect.gen(function* () {
      // The same traversal with no `.invites/x.json` at all must still be the
      // escape refusal — "does not exist" would mean the port had looked inside.
      const fixture = yield* bench(files);
      yield* makeDirectory(files, path.join(fixture.root, 'outside'));
      yield* symlink(files, path.join(fixture.root, 'outside'), path.join(fixture.invites, 'd'));
      yield* plant(files, path.join(fixture.root, 'x.json'), 'escaped-secret');
      expect(refusal(yield* load(fixture.workspace, { invite: '.invites/d/../x.json' }))).toBe(
        INVITE_ESCAPES
      );
    })
  );

  inviteTest('a `..` that lands back inside .invites/ is accepted', (files) =>
    Effect.gen(function* () {
      // `.invites/deep` -> `.invites/a/b`, so `.invites/deep/../../keep.json` is
      // `.invites/keep.json`.  Lexical normalization makes it `<root>/keep.json`
      // and refuses a perfectly good invitation — the divergence in the other
      // direction, and just as wrong.
      const fixture = yield* bench(files);
      yield* makeDirectory(files, path.join(fixture.invites, 'a', 'b'), true);
      yield* symlink(files, path.join(fixture.invites, 'a', 'b'), path.join(fixture.invites, 'deep'));
      yield* fixture.write('keep.json', validInvite({ join_token: 'kept-secret' }));
      expect(
        accepted(yield* load(fixture.workspace, { invite: '.invites/deep/../../keep.json' })).token
      ).toBe('kept-secret');
    })
  );

  inviteTest('a relative symlink target is resolved against its own directory', (files) =>
    Effect.gen(function* () {
      const fixture = yield* bench(files);
      yield* makeDirectory(files, path.join(fixture.invites, 'a', 'b'), true);
      yield* symlink(files, 'a/b', path.join(fixture.invites, 'rel'));
      yield* fixture.write('keep.json', validInvite({ join_token: 'relative-target-secret' }));
      expect(
        accepted(yield* load(fixture.workspace, { invite: '.invites/rel/../../keep.json' })).token
      ).toBe('relative-target-secret');
    })
  );

  inviteTest('a symlink to the workspace root is still an escape', (files) =>
    Effect.gen(function* () {
      const fixture = yield* bench(files);
      yield* symlink(files, '..', path.join(fixture.invites, 'up'));
      yield* plant(files, path.join(fixture.root, 'escaped.json'), 'escaped-secret');
      expect(refusal(yield* load(fixture.workspace, { invite: '.invites/up/escaped.json' }))).toBe(
        INVITE_ESCAPES
      );
    })
  );

  inviteTest('a symlink loop terminates as a missing invitation, not a hang', (files) =>
    Effect.gen(function* () {
      // Non-strict resolution keeps the unresolved remainder rather than raising
      // ELOOP, so the path stays inside `.invites/` and fails the `is_file` test.
      const fixture = yield* bench(files);
      yield* symlink(files, 'ping', path.join(fixture.invites, 'pong'));
      yield* symlink(files, 'pong', path.join(fixture.invites, 'ping'));
      expect(refusal(yield* load(fixture.workspace, { invite: '.invites/ping' }))).toBe(
        `the configured invitation for ${GAME_ID} does not exist. ${REMEDY}`
      );
    })
  );

  inviteTest('a symlinked .invites is refused before any of this matters', (files) =>
    Effect.gen(function* () {
      // The root check runs first, so a hostile `.invites` never gets to argue
      // about traversal at all.
      const fixture = yield* bench(files, { invites: false });
      const outside = path.join(fixture.root, 'real-invites');
      yield* makeDirectory(files, outside);
      yield* symlink(files, outside, path.join(fixture.root, '.invites'));
      yield* plant(files, path.join(outside, `${GAME_ID}.json`), 'linked-secret');
      expect(refusal(yield* load(fixture.workspace))).toBe(INVITE_ROOT_NOT_REAL);
    })
  );
});

// ---------------------------------------------------------------------------

/**
 * `str.strip()` is not `String.prototype.trim()`, and this is the loader that
 * cares.  Python strips `\x1c`-`\x1f` and `\x85` and JavaScript does not;
 * JavaScript strips `﻿` and Python does not.  Every expectation below was
 * taken from `client._invite` with a patched `ROOT`, not from the port.
 */
describe("CPython's whitespace class decides who is a credential", () => {
  const SEPARATORS: ReadonlyArray<readonly [string, string]> = [
    ['\x1c', 'FS'],
    ['\x1d', 'GS'],
    ['\x1e', 'RS'],
    ['\x1f', 'US'],
    ['\x85', 'NEL'],
  ];

  for (const [character, label] of SEPARATORS) {
    inviteTest(`a stored join token ending in ${label} is untrimmed-equal and refused`, (files) =>
      Effect.gen(function* () {
        // `.trim()` leaves these alone, so the port used to ACCEPT the file and
        // send `join-secret\x1f` as the bearer — a credential that is not the one
        // CPython would ever send.
        const fixture = yield* bench(files);
        yield* fixture.write(`${GAME_ID}.json`, validInvite({ join_token: `join-secret${character}` }));
        expect(refusal(yield* load(fixture.workspace))).toBe(
          `the invitation for ${GAME_ID} has an invalid join token. ${REMEDY}`
        );
      })
    );

    inviteTest(`AGENT_EVAL_JOIN_TOKEN of a lone ${label} is blank, not an override`, (files) =>
      Effect.gen(function* () {
        // The worst shape of the divergence: treating it as a token skips the
        // default invitation entirely, sends an empty bearer, and joins the
        // invitation's *replaced* origin rather than its declared one.
        const fixture = yield* bench(files);
        yield* fixture.write(`${GAME_ID}.json`, validInvite({ service_url: 'http://127.0.0.1:7777' }));
        expect(
          accepted(yield* load(fixture.workspace, {}, { AGENT_EVAL_JOIN_TOKEN: character }))
        ).toEqual({ token: 'join-secret', base: 'http://127.0.0.1:7777' });
      })
    );

    inviteTest(`a --invite path padded with ${label} still loads`, (files) =>
      Effect.gen(function* () {
        const fixture = yield* bench(files);
        yield* fixture.write('padded-path.json', validInvite({ join_token: 'path-secret' }));
        expect(
          accepted(
            yield* load(fixture.workspace, {
              invite: `${character}.invites/padded-path.json${character}`,
            })
          ).token
        ).toBe('path-secret');
      })
    );

    inviteTest(`a PLAY_INVITE path padded with ${label} still loads`, (files) =>
      Effect.gen(function* () {
        const fixture = yield* bench(files);
        yield* fixture.write('padded-path.json', validInvite({ join_token: 'path-secret' }));
        expect(
          accepted(
            yield* load(
              fixture.workspace,
              {},
              { PLAY_INVITE: `${character}.invites/padded-path.json${character}` }
            )
          ).token
        ).toBe('path-secret');
      })
    );
  }

  inviteTest('a stored join token ending in ZWNBSP is a perfectly good token', (files) =>
    Effect.gen(function* () {
      // The divergence in the other direction: `.trim()` strips `﻿`, so the
      // port used to refuse a file CPython accepts.
      const fixture = yield* bench(files);
      yield* fixture.write(`${GAME_ID}.json`, validInvite({ join_token: 'join-secret\uFEFF' }));
      expect(accepted(yield* load(fixture.workspace)).token).toBe('join-secret\uFEFF');
    })
  );

  inviteTest('AGENT_EVAL_JOIN_TOKEN of a lone ZWNBSP IS an override', (files) =>
    Effect.gen(function* () {
      const fixture = yield* bench(files);
      yield* fixture.write(`${GAME_ID}.json`, validInvite({ service_url: 'http://127.0.0.1:7777' }));
      expect(accepted(yield* load(fixture.workspace, {}, { AGENT_EVAL_JOIN_TOKEN: '\uFEFF' }))).toEqual({
        token: '\uFEFF',
        base: 'http://127.0.0.1:8765',
      });
    })
  );

  inviteTest('a --invite path padded with ZWNBSP is a different path, and escapes', (files) =>
    Effect.gen(function* () {
      const fixture = yield* bench(files);
      yield* fixture.write('padded-path.json', validInvite({ join_token: 'path-secret' }));
      expect(
        refusal(yield* load(fixture.workspace, { invite: '\uFEFF.invites/padded-path.json\uFEFF' }))
      ).toBe(INVITE_ESCAPES);
    })
  );
});

// ---------------------------------------------------------------------------

/**
 * `Path.read_text(encoding="utf-8")` is strict.  Node's `'utf8'` reader is not:
 * it substitutes U+FFFD for an invalid byte, which would turn a malformed
 * credential file into a *different, well-formed* credential.
 */
describe('the file is decoded strictly, or it is unreadable', () => {
  const UNREADABLE = `the invitation for ${GAME_ID} is unreadable. ${REMEDY}`;

  inviteTest('an invalid UTF-8 byte inside join_token is unreadable, not U+FFFD', (files) =>
    Effect.gen(function* () {
      const fixture = yield* bench(files);
      const bytes = Buffer.concat([
        Buffer.from(
          `{"schema_version":1,"game_id":"${GAME_ID}",` +
            '"service_url":"http://127.0.0.1:8765","join_token":"to',
          'utf8'
        ),
        Buffer.from([0xff]),
        Buffer.from('k"}', 'utf8'),
      ]);
      const invite = yield* writeBytes(files, fixture, 'bad.json', bytes);
      expect(refusal(yield* load(fixture.workspace, { invite }))).toBe(UNREADABLE);
    })
  );

  inviteTest('a truncated multi-byte sequence is unreadable', (files) =>
    Effect.gen(function* () {
      const fixture = yield* bench(files);
      const bytes = Buffer.concat([
        Buffer.from(inviteJson().slice(0, -1), 'utf8'),
        Buffer.from([0xe2, 0x82]),
        Buffer.from('}', 'utf8'),
      ]);
      const invite = yield* writeBytes(files, fixture, 'trunc.json', bytes);
      expect(refusal(yield* load(fixture.workspace, { invite }))).toBe(UNREADABLE);
    })
  );

  inviteTest('a leading BOM is kept, so the JSON is unreadable exactly as CPython says', (files) =>
    Effect.gen(function* () {
      // `TextDecoder` strips the BOM by default; the utf-8 codec does not, and
      // `json.loads("\uFEFF{…}")` raises.  Stripping it would ACCEPT a file
      // CPython refuses.
      const fixture = yield* bench(files);
      const bytes = Buffer.concat([
        Buffer.from([0xef, 0xbb, 0xbf]),
        Buffer.from(inviteJson(), 'utf8'),
      ]);
      const invite = yield* writeBytes(files, fixture, 'bom.json', bytes);
      expect(refusal(yield* load(fixture.workspace, { invite }))).toBe(UNREADABLE);
    })
  );

  inviteTest('a well-formed non-ASCII token survives the strict decode', (files) =>
    Effect.gen(function* () {
      const fixture = yield* bench(files);
      yield* fixture.write(`${GAME_ID}.json`, validInvite({ join_token: 'jöin-sécret-✓' }));
      expect(accepted(yield* load(fixture.workspace)).token).toBe('jöin-sécret-✓');
    })
  );
});

// ---------------------------------------------------------------------------

describe('the returned origin', () => {
  inviteTest('a default invitation without a usable URL falls back to the environment', (files) =>
    Effect.gen(function* () {
      // `service_url` is validated as a string but an empty one is `None` to
      // `service_url()`, which then reads AGENT_EVAL_SERVICE_URL.
      const fixture = yield* bench(files);
      yield* fixture.write(`${GAME_ID}.json`, validInvite({ service_url: '' }));
      expect(
        accepted(
          yield* load(fixture.workspace, {}, { AGENT_EVAL_SERVICE_URL: 'https://supervisor.test' })
        ).base
      ).toBe('https://supervisor.test');
    })
  );

  inviteTest('the origin is normalized exactly once, trailing slash and case', (files) =>
    Effect.gen(function* () {
      const fixture = yield* bench(files);
      yield* fixture.write(`${GAME_ID}.json`, validInvite({ service_url: 'HTTP://127.0.0.1:8765/' }));
      expect(accepted(yield* load(fixture.workspace)).base).toBe('http://127.0.0.1:8765');
    })
  );
});
