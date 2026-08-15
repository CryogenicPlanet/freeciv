import { FileSystem, Path } from '@effect/platform';
import { BunFileSystem, BunPath } from '@effect/platform-bun';
import { Effect, Layer } from 'effect';
import { provideTestLayer } from 'test/_effect-test';

const pathContext = Effect.runSync(Effect.scoped(Layer.build(BunPath.layer)));
const fileSystemContext = Effect.runSync(Effect.scoped(Layer.build(BunFileSystem.layer)));

/** The platform path service used by tests that need pure path manipulation. */
export const path = Effect.runSync(Effect.provide(Path.Path, pathContext));

/** The Bun-backed platform filesystem used by low-level filesystem fixtures. */
export const fileSystem = Effect.runSync(
  Effect.provide(FileSystem.FileSystem, fileSystemContext)
);

/** Run test filesystem work through Bun's platform FileSystem service. */
export const withTestFileSystem = <A, E, R>(
  body: (files: FileSystem.FileSystem) => Effect.Effect<A, E, R>
): Effect.Effect<A, E, R> =>
  provideTestLayer(Effect.flatMap(FileSystem.FileSystem, body), BunFileSystem.layer);
