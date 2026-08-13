-- Storage tuning for the columns that hold already-compressed payloads.
--
-- Postgres' default for a `bytea` column is EXTENDED: try LZ compression first,
-- then move the value out of line into TOAST. For a PNG (deflate), an H.264
-- MP4 and a gzipped savegame that compression pass burns CPU on every write and
-- reliably fails to shrink anything, so those three columns are set to EXTERNAL
-- — out of line, uncompressed. It also makes a substring read of a large value
-- cheap, which is what a future chunked video reader would want.
--
-- The JSON documents (`run_documents.bytes`, `run_replay_tail.tail_bytes`,
-- `derivation_cache.bytes`) are deliberately left EXTENDED: they are JSON text
-- and compress roughly 4x.
--
-- drizzle-kit does not model column storage, so this is a `--custom` migration.
-- It changes no schema shape and therefore never conflicts with a generated one.

ALTER TABLE "run_frames" ALTER COLUMN "bytes" SET STORAGE EXTERNAL;--> statement-breakpoint
ALTER TABLE "run_videos" ALTER COLUMN "bytes" SET STORAGE EXTERNAL;--> statement-breakpoint
ALTER TABLE "run_saves" ALTER COLUMN "head_bytes" SET STORAGE EXTERNAL;
