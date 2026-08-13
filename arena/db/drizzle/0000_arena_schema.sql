CREATE TYPE "public"."document_kind" AS ENUM('manifest', 'report', 'victory');--> statement-breakpoint
CREATE TYPE "public"."document_status" AS ENUM('ok', 'unusable');--> statement-breakpoint
CREATE TYPE "public"."save_kind" AS ENUM('autosave', 'ppm', 'other');--> statement-breakpoint
CREATE TABLE "derivation_cache" (
	"cache_key" text NOT NULL,
	"game_id" text NOT NULL,
	"entry_name" text NOT NULL,
	"bytes" "bytea" NOT NULL,
	"byte_size" integer NOT NULL,
	"written_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "derivation_cache_cache_key_game_id_entry_name_pk" PRIMARY KEY("cache_key","game_id","entry_name")
);
--> statement-breakpoint
CREATE TABLE "derivation_workdirs" (
	"runs_root" text NOT NULL,
	"game_id" text NOT NULL,
	"path" text NOT NULL,
	"saves_hash" "bytea" NOT NULL,
	"materialized_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "derivation_workdirs_runs_root_game_id_pk" PRIMARY KEY("runs_root","game_id")
);
--> statement-breakpoint
CREATE TABLE "ingest_sweeps" (
	"sweep_id" bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "ingest_sweeps_sweep_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1),
	"runs_root" text NOT NULL,
	"started_at" timestamp with time zone DEFAULT now() NOT NULL,
	"finished_at" timestamp with time zone,
	"status" text NOT NULL,
	"seen_count" integer DEFAULT 0 NOT NULL
);
--> statement-breakpoint
CREATE TABLE "run_documents" (
	"runs_root" text NOT NULL,
	"game_id" text NOT NULL,
	"kind" "document_kind" NOT NULL,
	"status" "document_status" NOT NULL,
	"bytes" "bytea" NOT NULL,
	"byte_size" integer NOT NULL,
	"sha256" "bytea" NOT NULL,
	CONSTRAINT "run_documents_runs_root_game_id_kind_pk" PRIMARY KEY("runs_root","game_id","kind")
);
--> statement-breakpoint
CREATE TABLE "run_frames" (
	"runs_root" text NOT NULL,
	"game_id" text NOT NULL,
	"name" text NOT NULL,
	"frame_index" integer,
	"bytes" "bytea" NOT NULL,
	"byte_size" integer NOT NULL,
	"sha256" "bytea" NOT NULL,
	CONSTRAINT "run_frames_runs_root_game_id_name_pk" PRIMARY KEY("runs_root","game_id","name")
);
--> statement-breakpoint
CREATE TABLE "run_replay_tail" (
	"runs_root" text NOT NULL,
	"game_id" text NOT NULL,
	"byte_size" bigint NOT NULL,
	"tail_bytes" "bytea" NOT NULL,
	"sha256" "bytea" NOT NULL,
	CONSTRAINT "run_replay_tail_runs_root_game_id_pk" PRIMARY KEY("runs_root","game_id")
);
--> statement-breakpoint
CREATE TABLE "run_saves" (
	"runs_root" text NOT NULL,
	"game_id" text NOT NULL,
	"name" text NOT NULL,
	"kind" "save_kind" NOT NULL,
	"save_turn" bigint,
	"byte_size" bigint NOT NULL,
	"head_bytes" "bytea" NOT NULL,
	"is_whole" boolean NOT NULL,
	"sha256" "bytea" NOT NULL,
	CONSTRAINT "run_saves_runs_root_game_id_name_pk" PRIMARY KEY("runs_root","game_id","name")
);
--> statement-breakpoint
CREATE TABLE "run_videos" (
	"runs_root" text NOT NULL,
	"game_id" text NOT NULL,
	"bytes" "bytea" NOT NULL,
	"byte_size" integer NOT NULL,
	"sha256" "bytea" NOT NULL,
	CONSTRAINT "run_videos_runs_root_game_id_pk" PRIMARY KEY("runs_root","game_id")
);
--> statement-breakpoint
CREATE TABLE "runs" (
	"runs_root" text NOT NULL,
	"game_id" text NOT NULL,
	"content_hash" "bytea" NOT NULL,
	"ingested_at" timestamp with time zone DEFAULT now() NOT NULL,
	"sweep_id" bigint NOT NULL,
	"frames_dir_ok" boolean NOT NULL,
	"saves_dir_ok" boolean NOT NULL,
	"state" text,
	"created_at" double precision,
	"finished_at" double precision,
	"benchmark_valid" boolean,
	CONSTRAINT "runs_runs_root_game_id_pk" PRIMARY KEY("runs_root","game_id")
);
--> statement-breakpoint
ALTER TABLE "run_documents" ADD CONSTRAINT "run_documents_runs_root_game_id_runs_runs_root_game_id_fk" FOREIGN KEY ("runs_root","game_id") REFERENCES "public"."runs"("runs_root","game_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "run_frames" ADD CONSTRAINT "run_frames_runs_root_game_id_runs_runs_root_game_id_fk" FOREIGN KEY ("runs_root","game_id") REFERENCES "public"."runs"("runs_root","game_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "run_replay_tail" ADD CONSTRAINT "run_replay_tail_runs_root_game_id_runs_runs_root_game_id_fk" FOREIGN KEY ("runs_root","game_id") REFERENCES "public"."runs"("runs_root","game_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "run_saves" ADD CONSTRAINT "run_saves_runs_root_game_id_runs_runs_root_game_id_fk" FOREIGN KEY ("runs_root","game_id") REFERENCES "public"."runs"("runs_root","game_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "run_videos" ADD CONSTRAINT "run_videos_runs_root_game_id_runs_runs_root_game_id_fk" FOREIGN KEY ("runs_root","game_id") REFERENCES "public"."runs"("runs_root","game_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "runs" ADD CONSTRAINT "runs_sweep_id_ingest_sweeps_sweep_id_fk" FOREIGN KEY ("sweep_id") REFERENCES "public"."ingest_sweeps"("sweep_id") ON DELETE no action ON UPDATE no action;