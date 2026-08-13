CREATE TYPE "public"."document_status" AS ENUM('ok', 'unusable', 'absent');--> statement-breakpoint
CREATE TYPE "public"."run_state" AS ENUM('lobby', 'starting', 'running', 'completed', 'invalid', 'failed', 'cancelled');--> statement-breakpoint
CREATE TYPE "public"."seat_kind" AS ENUM('agent', 'cpu');--> statement-breakpoint
CREATE TABLE "agent_stats" (
	"game_id" text NOT NULL,
	"seat_id" text NOT NULL,
	"controller_fingerprint" text,
	"turns" integer,
	"decisions" integer,
	"fallbacks" integer,
	"input_tokens" bigint,
	"output_tokens" bigint,
	"mean_latency_ms" double precision,
	CONSTRAINT "agent_stats_game_id_seat_id_pk" PRIMARY KEY("game_id","seat_id")
);
--> statement-breakpoint
CREATE TABLE "board_state" (
	"game_id" text NOT NULL,
	"turn" integer NOT NULL,
	"board" jsonb NOT NULL,
	CONSTRAINT "board_state_game_id_turn_pk" PRIMARY KEY("game_id","turn")
);
--> statement-breakpoint
CREATE TABLE "games" (
	"game_id" text PRIMARY KEY NOT NULL,
	"state" "run_state" NOT NULL,
	"schema_version" integer,
	"created_at" double precision,
	"started_at" double precision,
	"finished_at" double precision,
	"current_turn" integer,
	"last_replay_turn" integer,
	"benchmark_valid" boolean,
	"name" text,
	"ruleset" text,
	"mode" text,
	"timing_mode" text,
	"max_turns" integer,
	"objective" text,
	"config" jsonb,
	"manifest_status" "document_status" NOT NULL,
	"manifest_byte_size" bigint,
	"report_status" "document_status" NOT NULL,
	"report_byte_size" bigint,
	"content_hash" "bytea" NOT NULL,
	"ingested_at" timestamp with time zone DEFAULT now() NOT NULL,
	"extras" jsonb
);
--> statement-breakpoint
CREATE TABLE "player_turns" (
	"game_id" text NOT NULL,
	"turn" integer NOT NULL,
	"seat_id" text NOT NULL,
	"player_id" integer,
	"player_name" text,
	"nation" text,
	"government" text,
	"alive" boolean,
	"score" double precision,
	"cities" integer,
	"citizens" integer,
	"population" bigint,
	"units" integer,
	"gold" double precision,
	"culture" double precision,
	"future_techs" integer,
	"known_tech_ids" jsonb,
	"research" jsonb,
	CONSTRAINT "player_turns_game_id_turn_seat_id_pk" PRIMARY KEY("game_id","turn","seat_id")
);
--> statement-breakpoint
CREATE TABLE "seats" (
	"game_id" text NOT NULL,
	"seat_index" integer NOT NULL,
	"seat_id" text,
	"kind" "seat_kind",
	"label" text,
	"fingerprint" text,
	"metadata" jsonb,
	CONSTRAINT "seats_game_id_seat_index_pk" PRIMARY KEY("game_id","seat_index")
);
--> statement-breakpoint
CREATE TABLE "turns" (
	"game_id" text NOT NULL,
	"turn" integer NOT NULL,
	"year" integer,
	CONSTRAINT "turns_game_id_turn_pk" PRIMARY KEY("game_id","turn")
);
--> statement-breakpoint
ALTER TABLE "agent_stats" ADD CONSTRAINT "agent_stats_game_id_games_game_id_fk" FOREIGN KEY ("game_id") REFERENCES "public"."games"("game_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "board_state" ADD CONSTRAINT "board_state_game_id_turn_turns_game_id_turn_fk" FOREIGN KEY ("game_id","turn") REFERENCES "public"."turns"("game_id","turn") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "player_turns" ADD CONSTRAINT "player_turns_game_id_turn_turns_game_id_turn_fk" FOREIGN KEY ("game_id","turn") REFERENCES "public"."turns"("game_id","turn") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "seats" ADD CONSTRAINT "seats_game_id_games_game_id_fk" FOREIGN KEY ("game_id") REFERENCES "public"."games"("game_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "turns" ADD CONSTRAINT "turns_game_id_games_game_id_fk" FOREIGN KEY ("game_id") REFERENCES "public"."games"("game_id") ON DELETE cascade ON UPDATE no action;