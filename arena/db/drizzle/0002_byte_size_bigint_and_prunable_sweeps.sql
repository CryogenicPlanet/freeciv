ALTER TABLE "runs" DROP CONSTRAINT "runs_sweep_id_ingest_sweeps_sweep_id_fk";
--> statement-breakpoint
ALTER TABLE "derivation_cache" ALTER COLUMN "byte_size" SET DATA TYPE bigint;--> statement-breakpoint
ALTER TABLE "run_documents" ALTER COLUMN "byte_size" SET DATA TYPE bigint;--> statement-breakpoint
ALTER TABLE "run_frames" ALTER COLUMN "byte_size" SET DATA TYPE bigint;--> statement-breakpoint
ALTER TABLE "run_videos" ALTER COLUMN "byte_size" SET DATA TYPE bigint;--> statement-breakpoint
ALTER TABLE "runs" ALTER COLUMN "sweep_id" DROP NOT NULL;--> statement-breakpoint
ALTER TABLE "runs" ADD CONSTRAINT "runs_sweep_id_ingest_sweeps_sweep_id_fk" FOREIGN KEY ("sweep_id") REFERENCES "public"."ingest_sweeps"("sweep_id") ON DELETE set null ON UPDATE no action;