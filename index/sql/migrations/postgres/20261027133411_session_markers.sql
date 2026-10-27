-- Create "session_markers" table
CREATE TABLE "session_markers" ("id" character varying NOT NULL, "agent_id" character varying NOT NULL, "backup_set" character varying NOT NULL, "restore" bytea NULL, "started" timestamptz NOT NULL, "sealed" bigint NOT NULL DEFAULT 0, "outcome" character varying NULL, "ended_at" timestamptz NULL, PRIMARY KEY ("id"));
