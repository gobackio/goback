-- Create "schedules" table
CREATE TABLE "schedules" ("set_name" character varying NOT NULL, "agent_id" character varying NOT NULL, "kind" character varying NOT NULL, "longest_gap" bigint NOT NULL, "jitter" bigint NOT NULL, "commit_every" bigint NOT NULL, "since" timestamptz NOT NULL, "reported_at" timestamptz NOT NULL, PRIMARY KEY ("set_name"));
-- Create index "schedule_agent_id" to table: "schedules"
CREATE INDEX "schedule_agent_id" ON "schedules" ("agent_id");
-- Create index "schedule_reported_at" to table: "schedules"
CREATE INDEX "schedule_reported_at" ON "schedules" ("reported_at");
