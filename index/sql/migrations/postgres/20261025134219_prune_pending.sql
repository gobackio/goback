-- Modify "sets" table
ALTER TABLE "sets" ADD COLUMN "prune_pending_since" timestamptz NULL;
