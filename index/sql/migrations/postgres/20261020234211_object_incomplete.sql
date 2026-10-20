-- Modify "objects" table
ALTER TABLE "objects" ADD COLUMN "incomplete" boolean NOT NULL DEFAULT false;
