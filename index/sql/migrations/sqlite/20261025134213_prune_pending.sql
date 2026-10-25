-- Add column "prune_pending_since" to table: "sets"
ALTER TABLE `sets` ADD COLUMN `prune_pending_since` datetime NULL;
