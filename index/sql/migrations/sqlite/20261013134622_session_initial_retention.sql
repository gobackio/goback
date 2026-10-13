-- Add column "initial_retention" to table: "sessions"
ALTER TABLE `sessions` ADD COLUMN `initial_retention` json NULL;
