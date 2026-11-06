-- Add column "goback_version" to table: "commits"
ALTER TABLE `commits` ADD COLUMN `goback_version` text NULL;
