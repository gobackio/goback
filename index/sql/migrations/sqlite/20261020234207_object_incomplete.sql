-- Modify "objects" table
ALTER TABLE `objects` ADD COLUMN `incomplete` bool NOT NULL DEFAULT (false);
