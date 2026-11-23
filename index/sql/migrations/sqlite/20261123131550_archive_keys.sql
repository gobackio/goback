-- Disable the enforcement of foreign-keys constraints
PRAGMA foreign_keys = off;
-- Create "new_archives" table
CREATE TABLE `new_archives` (`id` integer NOT NULL PRIMARY KEY AUTOINCREMENT, `name` text NOT NULL, `state` integer NOT NULL DEFAULT (0), `opened_at` datetime NULL, `created_at` datetime NULL, `session_id` text NULL, CONSTRAINT `archives_sessions_archives` FOREIGN KEY (`session_id`) REFERENCES `sessions` (`id`) ON DELETE CASCADE);
-- Copy rows from old table "archives" to new temporary table "new_archives", the old id becoming the name
INSERT INTO `new_archives` (`name`, `state`, `opened_at`, `created_at`, `session_id`) SELECT `id`, `state`, `opened_at`, `created_at`, `session_id` FROM `archives`;
-- Drop "archives" table after copying rows
DROP TABLE `archives`;
-- Rename temporary table "new_archives" to "archives"
ALTER TABLE `new_archives` RENAME TO `archives`;
-- Create index "archives_name_key" to table: "archives"
CREATE UNIQUE INDEX `archives_name_key` ON `archives` (`name`);
-- Create index "archives_session" to table: "archives"
CREATE INDEX `archives_session` ON `archives` (`session_id`) WHERE session_id IS NOT NULL;
-- Create "new_objects" table
CREATE TABLE `new_objects` (`id` integer NOT NULL PRIMARY KEY AUTOINCREMENT, `start` integer NOT NULL, `length` integer NOT NULL, `type` integer NOT NULL, `carried_time` integer NULL, `carried_offset` integer NULL, `incomplete` bool NOT NULL DEFAULT (false), `ref` blob NOT NULL, `archive_id` integer NOT NULL, CONSTRAINT `objects_archives_objects` FOREIGN KEY (`archive_id`) REFERENCES `archives` (`id`) ON DELETE CASCADE, CONSTRAINT `objects_ref_width` CHECK (length(ref) = 32));
-- Copy rows from old table "objects" to new temporary table "new_objects", naming each archive by its key
INSERT INTO `new_objects` (`id`, `start`, `length`, `type`, `carried_time`, `carried_offset`, `incomplete`, `ref`, `archive_id`) SELECT o.`id`, o.`start`, o.`length`, o.`type`, o.`carried_time`, o.`carried_offset`, o.`incomplete`, o.`ref`, a.`id` FROM `objects` o JOIN `archives` a ON a.`name` = o.`archive_id`;
-- Drop "objects" table after copying rows
DROP TABLE `objects`;
-- Rename temporary table "new_objects" to "objects"
ALTER TABLE `new_objects` RENAME TO `objects`;
-- Create index "objects_archive_ref" to table: "objects"
CREATE UNIQUE INDEX `objects_archive_ref` ON `objects` (`archive_id`, `ref`);
-- Create index "objects_ref" to table: "objects"
CREATE INDEX `objects_ref` ON `objects` (`ref`);
-- Create index "objects_tombstones" to table: "objects"
CREATE INDEX `objects_tombstones` ON `objects` (`ref`) WHERE type = 5;
-- Create index "objects_commits" to table: "objects"
CREATE INDEX `objects_commits` ON `objects` (`ref`) WHERE type = 1;
-- Enable back the enforcement of foreign-keys constraints
PRAGMA foreign_keys = on;
