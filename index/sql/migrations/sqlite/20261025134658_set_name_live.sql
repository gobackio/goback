-- Disable the enforcement of foreign-keys constraints
PRAGMA foreign_keys = off;
-- Create "new_sets" table
CREATE TABLE `new_sets` (`id` integer NOT NULL PRIMARY KEY AUTOINCREMENT, `name` text NOT NULL, `state` text NOT NULL DEFAULT ('active'), `retention_policy` text NULL, `retention_paused` bool NOT NULL DEFAULT (false), `erase` bool NOT NULL DEFAULT (false), `rescan` bool NOT NULL DEFAULT (false), `physical_size` integer NULL, `deduplicated_size` integer NULL, `alone_size` integer NULL, `exclusive_size` integer NULL, `deduplicated_alone_size` integer NULL, `logical_size` integer NULL, `unique_size` integer NULL, `sizes_digest` blob NULL, `prune_pending_since` datetime NULL);
-- Copy rows from old table "sets" to new temporary table "new_sets"
INSERT INTO `new_sets` (`id`, `name`, `state`, `retention_policy`, `retention_paused`, `erase`, `rescan`, `physical_size`, `deduplicated_size`, `alone_size`, `exclusive_size`, `deduplicated_alone_size`, `logical_size`, `unique_size`, `sizes_digest`, `prune_pending_since`) SELECT `id`, `name`, `state`, `retention_policy`, `retention_paused`, `erase`, `rescan`, `physical_size`, `deduplicated_size`, `alone_size`, `exclusive_size`, `deduplicated_alone_size`, `logical_size`, `unique_size`, `sizes_digest`, `prune_pending_since` FROM `sets`;
-- Drop "sets" table after copying rows
DROP TABLE `sets`;
-- Rename temporary table "new_sets" to "sets"
ALTER TABLE `new_sets` RENAME TO `sets`;
-- Create index "set_name" to table: "sets"
CREATE UNIQUE INDEX `set_name` ON `sets` (`name`) WHERE state <> 'deleted';
-- Create index "sets_by_size" to table: "sets"
CREATE INDEX `sets_by_size` ON `sets` (`physical_size` DESC, `name`);
-- Enable back the enforcement of foreign-keys constraints
PRAGMA foreign_keys = on;
