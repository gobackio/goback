-- Create "schedules" table
CREATE TABLE `schedules` (`set_name` text NOT NULL, `agent_id` text NOT NULL, `kind` text NOT NULL, `longest_gap` integer NOT NULL, `jitter` integer NOT NULL, `commit_every` integer NOT NULL, `since` datetime NOT NULL, `reported_at` datetime NOT NULL, PRIMARY KEY (`set_name`));
-- Create index "schedule_agent_id" to table: "schedules"
CREATE INDEX `schedule_agent_id` ON `schedules` (`agent_id`);
-- Create index "schedule_reported_at" to table: "schedules"
CREATE INDEX `schedule_reported_at` ON `schedules` (`reported_at`);
