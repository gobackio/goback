-- Create "session_markers" table
CREATE TABLE `session_markers` (`id` text NOT NULL, `agent_id` text NOT NULL, `backup_set` text NOT NULL, `restore` blob NULL, `started` datetime NOT NULL, `sealed` integer NOT NULL DEFAULT (0), `outcome` text NULL, `ended_at` datetime NULL, PRIMARY KEY (`id`));
