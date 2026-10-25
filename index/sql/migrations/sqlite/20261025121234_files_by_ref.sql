-- Create index "files_by_ref" to table: "files"
CREATE INDEX `files_by_ref` ON `files` (`ref`) WHERE ref IS NOT NULL;
