-- Create index "files_open_lost" to table: "files"
CREATE INDEX CONCURRENTLY "files_open_lost" ON "files" ("set_id") WHERE (lost AND (valid_until IS NULL));
-- Create index "files_open_sizes" to table: "files"
CREATE INDEX CONCURRENTLY "files_open_sizes" ON "files" ("set_id", "type", "size") WHERE (valid_until IS NULL);
