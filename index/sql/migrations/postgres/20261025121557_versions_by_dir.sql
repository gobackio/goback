-- Create index "files_by_dir" to table: "files"
CREATE INDEX CONCURRENTLY "files_by_dir" ON "files" ("set_id", "dir", "valid_until");
-- Create index "trees_by_dir" to table: "trees"
CREATE INDEX CONCURRENTLY "trees_by_dir" ON "trees" ("set_id", "dir", "valid_until");
