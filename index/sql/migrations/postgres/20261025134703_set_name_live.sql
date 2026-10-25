-- Drop index "set_name" from table: "sets"
DROP INDEX "set_name";
-- Create index "set_name" to table: "sets"
CREATE UNIQUE INDEX "set_name" ON "sets" ("name") WHERE ((state)::text <> 'deleted'::text);
