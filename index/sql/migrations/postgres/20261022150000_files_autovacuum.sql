-- Vacuum "files" after 1% of its rows change, so the visibility map stays
-- current and a set's size is summed from files_open_sizes alone
ALTER TABLE "files" SET (autovacuum_vacuum_scale_factor = 0.01, autovacuum_vacuum_insert_scale_factor = 0.01);
