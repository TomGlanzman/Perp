/* improveDB.sql 

  This script is intended to make various performance improvements to
  a Parsl-produced monitoring.db file (sqlite3).

  At some point some or all of these may be incorporated into Parsl
  itself.

     KEY conclusions for Parsl and its use of sqlite3:

     1. Add the task_hashsum index

     2. Run ANALYZE and/or PRAGMA optimize periodically and just before shutting down

     3. Run VACUUM

     4. There may be other optimizations to consider, such as:

     	a) Add foreign keys to certain tables that lack them (e.g.,
     	run_id in the block table) Note that with sqlite3, a foreign
     	key must be defined at the time of table creation (the "alter
     	table add constraint" command is not supported).  The
     	following does NOT work with sqlite3:

ALTER TABLE child ADD CONSTRAINT fk_child_parent
                  FOREIGN KEY (parent_id) 
                  REFERENCES parent(id);



        Instead, one can create a new column with the desired
        property, copy the old column to the new column, delete the
        old column, rename the new column:

ALTER TABLE child ADD COLUMN parent_id INTEGER REFERENCES parent(id);

        Or create the key when the table is defined:



CREATE TABLE child ( 
    id           INTEGER PRIMARY KEY, 
    parent_id    INTEGER, 
    description  TEXT,
    FOREIGN KEY (parent_id) REFERENCES parent(id)
);


        Or, create a new table, say "newchild" with the desired key.  Then,







*/
/*alter table task add column task_fail_cost float default 0.0;*/

/* These commands significantly improve performance */
analyze;
vacuum;
create index 'task_hashsum_x' on 'task'('task_hashsum')

