/* updateDB.sql 

  Update an old version of the monitoring.db schema by adding in missing columns

  Optimize monitoring.db files using experimentally determined sql magic

  The incantation below works for "Spring 2021" era monitoring.db
  files, bringing them up to "Summer 2021" standards.

*/
alter table task add column task_fail_cost float default 0.0;
analyze;
vacuum;
