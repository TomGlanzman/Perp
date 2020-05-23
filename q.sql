/* q.sql - query monitoring.db for task info for selected run 
   This can be executed from the sqlite3 shell using the
   command:  sqlite> .read q.sql
*/
/* Runs table snapshot
+----------+--------------------------------------+
|   RunNum | parsl run_id                         |
|----------+--------------------------------------|
|      000 | c1cfebb4-f01a-4311-87bb-85ecc909ff80 |
|      001 | 93505e5f-b94c-472c-a4a9-f0dd6fb9b0fc |
|      002 | 1e17f644-d29e-44b8-a0e7-8b6e916b9ae2 |
+----------+--------------------------------------+
*/

 
/* This query finds the task summary of the *CURRENT STATE*, and includes data from current and previous runs to get execution statistics, as necessary */

select t.run_id,t.task_id,y.try_id,y.hostname,y.task_time_submitted,max(s.timestamp),s.task_status_name,t.task_func_name
   from task t
      join try y on (t.run_id=y.run_id AND t.task_id=y.task_id)
      join status s on (y.run_id=s.run_id AND y.task_id=s.task_id AND y.try_id=s.try_id)
   where s.task_status_name!="memo_done" 
   group by t.task_id
   order by t.task_id asc;


/* This query finds the *FULL HISTORY* history for each task */

select t.run_id,t.task_id,y.try_id,y.task_time_submitted,s.timestamp,s.task_status_name,t.task_func_name
   from task t
      join try y on (t.run_id=y.run_id AND t.task_id=y.task_id)
      join status s on (y.run_id=s.run_id AND y.task_id=s.task_id AND y.try_id=s.try_id)
   where t.task_id>=0 AND t.task_id<2
   order by t.task_id,s.timestamp asc;
