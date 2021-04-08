/* makeViews.sql 

   Invoke within sqlite3:
sqlite> .read makeViews.sql

   To redefine the views, the old ones must first be removed, e.g.,
sqlite> drop view runview;drop view taskview;drop view sumv1;drop view sumv2;drop view summary

*/

/* create a view of all runs with global numbering */
create view if not exists runview as select
       row_number() over(order by time_began) as runnum,
       run_id,
       strftime('%Y-%m-%d %H:%M:%S',time_began) as began,
       strftime('%Y-%m-%d %H:%M:%S',time_completed) as completed,
       time((julianday(time_completed)-julianday(time_began))*86400,'unixepoch') as runElapsedTime
       from workflow;


/* create a view of all non-cached "transient" tasks based on latest invocation */
create view if not exists nctaskview as select
       rv.runnum,
       t.task_id,
       t.task_func_name as function,
       t.task_fail_count as fails,
       strftime('%Y-%m-%d %H:%M:%S',max(t.task_time_invoked)) as invoked,
       strftime('%Y-%m-%d %H:%M:%S',t.task_time_returned) as returned,
       time((julianday(t.task_time_returned)-julianday(t.task_time_invoked))*86400,'unixepoch') as elapsedTime
       from task t
       join runview rv on (t.run_id=rv.run_id)
       where (t.task_hashsum is null and task_memoize=0)
       group by t.task_func_name;


/* create a view of all (cached) tasks with global numbering based on time of first invocation */
/* ignore uncached parsl apps for now */
create view if not exists taskview as select
       rv.runnum,
       row_number() over(order by task_time_invoked) tasknum,
       t.task_id,
       t.task_hashsum,
       t.task_func_name as function,
       t.task_fail_count as fails,
       strftime('%Y-%m-%d %H:%M:%S',min(t.task_time_invoked)) as invoked,
       strftime('%Y-%m-%d %H:%M:%S',t.task_time_returned) as returned,
       time((julianday(t.task_time_returned)-julianday(t.task_time_invoked))*86400,'unixepoch') as elapsedTime,
       t.task_stdout as stdout
       from task t
       join runview rv on (t.run_id=rv.run_id)
       where t.task_hashsum is not null
       group by t.task_hashsum;


/* create a view containg current status of all invoked (cached) tasks */
/* Part I -- select the most recent "exec_done" for tasks that have gotten that far */
create view if not exists sumv1 as select
       rv.runnum,
       tv.tasknum,
       tv.task_id,
       tv.function,
       s.task_status_name as status,
       strftime('%Y-%m-%d %H:%M:%S',max(s.timestamp)) as lastUpdate,
       tv.fails,
       y.try_id,
       y.hostname,
       strftime('%Y-%m-%d %H:%M:%S',y.task_try_time_launched) as launched,
       strftime('%Y-%m-%d %H:%M:%S',y.task_try_time_running) as start,
       time((julianday(y.task_try_time_running)-julianday(y.task_try_time_launched))*86400,'unixepoch') as waitTime,
       strftime('%Y-%m-%d %H:%M:%S',y.task_try_time_returned) as ended,
       time((julianday(y.task_try_time_returned)-julianday(y.task_try_time_running))*86400,'unixepoch') as runTime

       from taskview tv
       join try y on (rv.run_id=y.run_id and tv.task_id=y.task_id)
       join status s on (rv.run_id=s.run_id and tv.task_id=s.task_id and y.try_id=s.try_id)
       join runview rv on (rv.run_id=y.run_id)
       where tv.task_hashsum is not null and s.task_status_name="exec_done"
       group by tv.task_hashsum
       order by tv.tasknum asc;

/* Part II -- select the most recent status for tasks that are not in the "exec_done" set */
create view if not exists sumv2 as select
       rv.runnum,
       tv.tasknum,
       tv.task_id,
       tv.function,
       s.task_status_name as status,
       strftime('%Y-%m-%d %H:%M:%S',max(s.timestamp)) as lastUpdate,
       tv.fails,
       y.try_id,
       y.hostname,
       strftime('%Y-%m-%d %H:%M:%S',y.task_try_time_launched) as launched,
       strftime('%Y-%m-%d %H:%M:%S',y.task_try_time_running) as start,
       time((julianday(y.task_try_time_running)-julianday(y.task_try_time_launched))*86400,'unixepoch') as waitTime,
       strftime('%Y-%m-%d %H:%M:%S',y.task_try_time_returned) as ended,
       time((julianday(y.task_try_time_returned)-julianday(y.task_try_time_running))*86400,'unixepoch') as runTime

       from taskview tv
       join try y on (rv.run_id=y.run_id and tv.task_id=y.task_id)
       join status s on (rv.run_id=s.run_id and tv.task_id=s.task_id and y.try_id=s.try_id)
       join runview rv on (rv.run_id=y.run_id)
       where tv.task_hashsum is not null
       	     and tv.tasknum not in (select v1.tasknum from sumv1 v1)
       group by tv.task_hashsum
       order by tv.tasknum asc;

/* Put everything together */
create view if not exists summary as
       select * from sumv1
       union
       select * from sumv2
       order by tasknum asc;
