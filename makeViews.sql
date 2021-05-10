/* makeViews.sql 

   Invoke within sqlite3:
sqlite> .read makeViews.sql

   To redefine the views, the old ones must first be removed, e.g.,
sqlite> drop view runview;drop view taskview;drop view sumv1;drop view sumv2;drop view summary

*/

/* create a view of all runs with global numbering */
create temporary view if not exists runview as select
       row_number() over(order by time_began) as runnum,
       run_id,
       strftime('%Y-%m-%d %H:%M:%S',time_began) as began,
       strftime('%Y-%m-%d %H:%M:%S',time_completed) as completed,
       time((julianday(time_completed)-julianday(time_began))*86400,'unixepoch') as runElapsedTime
       from workflow;


/* create a view of all non-cached "transient" tasks based on latest invocation */
create temporary view if not exists nctaskview as select
       rv.runnum,
       t.task_id,
       t.task_func_name as appname,
       t.task_fail_count as fails,
       s.task_status_name as status,
       strftime('%Y-%m-%d %H:%M:%S',max(s.timestamp)) as lastUpdate,
       strftime('%Y-%m-%d %H:%M:%S',(t.task_time_invoked)) as invoked,
       strftime('%Y-%m-%d %H:%M:%S',t.task_time_returned) as returned,
       time((julianday(t.task_time_returned)-julianday(t.task_time_invoked))*86400,'unixepoch') as elapsedTime,
       t.task_memoize as cached,
       y.task_joins as joins,
       t.task_depends as depends
       from task t
       join try y on (rv.run_id=y.run_id and t.task_id=y.task_id)
       join status s on (y.run_id=s.run_id and y.task_id=s.task_id and y.try_id=s.try_id)
       join runview rv on (t.run_id=rv.run_id)
       where (t.task_hashsum is null and task_memoize=0)
       group by t.task_func_name
       order by t.task_memoize,t.task_id;

/* create a view of non-dispatched cached tasks (not yet achieved "pending" state) */
/*  NOTE: python_apps are never "dispatched" */
/*  NOTE: this category of tasks _may_ disappear at some point */
create temporary view if not exists ndtaskview as select
       rv.runnum,
       t.task_id,
       t.task_func_name as appname,
       t.task_fail_count as fails,
       s.task_status_name as status,
       strftime('%Y-%m-%d %H:%M:%S',max(s.timestamp)) as statusUpdate,
       strftime('%Y-%m-%d %H:%M:%S',(t.task_time_invoked)) as invoked,
       strftime('%Y-%m-%d %H:%M:%S',t.task_time_returned) as returned,
       time((julianday(t.task_time_returned)-julianday(t.task_time_invoked))*86400,'unixepoch') as elapsedTime,
       t.task_memoize as cached,
       y.task_joins as joins,
       t.task_depends as depends
       from task t
       join try y on (rv.run_id=y.run_id and t.task_id=y.task_id)
       join status s on (y.run_id=s.run_id and y.task_id=s.task_id and y.try_id=s.try_id)
       join runview rv on (t.run_id=rv.run_id)
       where (t.task_hashsum is null and task_memoize=1)
       group by t.task_id,t.run_id
       order by t.task_memoize,t.task_id;


/* create a view of all (cached) tasks with global numbering based on time of first invocation */
/* ignore uncached parsl apps for now */
create temporary view if not exists taskview as select
       rv.runnum,
       row_number() over(order by task_time_invoked) tasknum,
       t.task_id,
       t.task_hashsum,
       t.task_func_name as appname,
       t.task_fail_count as fails,
       strftime('%Y-%m-%d %H:%M:%S',min(t.task_time_invoked)) as invoked,
       strftime('%Y-%m-%d %H:%M:%S',t.task_time_returned) as returned,
       time((julianday(t.task_time_returned)-julianday(t.task_time_invoked))*86400,'unixepoch') as elapsedTime,
       t.task_depends as depends,
       t.task_stdout as stdout
       from task t
       join runview rv on (t.run_id=rv.run_id)
       where t.task_hashsum is not null
       group by t.task_hashsum;


/* create a view containg current status of all invoked (cached) tasks */
/* Part I -- select the most recent "exec_done" for tasks that have gotten that far */
create temporary view if not exists sumv1 as select
       rv.runnum,
       tv.tasknum,
       s.task_id,
       tv.appname,
       s.task_status_name as status,
       strftime('%Y-%m-%d %H:%M:%S',max(s.timestamp)) as lastUpdate,
       t.task_fail_count as fails,
       y.try_id,
       y.hostname,
       strftime('%Y-%m-%d %H:%M:%S',y.task_try_time_launched) as launched,
       strftime('%Y-%m-%d %H:%M:%S',y.task_try_time_running) as start,
       time((julianday(y.task_try_time_running)-julianday(y.task_try_time_launched))*86400,'unixepoch') as waitTime,
       strftime('%Y-%m-%d %H:%M:%S',y.task_try_time_returned) as ended,
       time((julianday(y.task_try_time_returned)-julianday(y.task_try_time_running))*86400,'unixepoch') as runTime,
       y.task_joins,
       tv.depends,
       tv.stdout
       from task t
       join runview rv on (rv.run_id=t.run_id)
       join taskview tv on (t.task_hashsum=tv.task_hashsum)
       join try y on (t.run_id=y.run_id and t.task_id=y.task_id)
       join status s on (t.run_id=s.run_id and t.task_id=s.task_id and y.try_id=s.try_id)
       where tv.task_hashsum is not null and s.task_status_name="exec_done"
       group by tv.task_hashsum
       order by tv.tasknum asc;

/* Part II -- select the most recent status for tasks that are not in the "exec_done" set */
create temporary view if not exists sumv2 as select
       rv.runnum,
       tv.tasknum,
       s.task_id,
       tv.appname,
       s.task_status_name as status,
       strftime('%Y-%m-%d %H:%M:%S',max(s.timestamp)) as lastUpdate,
       t.task_fail_count as fails,
       y.try_id,
       y.hostname,
       strftime('%Y-%m-%d %H:%M:%S',y.task_try_time_launched) as launched,
       strftime('%Y-%m-%d %H:%M:%S',y.task_try_time_running) as start,
       time((julianday(y.task_try_time_running)-julianday(y.task_try_time_launched))*86400,'unixepoch') as waitTime,
       strftime('%Y-%m-%d %H:%M:%S',y.task_try_time_returned) as ended,
       time((julianday(y.task_try_time_returned)-julianday(y.task_try_time_running))*86400,'unixepoch') as runTime,
       y.task_joins,
       tv.depends,
       tv.stdout
       from task t
       join runview rv on (rv.run_id=t.run_id)
       join taskview tv on (t.task_hashsum=tv.task_hashsum)
       join try y on (t.run_id=y.run_id and t.task_id=y.task_id)
       join status s on (t.run_id=s.run_id and t.task_id=s.task_id and y.try_id=s.try_id)
       where tv.task_hashsum is not null
       	     and tv.tasknum not in (select v1.tasknum from sumv1 v1)
       group by tv.task_hashsum
       order by tv.tasknum asc;


/* Put everything together */
create temporary view if not exists summary as
       select * from sumv1
       union
       select * from sumv2
       order by tasknum asc;

/* Table of all batch jobs with run times */
create temporary view if not exists blockview as select
       rv.runnum,
       b.block_id,
       b.job_id,
       b.executor_label as xtor,
       strftime('%Y-%m-%d %H:%M:%S',min(b.timestamp)) as submitted,
       br.startRun,
       br.endRun,
       br.runTime,
       bc.completed,
       time((julianday(bc.completed)-julianday(strftime('%Y-%m-%d %H:%M:%S',min(b.timestamp))))*86400,'unixepoch') as elapsedTime,
       b.status,
       br.status,
       bc.status
       from block b
       join runview rv on (rv.run_id=b.run_id)
       left join blocksrunning br on (rv.runnum=br.runnum and br.block_id=b.block_id and br.job_id=b.job_id)
       left join blockscompleted bc on (rv.runnum=bc.runnum and bc.block_id=b.block_id and bc.job_id=b.job_id)
       where b.status='PENDING'
       group by b.job_id
       order by b.job_id;


create temporary view if not exists blocksrunning as select
       rv.runnum,
       b.block_id,
       b.job_id,
       b.executor_label xtor,
       strftime('%Y-%m-%d %H:%M:%S',min(b.timestamp)) as startRun,
       strftime('%Y-%m-%d %H:%M:%S',max(b.timestamp)) as endRun,
       time((julianday(max(timestamp))-julianday(min(timestamp)))*86400,'unixepoch') as runTime,
       b.status
       from block b
       join runview rv on b.run_id=rv.run_id
       where b.status='RUNNING'
       group by b.run_id,b.block_id,b.job_id
       order by rv.runnum,b.block_id,b.job_id asc;


create temporary view if not exists blockscompleted as select
       rv.runnum,
       b.block_id,
       b.job_id,
       b.executor_label xtor,
       strftime('%Y-%m-%d %H:%M:%S',min(b.timestamp)) as completed,
       b.status
       from block b
       join runview rv on b.run_id=rv.run_id
       where b.status='COMPLETED'
       group by b.run_id,b.block_id,b.job_id
       order by rv.runnum,b.block_id,b.job_id asc;

