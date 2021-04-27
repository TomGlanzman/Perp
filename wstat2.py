#!/usr/bin/env python3
## wstat.py - workflow status summary from Parsl monitoring database

## The idea is not to replace the "sqlite3" interactive command or the
## Parsl web interface, but to complement them to create some useful
## interactive summaries specific to Parsl workflows.

## Python dependencies: sqlite3, tabulate, matplotlib

## T.Glanzman - Spring 2019
__version__ = "2.0.0beta"  # 4/1/2021
pVersion='1.1.0:desc'    ## Parsl version

import sys,os
import sqlite3
from tabulate import tabulate
import datetime
import argparse
import matplotlib.pyplot as plt
import pandas as pd

## Table format is used by 'tabulate' to select the text-based output format
## 'grid' looks nice but is non-compact
## 'psql' looks almost as nice and is more compact
tblfmt = 'psql'


## Selection of SQL commands used within pmon

stdVariables = (
       'rv.runnum,'
       'tv.tasknum,'
       's.task_id,'
       'tv.appname,'
       's.task_status_name as status,'
       "strftime('%Y-%m-%d %H:%M:%S',s.timestamp) as timestamp,"
       'tv.fails,'
       'y.try_id,'
       'y.hostname,'
       "strftime('%Y-%m-%d %H:%M:%S',y.task_try_time_launched) as launched,"
       "strftime('%Y-%m-%d %H:%M:%S',y.task_try_time_running) as start,"
       "time((julianday(y.task_try_time_running)-julianday(y.task_try_time_launched))*86400,'unixepoch') as waitTime,"
       "strftime('%Y-%m-%d %H:%M:%S',y.task_try_time_returned) as ended,"
       "time((julianday(y.task_try_time_returned)-julianday(y.task_try_time_running))*86400,'unixepoch') as runTime "
    )

stdSources = (
    'from task t '
    'join runview rv on (rv.run_id=t.run_id) '
    'join taskview tv on t.task_hashsum=tv.task_hashsum '
    'join try y on (t.run_id=y.run_id and t.task_id=y.task_id) '
    'join status s on (t.run_id=s.run_id and t.task_id=s.task_id and y.try_id=s.try_id) '
    )

taskHistoryQuery = (
    'select '+stdVariables+stdSources+
    'where #morewhere# '
    'order by rv.runnum,tv.tasknum,s.timestamp asc ')

recentStatusQuery = ('select '+stdVariables+stdSources+
       'order by s.timestamp desc '
       'limit #limit# ')


## Collect runtime statistics for plotting: waitTime, runTime, and
## total elapsedTime (=waitTime+runTime).  Data can be per task (via
## tv.task_hashsum) or task type (via tv.appname) depending on the
## #groupby# choice. Time durations are returned in minutes.

plotStats = (
    'select '
    'tv.tasknum, '
    'tv.appname, '
    'count(y.try_id) as numTries, '
    'y.task_try_time_launched as launchTime, '
    'y.task_try_time_running as startTime, '
    'y.task_try_time_returned as endTime, '
    "sum(julianday(y.task_try_time_running)-julianday(y.task_try_time_launched))*1440 as waitTime, "
    "sum(julianday(y.task_try_time_returned)-julianday(y.task_try_time_running))*1440 as runTime, "
    "sum(julianday(y.task_try_time_returned)-julianday(y.task_try_time_launched))*1440 as elapsedTime "
    'from try y '
    'join task t on (t.run_id=y.run_id and t.task_id=y.task_id) '
    'join taskview tv on (tv.task_hashsum=t.task_hashsum) '
    'group by #groupby# '
    'order by tv.tasknum asc '
    )





class pmon:
    ### class pmon - read & interpret Parsl monitoring database
    def __init__(self,dbfile='monitoring.db',debug=0):
        ## Instance variables
        self.dbfile = dbfile
        self.debug = debug # [0=none,1=short(trace),2=more,3=even more,5=lengthy tables]
        self.PerpDir = os.path.dirname(os.path.realpath(__file__))

        ## sqlite3 database initialization
        self.con = sqlite3.connect(self.dbfile,
                                   timeout=30,             ## time limit if DB locked
                                   detect_types=sqlite3.PARSE_DECLTYPES |
                                   sqlite3.PARSE_COLNAMES) ## special connect to sqlite3 file
        self.con.row_factory = sqlite3.Row                 ## optimize output format
        self.cur = self.con.cursor()                       ## create a 'cursor'

        ## List of all task states defined by Parsl
        self.statList = ['pending','launched','running','joining','running_ended','unsched','unknown','exec_done','memo_done','failed','dep_fail','fail_retryable']

        ## Build initial task state tally dictionary, {<state>:<#tasks in that state>}
        self.statTemplate = {}
        for state in self.statList:
            self.statTemplate[state] = 0
            pass
        
        self.statPresets = {
            'notdone':['pending','launched','running'],
            'runz':['running','joining','running_ended','exec_done','memo_done','failed','dep_fail','fail_retryable'],
            'dead':['exec_done','memo_done','failed','dep_fail','fail_retryable'],
            'oddball':['unsched','unknown']
        }


        ## Prepare monitoring database with needed views, if necessary
        self.viewList = ['runview','nctaskview','ndtaskview','taskview','sumv1','sumv2','summary']
        self.viewsUpdated = False
        self.makeViewsSQL=os.path.join(sys.path[0],'makeViews.sql')
        if not self.checkViews():
            #print('%WARNING: This monitoring database does not contain the necessary "views" to continue')
            self.storeViews()
            pass


        ## Load in the workflow (run) summary table
        self.wrows = None
        self.wtitles = None
        self.runid2num = None
        self.runnum2id = None
        self.numRuns = 0
        self.runmin = 999999999
        self.runmax = -1
        self.loadWorkflowTable()

        ## Prepare for task summary data
        self.taskStats = {}   # {'taskname':{statTemplate}}
        self.taskList = []    # list of all task types in this workflow
        self.sumFlag = False  # flag indicating whether task summary data has been read
        self.taskLimit=0   # Set to non-zero to limit tasks processed for pTasks
        self.trows = None
        self.ttitles = None
        self.tSumCols = ['runnum','tasknum','task_id','appname','status','lastUpdate','fails','try_id',
                         'hostname','launched','start','waitTime','ended','runTime']
        self.tSumColsExt = self.tSumCols+['depends','stdout']

        ## nodeUsage is a list of nodes currently in use and the
        ## number of tasks running on them.  {nodeID:#runningTasks}
        self.nodeUsage = {}
        
        return


    def __del__(self):
        ## Class destructor 
        self.con.close()
        return


    ##########################
    ## Simple sqlite utilities
    ##########################


    def checkViews(self):
        ## Check that this sqlite3 database file contains the needed views
        views = self.getSchemaList(type='view')
        if len(views)==0:return False
        for view in self.viewList:
            if view not in views:return False
        return True
    
    def getSQLfromFile(self,filename):
        ## Read text file of sql and produce clean list of individual sql commands
        with open(filename,'r') as f:
            sql=f.read()
            pass
        ## Remove SQL /* comments */ from file content
        while True:
            start=sql.find('/*')
            end=sql.find('*/')+2
            if start == -1: break
            sql=sql[:start]+sql[end:]
            pass
        ## Must split multiple sql commands into separate python sqlite3 calls
        sqlList = sql.split(';')
        sqlList = sqlList[:-1]   # remove last (empty) element in list
        if self.debug>0:print(f'There were {len(sqlList)} sql commands found in the file')
        return sqlList
    
    def storeViews(self):
        ## Store custom views into the monitoring.db file
        ##   View definitions are stored in an external file
        if self.debug>0:
            print('Entering storeViews')
            print('Attempting to remove sqlite "views" in monitoring database')
            pass
        views = self.getSchemaList(type='view')
        for view in views:
            if view in self.viewList:
                sql = f'drop view {view}'
                self.sqlCmd(sql)
                pass
            pass
        if self.debug>0:
            print('Attempting to add sqlite "views" to monitoring database')
            print(f'makeViewsSQL = {self.makeViewsSQL}')
            pass
        sqlList2 = self.getSQLfromFile(self.makeViewsSQL)
        for cmd in sqlList2:
            self.sqlCmd(cmd)
            pass
        self.viewsUpdated = True
        return

    
    def getSchemaList(self,type='table'):
        ## Fetch list of all db tables and views
        if self.debug>0:print(f'Entering getSchemaList({type})')
        ## Parsl monitoring.db currently contains four tables: resource, status, task, workflow
        self.cur.execute(f"SELECT name FROM sqlite_master WHERE type='{type}';")
        rawTableList = self.cur.fetchall()
        tableList = []
        for table in rawTableList:
            tableList.append(table[0])
            pass
        return tableList

    def getSchema(self,type='table',table='all'):
        ## Fetch the schema for one or more db tables or views
        if self.debug>0:print(f'Entering getSchema({type},{table})')
        if table == 'all':
            sql = (f"select sql "
                   f"from sqlite_master "
                   f"where type = '{type}' ;")
        else:
            sql = (f"select sql "
                   f"from sqlite_master "
                   f"where type = '{type}' and name = '"+table+"';")
            pass
        self.cur.execute(sql)
        schemas = self.cur.fetchall()
        return schemas

    
    def printRow(self,titles,row):
        ## Pretty print one db row with associated column names
        if self.debug>0:print(f'Entering printRow({titles},{row})')
        for title,col in zip(titles,row):
            print(title[0],": ",col)
            pass
        pass
        return

    
    def dumpTable(self,titles,rowz):
        ## Pretty print all db rows with column names
        print("\ndumpTable:\n==========")
        print("titles = ",titles)
        print("Table contains ",len(rowz)," rows")
        # for row in rowz:
        #     for key in row.keys():
        #         print(row[key])
        #     pass
        print(tabulate(rowz,headers=titles,tablefmt=tblfmt))
        print("-------------end of dump--------------")
        return
    

    def stdQuery(self,sql):
        ## Perform a db query, fetch all results and column headers
        if self.debug > 0: print(f'Entering stdQuery({sql})')
        result = self.cur.execute(sql)
        rows = result.fetchall()   # <-- This is a list of db rows in the result set
        ## This will generate a list of column headings (titles) for the result set
        titlez = result.description
        ## Convert silly 7-tuple title into a single useful value
        titles = []
        for title in titlez:
            titles.append(title[0])
            pass
        if self.debug > 0:
            print("titles = ",titles)
            print("#rows = ",len(rows))
            if self.debug > 4: print("rows = ",rows)
            pass
        return (rows,titles)


    def sqlCmd(self,sql):
        ## Perform an arbitrary SQL command
        if self.debug > 0: print(f'Entering sqlCmd({sql})')
        result = self.cur.execute(sql)
        rows = result.fetchall()   # <-- This is a list of db rows in the result set
        if self.debug > 0:
            print("#rows = ",len(rows))
            if self.debug > 4: print("rows = ",rows)
            pass
        return (rows)





    ######################################
    ## Parsl monitoring analysis functions
    ######################################
    
    def loadWorkflowTable(self):
        ## Extract all rows from 'workflow' table in monitoring.db
        ##  called from constructor to initialize workflow data in self.wrows
        #
        if self.debug > 0:print("Entering loadWorkflowTable()")
        ##
        ##  result set: [runnum,run_id,workflow_name,workflow_version,
        ##               began,completed,runElapsedTime,host,user,rundir,
        ##               failed_count,completed_count]
        ##
        ## This alternate query returns a list of one 'row' containing the most recent entry
        #sql = "select * from workflow order by time_began desc limit 1"
        ##
        
        sql=("select rv.runnum,w.run_id,w.workflow_name,"
             "rv.began,rv.completed,rv.runElapsedTime,w.host,w.user,"
             "w.tasks_completed_count as completed_count,w.tasks_failed_count as failed_count,"
             "w.rundir "
             "from workflow w "
             "join runview rv on (w.run_id=rv.run_id) "
             "order by w.time_began asc ")

        (self.wrows,self.wtitles) = self.stdQuery(sql)
        self.runid2num = {}
        self.runnum2id = {}
        for row in self.wrows:
            runID = row['run_id']
            runnum = row['runnum']
            #runDir = os.path.basename(row['rundir'])   ## "runDir" is defined by the runinfo/NNN directory
            self.runid2num[runID] = runnum
            self.runnum2id[int(runnum)] = runID
            if int(runnum) > self.runmax: self.runmax = int(runnum)
            if int(runnum) < self.runmin: self.runmin = int(runnum)
            pass
        self.numRuns = len(self.wrows)
        if self.debug > 1:
            print('numRuns   = ',self.numRuns)
            print('runid2num = ',self.runid2num)
            print('runnum2id = ',self.runnum2id)
            print('runmin    = ',self.runmin)
            print('runmax    = ',self.runmax)
            pass
        return


    def printWorkflowSummary(self,runnum=None):
        ## Summarize current state of workflow
        if self.debug>0:print(f'Entering printWorkflowSummary({runnum})')
        ## This is a highly-customized view
        repDate = datetime.datetime.now()
        titles = self.wtitles

        ##  Select desired workflow 'run'
        nRuns = self.numRuns
        rowindex = self.selectRunID(runnum)
        row = self.wrows[rowindex]

        runnum = row['runnum']
        irunNum = int(runnum)
        runNumTxt = f'{runnum}'
        runInfo = irunNum-1
        runInfoTxt = f'{runInfo:03d}'
        if irunNum == int(self.runmax):runNumTxt += '    <<-most current run->>'
        exeDir = os.path.dirname(os.path.dirname(row['rundir']))
        completedTasks = row['completed_count']+row['failed_count']

        ## Running times and duration
        runStart = row['began']
        if runStart == None: runStart = '*pending*'
        runEnd   = row['completed']
        if runEnd == None: runEnd = '*pending*'
        duration = row['runElapsedTime']
        if duration == None: duration = '*pending*'

        ## Task tallies for selected run
        where = ''
        myruns = 'all runs'
        if rowindex>-1:
            where = f' where runnum={runnum}'
            myruns = f'run {runnum}'
            pass
        sql1 = f'select count(*) from taskview'+where
        sql2 = f'select count(*) from ndtaskview'+where
        sql3 = f'select count(*) from nctaskview'+where
        nTasks = self.sqlCmd(sql1)[0][0]
        ndTasks = self.sqlCmd(sql2)[0][0]
        ncTasks = self.sqlCmd(sql3)[0][0]

        
        ## Print workflow run summary
        print('Workflow summary at',repDate,'\n==============================================')
        wSummaryList = []
        wSummaryList.append(['workflow name',row['workflow_name']])
        wSummaryList.append(['run num',runNumTxt ])
        wSummaryList.append(['runinfo/NNN',runInfoTxt ])
        wSummaryList.append(['run start',runStart ])
        wSummaryList.append(['run end ',runEnd ])
        wSummaryList.append(['run duration ', duration])
        wSummaryList.append(['tasks completed',completedTasks ])
        wSummaryList.append(['tasks completed: success', row['completed_count']])
        wSummaryList.append(['tasks completed: failed',row['failed_count'] ])
        wSummaryList.append(['----------','----------'])
        wSummaryList.append([f'summary of {myruns}',''])
        wSummaryList.append(['cached tasks ',nTasks])
        wSummaryList.append(['non-dispatched cached tasks ',ndTasks])
        wSummaryList.append(['non-cached tasks ',ncTasks])
        wSummaryList.append(['----------','----------'])
        wSummaryList.append(['workflow user', row['user']+'@'+row['host']])
        wSummaryList.append(['workflow rundir',exeDir])
        wSummaryList.append(['MonitorDB',self.dbfile])
        print(tabulate(wSummaryList, tablefmt=tblfmt))
        return


    def selectRunID(self,runnum=None):
        ## Select the 'workflow' table row based on the requested
        ## runNumber, 1 to N (not to be confused with the many digit,
        ## hex "run_id")
        if self.debug>0: print("Entering selectRunID, runnum=",runnum)
        
        if runnum == None:         # Select most recent workflow run
            #print("runnum = None, returning -1")
            return -1
        else:
            for rdx in list(range(len(self.wrows))):
                if self.wrows[rdx]['run_id'] == self.runnum2id[runnum]:
                    #print("runnum = ",runnum,', workflow table row = ',rdx)
                    return rdx
                pass
            assert False,"Help!"
            pass
        pass


    def loadTaskData(self,what='*',where=''):
        # Load in current task summary data (default parameters => load everything)
        if self.debug>0:print(f'Entering loadTaskData({what},{where})')
        sql = (f"select {what} from summary {where}")
        if self.debug > 0: print('sql = ',sql)
        (self.trows,self.ttitles) = self.stdQuery(sql)

        ## Tally status for each task type
        ##  Store -> taskStats{}:
        ##     self.taskStats{'taskname1':{#status1:num1,#status2:num2,...},...}
        tNameIndx = self.ttitles.index('appname')
        tStatIndx = self.ttitles.index('status')
        nTaskTypes = 0
        nTasks = 0
        statTotals=dict(self.statTemplate)  # bottom row = vertical totals
        statTotals['TOTAL'] = 0
        for task in self.trows:
            nTasks += 1
            tName = task[tNameIndx]
            tStat = task[tStatIndx]
            if tName not in self.taskStats.keys():
                nTaskTypes += 1
                self.taskStats[tName] = dict(self.statTemplate)
                self.taskStats[tName]['TOTAL'] = 0
                pass
            self.taskStats[tName][tStat] += 1
            self.taskStats[tName]['TOTAL'] += 1
            statTotals[tStat] += 1
            statTotals['TOTAL'] += 1
            pass
        self.taskStats['TOTAL'] = dict(statTotals)
        self.taskList = list(self.taskStats.keys())[:-1]
        self.sumFlag = True
        return


        
    def taskStatusMatrix(self,runnum=None):
        ## print matrix of task function name vs Parsl state
        if self.debug>0:print('Entering taskStatusMatrix')

        runTxt=' for all runs'
        where = ''
        if runnum!=None:
            where=f'where runnum={runnum} '
            runTxt=f' for run {runnum}'
            pass
        if not self.sumFlag: self.loadTaskData(where=where)
        if len(self.trows)<1:
            print('No tasks to summarize')
            return

        ## Convert statistics data into a Pandas dataframe
        ##    [Defining a pandas dataframe]
        ##    df = pandas.DataFrame(columns=['a','b','c','d'], index=['x','y','z'])
        ##    df.loc['y'] = pandas.Series({'a':1, 'b':5, 'c':2, 'd':3})
        pTaskStats = pd.DataFrame(columns=list(self.statTemplate.keys())+['TOTAL'],
                                  index=self.taskStats.keys())
        for task in self.taskStats:
            pTaskStats.loc[task] = pd.Series(self.taskStats[task])
            pass


        print(f'\nTask status matrix{runTxt}:')
        print(tabulate(pTaskStats,headers='keys',tablefmt=tblfmt))
        return


    
    def taskSum(self,runnum=None,tasknum=None,taskid=None,taskname=None,status=None,
                limit=None,extendedCols=False,oddball=False):
        # Prepare and print out a summary of all (selected) tasks for this workflow
        if self.debug>0:
            print("Entering taskSum")
            print(f'runnum={runnum},tasknum={tasknum},taskid={taskid},taskname={taskname},'
                  f'status={status},limit={limit},extendedCols={extendedCols}')
            pass

        # Prepare list of variables (columns) to request, regular or extended
        what = ','.join(self.tSumCols)
        if extendedCols:
            what = ','.join(self.tSumColsExt)
        if self.debug>0: print(f'what = {what}')
        
        # Prepare 'where' clause for sql
        where = ''
        whereList = []
        if runnum!=None:whereList.append(f' runnum={runnum} ')
        if tasknum!= None:whereList.append(f' tasknum={tasknum} ')
        if taskid!= None:whereList.append(f' task_id={taskid} ')
        if taskname!=None:whereList.append(f' appname="{taskname}" ')
        if status!=None:whereList.append(f' status="{status}" ')
        if len(whereList)>0:where = 'where '+' and '.join(whereList)
        
        # Fetch data from DB
        self.loadTaskData(what=what,where=where)
        rows = self.trows
        titles = self.ttitles
        
        # Check if output is limited
        last=len(rows)
        if limit!=None and limit!=0: last=limit
        
        # Pretty print
        if len(rows)>0:
            print(f'MOST RECENT STATUS FOR SELECTED TASKS (# tasks selected = {len(rows)}, print limit = {last})')
            print(tabulate(rows[0:last],headers=titles,tablefmt=tblfmt))
        else:
            print(f'No ordinary cached tasks have been selected for display')
            pass
        
        ## Print oddball task?
        if oddball:
            self.ndtaskSummary(runnum=runnum,taskname=taskname)
            self.nctaskSummary(runnum=runnum)
        return

    def nctaskSummary(self,runnum=None):
        ## This produces a list of the most recently invoked non-cached tasks
        if self.debug>0:print(f'Entering nctaskSummary()')
        where = ''
        runtxt = 'for all runs'
        if runnum!=None:
            where = f' where runnum={runnum}'
            runtxt = f'for run {runnum}'
        sql = 'select * from nctaskview'+where
        (rows,titles) = self.stdQuery(sql)
        if len(rows)>0:
            print(f'List of most recent invocation of all {len(rows)} non-cached tasks {runtxt}')
            print(tabulate(rows,headers=titles,tablefmt=tblfmt))
        else:
            print('There are no non-cached tasks to report.')
            pass
        return

    def ndtaskSummary(self,runnum=None,taskname=None):
        ## This produces a list of non-dispatched cached tasks (no task_hashsum)
        if self.debug>0:print(f'Entering ndtaskSummary(runnum={runnum},taskname={taskname})')
        where = ''
        whereList = []
        runtxt = 'for all runs'
        if runnum!=None:
            whereList.append(f' runnum={runnum}')
            runtxt = f'for run {runnum}'
            pass
        if taskname!=None:
            whereList.append(f' appname="{taskname}" ')
            runtxt += f' and for app name={taskname} '
            pass
        if len(whereList)>0: where=' where '+'and'.join(whereList)

        sql = 'select * from ndtaskview '+where
        (rows,titles) = self.stdQuery(sql)
        if len(rows)>0:
            print(f'List of {len(rows)} non-dispatched cached tasks {runtxt}')
            print(tabulate(rows,headers=titles,tablefmt=tblfmt))
        else:
            print('There are no non-dispatched tasks to report.')
        return

   
    def taskHis(self,runnum=None,tasknum=None,taskid=None,taskname=None,status=None,limit=None):
        # Print out the full history for a single, specified task in this workflow
        if self.debug>0:
            print(f'Entering taskHis(runnum={runnum},tasknum={tasknum},taskid={taskid},'
                  f'taskname={taskname},status={status},limit={limit}')
            pass
        if taskname==None and tasknum==None and (taskid==None or runnum==None):
            print(f'%ERROR: you must specify a task (app) name, a task number or (a taskID and run number) for this report')
            sys.exit(1)

        # Prepare 'where' clause for sql
        morewhere = ''
        whereList = [' ']
        if runnum!=None:whereList.append(f' rv.runnum={runnum} ')
        if tasknum!=None:whereList.append(f' tv.tasknum={tasknum}')
        if taskid!=None:whereList.append(f' tv.task_id={taskid}')
        if taskname!=None:whereList.append(f' tv.appname="{taskname}"')
        if status!=None:whereList.append(f' status="{status}" ')

        morewhere = whereList[0]
        if len(whereList)>1:morewhere = ' and '.join(whereList[1:])

        if self.debug>0:print(f'morewhere = {morewhere}')

        # Fetch data from DB
        #sql = taskHistoryQuery.replace('#tasknum#',f'{tasknum}')
        sql = taskHistoryQuery
        sql = sql.replace('#morewhere#',morewhere)
        (rows,titles) = self.stdQuery(sql)
        
        # Pretty print
        print(f'Full history of task {tasknum}, containing {len(rows)} state changes')
        print(tabulate(rows,headers=titles,tablefmt=tblfmt))
        return

    def runStats(self):
        ## Display table of integrated time intervals per app
        if self.debug>0:print(f'Entering runStats()')
        sql2 = plotStats.replace('#groupby#','tv.appname')
        (crows,ctitles)=self.stdQuery(sql2)

        statsTitles=['appname','#(re)tries','waitTime','runTime','elapsedTime']
        stats = []
        for crow in crows:
            stats.append([crow[1],crow[2],crow[6],crow[7],crow[8]])
            pass

        ## print and return
        print(f'Accumulated run statistics for all {len(crows)} app types (includes any retries). Time in minutes.')
        print(tabulate(stats,headers=statsTitles,tablefmt=tblfmt))

        return

        
    
    def makePlots(self):
        ## Produce various plots
        if self.debug>0:print('Entering makePlots()')
        histList = ['waitTime','runTime','elapsedTime']

        ## Load generic task data
        self.loadTaskData()
        print(f'There are {len(self.taskList)} task types in this workflow: {self.taskList}')
        
        ## Query timing data from monitoring database.
        ## [tasknum,appname,numTries,launchTime,startTime,endTime,waitTime,runTime,elapsedTime]
        ## Two types of plots:
        ##   1) data for each individual parsl task; and,
        ##   2) cummulative data for each task type (i.e., appname)
        sql1 = plotStats.replace('#groupby#','tv.task_hashsum')
        (trows,ttitles)=self.stdQuery(sql1)

        for h in histList:
            hx = ttitles.index(h)
            if self.debug>0:print(f'h={h}, hx={hx}')
        
            ## Organize data for plotting
            ## Time interval data is represented as minutes
            hists={}   # {<appname>:[datum1,datum2,...]}

            nhists=len(self.taskList)
            for task in self.taskList:
                hists[task] = []       # initialize histogram data
                pass
            for trow in trows: hists[trow[1]].append(trow[hx])  # fill hist list of runTime

            if self.debug>1:
                for k in hists:
                    print(f'===> {k}[{len(hists[k])}] : {hists[k]}')
                    pass
                pass
        
            ## Prepare plotting canvas (a grid of up to 4 cols x N rows)
            ncols=4
            if nhists <= ncols: ncols=nhists
            if nhists%ncols == 0:
                nrows = int(nhists/ncols)
            else:
                nrows = 1 + int(nhists/ncols)
                pass
            fig = plt.figure(figsize=(11,8.5),tight_layout=True)  ## Establish canvas
            plt.suptitle(f'Task {ttitles[hx]}s')   ## define plot title (before making plots)
            nhist = 1
            if self.debug>0: print(f'nhists={nhists}:  nrows={nrows}, ncols={ncols}')

            ## Fill matplotlib histograms
            for taskType in self.taskList:
                row = int((nhist-1)/ncols)   # row of plot on canvas
                col = (nhist-1)%ncols        # col of plot on canvas
                if self.debug>0:print(f'{taskType} [{len(hists[taskType])}]: nhist {nhist}, row {row}, col {col}')
                x = fig.add_subplot(nrows,ncols,nhist)  # create a spot for the histogram
                #x = plt.subplot(nrows,ncols,nhist)  # alternative to fig.add_subplot()
                x.hist(hists[taskType])  # hand histo data to matplotlib
                #n, bins, patches = x.hist(hists[taskType])  # in case you want the binned data

                #  Use, e.g., r' ... $\sigma$ ...' strings for greek (in matplotlib only)
                x.set_xlabel(f'{ttitles[hx]} in minutes')
                x.set_ylabel(f'# tasks')
                x.set_title(fr'{taskType}')
                #x.set_title(fr'{nhist}[{row},{col}] {taskType}')
                nhist += 1
                pass
        
            # Display plots
            plt.savefig(f'plots-{ttitles[hx]}.jpg')
            plt.show()
            pass
        return
    


    ####################
    ## Driver functions
    ####################

    def shortSummary(self,runnum=None):
        ## This is the short summary.
        if self.debug>0:print(f'Entering shortSummary({runnum})')
        self.printWorkflowSummary(runnum)
        self.taskStatusMatrix(runnum=runnum)
        ##self.printTaskSummary(runnum,opt='short')
        return

    def taskSummary(self,runnum=None,tasknum=None,taskid=None,taskname=None,status=None,
                    limit=None,extendedCols=False,oddball=False):
        ## This is a summary of all cached tasks in the workflow.
        if self.debug>0:print(f'Entering taskSummary(runnum={runnum},tasknum={tasknum},'
                              f'taskid={taskid},taskname={taskname},status={status},'
                              f'limit={limit},extendedCols={extendedCols})')
        self.printWorkflowSummary(runnum)
        self.taskSum(runnum=runnum,tasknum=tasknum,taskid=taskid,taskname=taskname,status=status,
                     limit=limit,extendedCols=extendedCols,oddball=oddball)
        self.taskStatusMatrix(runnum=runnum)
        return

    def taskHistory(self,runnum=None,tasknum=None,taskid=None,taskname=None,status=None,limit=None):
        ## This produces a full history for specified task(s)
        if self.debug>0:print(f'Entering taskHistory()')
        if runnum!=None: self.printWorkflowSummary(runnum)
        self.taskHis(runnum=runnum,tasknum=tasknum,taskid=taskid,taskname=taskname,status=status,limit=limit)
        self.taskStatusMatrix(runnum=runnum)
        return


    def runHistory(self):
        ## This is the runHistory: details for each workflow 'run'
        if self.debug>0:print("Entering runHistory()")
        rows = []
        for wrow in self.wrows:
            row = list(wrow)
            rows.append(row)
            pass
        print(tabulate(rows,headers=self.wtitles, tablefmt=tblfmt))
        return

    def recentStatus(self,limit=50):
        ## Display the most recent status updates
        if self.debug>0:print('Entering recentStatus()')
        # Fetch data from DB
        sql = recentStatusQuery.replace('#limit#',str(limit))
        (rows,titles) = self.stdQuery(sql)
        # Pretty print
        print(f'Recent workflow activity')
        print(tabulate(rows,headers=titles,tablefmt=tblfmt))
        return

    def plots(self):
        ## Produce various performance plots for this workflow **EXPERIMENTAL**
        if self.debug>0:print(f'Entering plots()')
        self.runStats()
        self.makePlots()
        return

    
#############################################################################
#############################################################################
##
##                                   M A I N
##
#############################################################################
#############################################################################


if __name__ == '__main__':


    reportTypes = ['shortSummary','taskSummary','taskHistory','nctaskSummary','runHistory','recentStatus','plots']

    ## Parse command line arguments
    parser = argparse.ArgumentParser(description='A simple Parsl status reporter. Available reports include:'+str(reportTypes)+'.',
                                     usage='$ python wstat [options] {report type}',
                                     epilog='Note that not all options are meaningful for all report types, and some options are required for certain reports.')
    parser.add_argument('reportType',help='Type of report to display (default=%(default)s)',nargs='?',default='shortSummary')
    parser.add_argument('-f','--file',default='./monitoring.db',help='name of Parsl monitoring database file (default=%(default)s)')
    parser.add_argument('-r','--runnum',type=int,help='Specific run number of interest (default = latest)')
    parser.add_argument('-s','--schemas',action='store_true',default=False,help="only print out monitoring db schema for all tables")
    parser.add_argument('-t','--tasknum',default=None,help="specify tasknum (required for taskHistory)")
    parser.add_argument('-T','--taskID',default=None,help="specify taskID")
    parser.add_argument('-o','--oddballTasks',action='store_true',default=False,help="include non-cached/non-dispatched tasks")
    parser.add_argument('-n','--taskName',default=None,help="specify task_func_name")
    parser.add_argument('-S','--taskStatus',default=None,help="specify task_status_name")
    parser.add_argument('-l','--taskLimit',type=int,default=None,help="limit output to N tasks (default is no limit)")
    parser.add_argument('-L','--statusLimit',type=int,default=20,help="limit status lines to N (default = %(default)s)")
    parser.add_argument('-x','--extendedCols',action='store_true',default=False,help="print out extended columns")
    parser.add_argument('-u','--updateViews',action='store_true',default=False,help="force update of sqlite3 views (currently a no-op)")
    parser.add_argument('-d','--debug',type=int,default=0,help='Set debug level (default = %(default)s)')

    parser.add_argument('-v','--version', action='version', version=__version__)


    print(sys.version)
    
    args = parser.parse_args()
    print('wstat - Parsl workflow status (version ',__version__,', written for Parsl version '+pVersion+')\n')

    if args.debug > 0:
        print('command line args: ',args)
        pass
    
    startTime = datetime.datetime.now()

    ## Check monitoring database exists
    if not os.path.exists(args.file):
        print("%ERROR: monitoring database file not found, ",args.file)
        sys.exit(1)

    ## Create a Parsl Monitor object
    m = pmon(dbfile=args.file,debug=args.debug)
    
    ## Print out table schemas only
    if args.schemas:
        ## Fetch a list of all tables and views in this database
        print('Fetching list of tables and views')
        tableList = m.getSchemaList('table')
        print('Tables: ',tableList)
        viewList = m.getSchemaList('view')
        print('Views: ',viewList)
        ## Print out schema for all tables
        for table in tableList:
            schema = m.getSchema('table',table)
            print(schema[0][0])
            pass
        ## Print out schema for all views
        for view in viewList:
            schema = m.getSchema('view',view)
            print(schema[0][0])
            pass
        sys.exit()

    ## Update the sqlite views in the monitoring database
    if args.updateViews:
        if not m.viewsUpdated: m.storeViews()
        sys.exit()

        
    ## Check validity of run number
    if not args.runnum == None and (int(args.runnum) > m.runmax or int(args.runnum) < m.runmin):
        print('%ERROR: Requested run number, ',args.runnum,' is out of range (',m.runmin,'-',m.runmax,')')
        sys.exit(1)

    ## Print out requested report
    if args.reportType == 'shortSummary':
        m.shortSummary(runnum=args.runnum)
    elif args.reportType == 'taskSummary':
        m.taskSummary(runnum=args.runnum,tasknum=args.tasknum,taskid=args.taskID,status=args.taskStatus,
                      taskname=args.taskName,limit=args.taskLimit,
                      extendedCols=args.extendedCols,oddball=args.oddballTasks)
    elif args.reportType == 'taskHistory':
        m.taskHistory(runnum=args.runnum,tasknum=args.tasknum,taskid=args.taskID,status=args.taskStatus,
                      taskname=args.taskName,limit=args.taskLimit)
    elif args.reportType == 'nctaskSummary':
        m.nctaskSummary()
    elif args.reportType == 'runHistory':
        m.runHistory()
    elif args.reportType == 'recentStatus':
        m.recentStatus(args.statusLimit)
    elif args.reportType == 'plots':
        m.plots()
    else:
        print("%ERROR: Unrecognized reportType: ",args.reportType)
        print("Must be one of: ",reportTypes)
        print("Exiting...")
        sys.exit(1)
        pass
    
    ## Done
    endTime = datetime.datetime.now()
    print("wstat elapsed time = ",endTime-startTime)
    #sys.exit()













































    

########################################################################################################
########################################################################################################
########################################################################################################
########################   OLD STUFF FOLLOWS  ##########################################################
########################################################################################################
########################################################################################################
########################################################################################################

"""
    def plotStats(self,runnum=None):
        ## plot statistics for most current parsl run
        #
        
        ## Confirm pTasks[] has been filled
        if not self.pTasksFilled: self.getTaskData(runnum=runnum,printSummary=False)

        ## if no tasks defined, bail
        if len(self.pTasks) < 1:
            print("Nothing to plot: pTasks is empty!")
            return
        else:
            print("There are ",len(self.pTasks)," tasks in pTasks.")
            pass
        
        #
        ## Sort task list by task type
        ##  Input from pTasks[]:
        ##     pTitles = {'task_id':0,'task_name':1,'run_num':2,'status':3,'hostname':4,
        ##     '#fails':5,'submitTime':6,'startTime':7,'endTime':8,'runTime':9,'stdout':10}

        ## Tally execution runtimes for "done" tasks
        histData = {}  ## {"taskName":[runTime1,runTime2,...],...}
        tNameIndx = self.pTitles['task_name']
        tStatIndx = self.pTitles['status']
        tRunIndx  = self.pTitles['runTime']
        nTasks = 0
        nTaskTypes = 0
        nDone = 0
        nErrors = 0
        for task in self.pTasks:
            nTasks += 1
            tName = task[tNameIndx]
            tStat = task[tStatIndx]
#            if tStat.endswith('done'):      ## Currently includes "exec_done" and "memo_done"
            if 'done' in tStat:      ## Currently includes "exec_done" and "memo_done"
                nDone += 1
                if task[tRunIndx] == None:
                    nErrors += 1
                    if nErrors < 10:
                        print('%ERROR: monitoring.db bug.  Completed task has no runtime: ',task[:9])
                    elif nErrors == 10:
                        print('%ERROR: Too many tasks with no runtime...')
                        pass
                    continue
                if task[tNameIndx] not in histData.keys():
                    nTaskTypes += 1
                    histData[tName] = [task[tRunIndx].total_seconds()/60]
                else:
                    histData[tName].append(task[tRunIndx].total_seconds()/60)
                    pass
                pass
            pass

        print('Total tasks = ',nTasks,'\nTotal task types = ',nTaskTypes,'\nTotal tasks done = ',nDone,'\nTotal missing runtimes = ',nErrors)

        if self.debug > 0 :
            for task in histData:
                print('task: ',task,', len = ',len(histData[task]))
                if self.debug > 1: print('  histData: ',histData[task])
                pass
            pass


        ## At this point, all data is stored in tuples (in the histData{})
        h = []
        hno=0
        import pickle
        for task in histData.keys():
            print("Histogramming data for task ",task)
            #fig = plt.figure()
            #plt.suptitle(task)
            df = pd.DataFrame(histData[task])
            print(f'df = {df}')
            h.append(df.plot.hist(bins=25))
            ###print("type(h) = ",type(h))
            ###print("dir(h) = ",dir(h))
            h[-1].set_ylabel("# instances")
            h[-1].set_xlabel("time (min)")
            h[-1].set_title(task)
            hno+=1
                         
            plt.show()
            pass
        pickle.dump(h,open('plots.pickle','wb'))

        return
        #
        ## Histogram run time separately for each task type
        #print('task = ',task,', histData[task] = ',str(histData[task]))
        #a, b, c = plt.hist(histData[task],bins=100,histtype='step')  # <-- this DOES work!

        ## The following stanza was borrowed from ancient RSP code
        # fig = plt.figure(figsize=(11,8.5))  ## Establish canvas
        # plt.suptitle("Task Runtimes)   ## define plot title (before making plots)
        # ax = fig.add_subplot(411)  ## 411 => 4 rows x 1 col of plots, this is plot #1
        # #ax.plot(timex,flux,'k-',timex,flux,'r,',label="FLUX",linewidth=0.5,drawstyle='steps-mid')
        # ax.set_ylabel('# tasks')
        # ax.set_xlabel('Runtime (min)')
        # ax.grid(True)
        # #ax.ticklabel_format(style='sci',scilimits=(0,0),axis='y')  ## Numerical formatting
        # plt.setp(ax.get_xticklabels(), visible=False)

        ## matplotlib's hist function:
        # matplotlib.pyplot.hist(x, bins=None, range=None,
        # density=None, weights=None, cumulative=False, bottom=None,
        # histtype='bar', align='mid', orientation='vertical',
        # rwidth=None, log=False, color=None, label=None,
        # stacked=False, normed=None, *, data=None, **kwargs)[source]

        ## Setup histograms
        nHists = len(histData.keys())
        print("Preparing ",nHists," histograms.")
        ncols = 3 # number of histograms across the page
        if nHists%ncols == 0:
            nrows = int(nHists/ncols)
        else:
            nrows = int(nHists/ncols) + 1
            pass
        print("(ncols,nrows) = (",ncols,nrows,")")
        num_bins = 50


        ## matplotlib.pyplot.subplots(nrows=1, ncols=1, sharex=False, sharey=False, squeeze=True, subplot_kw=None, gridspec_kw=None, **fig_kw)[source]
        fig, ax = plt.subplots(nrows=nrows,ncols=ncols)
        print('fig = ',fig)
        print('ax = ',ax)

        for task in histData.keys():

            print('Plotting: task = ',task)

            # the histogram of the data
            n, bins, patches = ax.hist(histData[task], num_bins)

            # add a 'best fit' line
            # y = ((1 / (np.sqrt(2 * np.pi) * sigma)) *
            #      np.exp(-0.5 * (1 / sigma * (bins - mu))**2))
            #ax.plot(bins, y, '--')
            ax.set_xlabel('Execution time on Cori KNL (min)')
            ax.set_ylabel('# Tasks')
            ax.grid(True)
            ax.set_title('Distribution of '+task+' execution times')

            # Tweak spacing to prevent clipping of ylabel
            fig.tight_layout()
            plt.show()
        
            pass

        #
        ## Display histograms
        #
        plt.show()
        return

    
    
"""
