## wstat.py - workflow status summary from Parsl monitoring database

## The idea is not to replace the "sqlite3" interactive command or the
## Parsl web interface, but to complement them to create some useful
## interactive summaries specific to Parsl workflows.

## Python dependencies: sqlite3, tabulate, matplotlib

## T.Glanzman - Spring 2019
__version__ = "1.1.0"
pVersion='1.0.0:lsst-dm-202005'    ## Parsl version

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

class pmon:
    ### class pmon - interpret Parsl monitoring database
    def __init__(self,dbfile='monitoring.db',debug=0):
        ## Instance variables
        self.dbfile = dbfile
        self.debug = debug # [0=none,1=short,2=more,3=even more,5=lengthy tables]

        ## sqlite3 database connection and cursor
        self.con = sqlite3.connect(self.dbfile,
                                   detect_types=sqlite3.PARSE_DECLTYPES |
                                   sqlite3.PARSE_COLNAMES) ## special connect to sqlite3 file
#        self.con = sqlite3.connect(self.dbfile)           ## vanilla connect to sqlite3 file
        self.con.row_factory = sqlite3.Row                 ## optimize output format
        self.cur = self.con.cursor()                       ## create a 'cursor'

        ## List of all task stati defined by Parsl
        self.statList = ['pending','launched','joining','running','unsched','unknown','exec_done','memo_done','failed','dep_fail','fail_retryable']
        ## This template contains all known parsl task states
        #OBS#self.statTemplate = {'pending':0,'launched':0,'running':0,'exec_done':0,'memo_done':0,'failed':0,'dep_fail':0,'fail_retryable':0,'unsched':0,'unknown':0}
        self.statTemplate = {}
        ## Build initial task state tally dictionary
        for state in self.statList:
            self.statTemplate[state] = 0
            pass
        
        self.statPresets = {
            'notdone':['pending','launched','running'],
            'runz':['running','exec_done','memo_done','failed','dep_fail','fail_retryable'],
            'dead':['exec_done','memo_done','failed','dep_fail','fail_retryable'],
            'oddball':['unsched','unknown']
        }

        ## Read in the workflow (summary) table
        self.wrows = None
        self.wtitles = None
        self.runid2num = None
        self.runnum2id = None
        self.numRuns = 0
        self.runmin = 999999999
        self.runmax = -1
        self.readWorkflowTable()

        ## pTasks contains selected task information filled by deepTaskSummary()
        self.pTitles = {'task_id':0,'task_name':1,'run_num':2,'status':3,'hostname':4,'try':5,
                        '#fails':6,'submitTime':7,'startTime':8,'endTime':9,'runTime':10,'stdout':11}
        self.pTasks = []
        self.pTasksFilled = False
        self.taskLimit=0   # Set to non-zero to limit tasks processed for pTasks

        ## nodeUsage is a list of nodes currently in use and the
        ## number of tasks running on them.  {nodeID:#runningTasks}
        self.nodeUsage = {}
        
        return


    def __del__(self):
        ## Class destructor 
        self.con.close()
        return

    ####################
    ## Parsl time stamp convenience functions (Parsl times are strings)
    ####################
    def stripms(self,intime):
        ## Trivial function to strip off millisec from Parsl time string
        if intime == None: return None
        return datetime.datetime.strptime(str(intime),'%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d %H:%M:%S')
    
    def makedt(self,intime):
        ## Trivial function to return a python datetime object from Parsl time string
        return datetime.datetime.strptime(str(intime),'%Y-%m-%d %H:%M:%S.%f')
    
    def timeDiff(self,start,end):
        ## Calculate difference in two Parsl times from monitoringDB
        if start == None or end == None: return None
        diff = self.makedt(end) - self.makedt(start)
        return diff


    ####################
    ## Simple sqlite utilities
    ####################
    def getTableList(self):
        ## Fetch list of all db tables
        ## Parsl monitoring.db currently contains four tables: resource, status, task, workflow
        self.cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        rawTableList = self.cur.fetchall()
        tableList = []
        for table in rawTableList:
            tableList.append(table[0])
            pass
        return tableList

    def getTableSchema(self,table='all'):
        ## Fetch the schema for one or more db tables
        if table == 'all':
            sql = ("select sql "
                   "from sqlite_master "
                   "where type = 'table' ;")
        else:
            sql = ("select sql "
                   "from sqlite_master "
                   "where type = 'table' and name = '"+table+"';")
            pass
        self.cur.execute(sql)
        schemas = self.cur.fetchall()
        return schemas


    def printRow(self,titles,row):
        ## Pretty print one db row with associated column names
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
        if self.debug > 0: print("Entering stdQuery, sql = ",sql)
        ## Perform a db query, fetch all results and column headers
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



    ####################
    ## Parsl monitoring analysis functions
    ####################
    def readWorkflowTable(self,sql="select * from workflow order by time_began asc"):
        ## Extract all rows from 'workflow' table in monitoring.db
        ##  called from constructor to initialize workflow data in self.wrows
        #
        if self.debug > 0:print("Entering readWorkflowTable, sql = ",sql)
        ## workflow table:  ['run_id', 'workflow_name', 'workflow_version', 'time_began', 
        ##                   'time_completed', 'host', 'user', 'rundir', 
        ##                   'tasks_failed_count', 'tasks_completed_count']
        ##
        ## This alternate query returns a list of one 'row' containing the most recent entry
        #sql = "select * from workflow order by time_began desc limit 1"
        ##
        (self.wrows,self.wtitles) = self.stdQuery(sql)
        self.runid2num = {}
        self.runnum2id = {}
        for row in self.wrows:
            runID = row['run_id']
            runNum = os.path.basename(row['rundir'])   ## "runNum" is defined by the runinfo/NNN directory
            self.runid2num[runID] = runNum
            self.runnum2id[int(runNum)] = runID
            if int(runNum) > self.runmax: self.runmax = int(runNum)
            if int(runNum) < self.runmin: self.runmin = int(runNum)
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


    def selectRunID(self,runnum=None):
        ## Select the 'workflow' table row based on the requested
        ## runNumber (not to be confused with the many digit, hex
        ## "run_id")
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


    def printWorkflowSummary(self,runnum=None):
        ## Summarize current state of workflow
        repDate = datetime.datetime.now()
        titles = self.wtitles

        ##  Select desired workflow 'run'
        nRuns = self.numRuns
        rowindex = self.selectRunID(runnum)
        row = self.wrows[rowindex]

        runNum = os.path.basename(row['rundir'])
        runNumTxt = runNum
        irunNum = int(runNum)
        if irunNum == int(self.runmax):runNumTxt += '    <<-most current run->>'
        runID = row['run_id']
        exeDir = os.path.dirname(os.path.dirname(row['rundir']))
        completedTasks = row['tasks_completed_count']+row['tasks_failed_count']

        ## Run times
        runStart = row['time_began']
        if runStart == None:
            runStart = '*pending*'
        else:
            runStart = self.stripms(runStart)
            pass

        runEnd   = row['time_completed']
        if runEnd == None:
            runEnd = '*pending*'
        else:
            runEnd = self.stripms(runEnd)
            pass

        ## Compute run duration
        if row['time_began'] != None and row['time_completed'] != None :
            duration = self.timeDiff(row['time_began'],row['time_completed'])
        else:
            duration = '*pending*'
            pass

        ##   Print SUMMARIES
        print('Workflow summary at',repDate,'\n==============================================')
        wSummaryList = []
        wSummaryList.append(['workflow name',row['workflow_name']])
        wSummaryList.append(['run',runNumTxt ])
        wSummaryList.append(['run start',runStart ])
        wSummaryList.append(['run end ',runEnd ])
        wSummaryList.append(['run duration ', duration])
        wSummaryList.append(['tasks completed',completedTasks ])
        wSummaryList.append(['tasks completed: success', row['tasks_completed_count']])
        wSummaryList.append(['tasks completed: failed',row['tasks_failed_count'] ])
        wSummaryList.append(['----------','----------'])
        #wSummaryList.append(['Report Date/Time ',repDate ])
        wSummaryList.append(['workflow user', row['user']+'@'+row['host']])
        wSummaryList.append(['workflow rundir',exeDir])
        wSummaryList.append(['MonitorDB',self.dbfile])
        #wSummaryList.append(['workflow node', row['host']])
        print(tabulate(wSummaryList, tablefmt=tblfmt))
        return



    def getTaskData(self,runnum=None,opt=None,repType="Summary",printSummary=True,taskID=None,taskStatus=None,taskName=None):
        ## The main purpose of this function is to fill the pTask and nodeUsage data structures
        ##   repType = report type, either "Summary" or "History"
        ##   printSummary=True to print (a potentially lengthy) task summary table
        
        if self.debug > 0 : print("Entering getTaskData(runnum=",runnum,", opt=",opt,", repType=",repType,", printSummary=",printSummary,", taskID=",taskID," taskStatus=",taskStatus,")")
        
        ##  Select requested initial Run in workflow table
        rowindex = self.selectRunID(runnum)
        wrow = self.wrows[rowindex]
        if runnum == None: runnum = int(self.runid2num[wrow['run_id']])  # most recent run
        if self.debug > 0: print('--> run_id = ',wrow['run_id'])

        ## Create DB query for task data
        if repType == "Summary" :
            ## Fetch current state of each task
            #where = "where s.task_status_name!='memo_done' "
            where = ""
            if taskID != None:
                where += "t.task_id='"+str(taskID)+"' "
                pass
            if taskName != None:
                if len(where) > 0: where += " and "
                where += " t.task_func_name='"+str(taskName)+"' "
            if len(where) > 0: where = " where "+where
            
            ## The following sql is my current magnum opus in the field of databasery ;)
            #############Original sql as of 10/14/2020##################
            sql = ("select t.run_id,t.task_id,t.task_hashsum,t.task_fail_count,t.task_func_name,t.task_stdout,"
                   "max(s.timestamp),s.task_status_name,"
                   "y.hostname,y.try_id,y.task_try_time_launched,y.task_try_time_running,y.task_try_time_returned "
                   "from task t "
                   "join try y on (t.run_id=y.run_id AND t.task_id=y.task_id) "
                   "join status s on (y.run_id=s.run_id AND y.task_id=s.task_id AND y.try_id=s.try_id) "
                   +where+
                   "group by t.task_id "
                   "order by t.task_id asc ")
            ### EXPERIMENTAL ###
            # sql = ("select t.run_id,t.task_id,t.task_hashsum,t.task_fail_count,t.task_func_name,t.task_stdout,"
            #        "max(s.timestamp),s.task_status_name,"
            #        "y.hostname,y.try_id,y.task_try_time_launched,y.task_try_time_running,y.task_try_time_returned "
            #        "from task t "
            #        "join try y on (t.run_id=y.run_id AND t.task_id=y.task_id) "
            #        "join status s on (y.run_id=s.run_id AND y.task_id=s.task_id AND y.try_id=s.try_id) "
            #        +where+
            #        "group by t.task_hashsum "
            #        "order by t.task_id asc")
        elif repType == "History" :
            ## Fetch full history of each task
            where = " "
            if taskID != None:
                where += "where t.task_id='"+str(taskID)+"' "
                pass
            sql = ("select t.run_id,t.task_id,t.task_hashsum,t.task_fail_count,t.task_func_name,t.task_stdout,"
                   "s.timestamp,s.task_status_name,"
                   "y.hostname,y.try_id,y.task_try_time_launched,y.task_try_time_running,y.task_try_time_returned "
                   "from task t "
                   "join try y on (t.run_id=y.run_id AND t.task_id=y.task_id) "
                   "join status s on (y.run_id=s.run_id AND y.task_id=s.task_id AND y.try_id=s.try_id) "
                   +where+
                   "order by t.task_id,s.timestamp asc")
            pass
        if self.debug > 0 : print("getTaskData: sql = ",sql)

        ## Perform DB query
        (tRowz,tTitles) = self.stdQuery(sql)
        if self.debug > 1:
            self.dumpTable(tTitles,tRowz)
            pass

        
        ## Look at every task...
        rCount = 0
        grCount = 0   # 'good' row count
        nRunning = 0
        for row in tRowz:
            rCount += 1

            if taskStatus != None:
                ## Select tasks based on status or a collection of stati
                keep = False
                if taskStatus in list(self.statPresets.keys()):
                    for prekey in self.statPresets:
                        if taskStatus == prekey and row['task_status_name'] in self.statPresets[prekey]:
                            keep = True
                            break
                        pass
                    pass
                elif taskStatus in row['task_status_name']:
                    keep = True
                    pass
                if not keep: continue
                pass
            grCount += 1


            if self.debug > 1:
                print('rCount,task_stdout = ',rCount,row['task_stdout'])
                print(row)
                pass

            ## Prepare stdDir, directory containing stderr and stdout
            stdDir = row['task_stdout']
            if stdDir == None:
                stdDir = "None"
            else:
                stdDir = os.path.splitext(stdDir)[0]
                pass
                      
            ## Fill Task data list
            pTask = [row['task_id'],
                     row['task_func_name'],
                     self.runid2num[row['run_id']],
                     row['task_status_name'],
                     row['hostname'],
                     row['try_id'],
                     row['task_fail_count'],
                     self.stripms(row['task_try_time_launched']),
                     self.stripms(row['task_try_time_running']),
                     self.stripms(row['task_try_time_returned']),
                     self.timeDiff(row['task_try_time_running'],row['task_try_time_returned']),
                     stdDir
                     ]
                                  

            ## Fill nodeUsage dict (only for running tasks)
            if row['task_status_name'] == "running":
                nRunning += 1
                if row['hostname'] == None:
                    hn = 'unknown'
                else:
                    hn = row['hostname']
                    pass
                if hn not in self.nodeUsage:
                    self.nodeUsage[hn] = 1
                else:
                    self.nodeUsage[hn] += 1
                    pass
                pass

            ## Finally, add this task's data to the master summary list
            self.pTasks.append(pTask)
            
            ## (Possibly) limit # of tasks processed -- most a development/debugging feature
            if self.taskLimit > 0 and grCount >= self.taskLimit: break
            pass
        
        ## Print out full task table
        if printSummary: print(tabulate(self.pTasks,headers=list(self.pTitles.keys()),tablefmt=tblfmt))
        self.pTasksFilled = True

        ## Print out node usage summary
        if len(self.nodeUsage) > 0:
            print("\nNode usage summary:")
            headers = ['Node', '#running']
            print(tabulate(sorted(self.nodeUsage.items()), headers=headers, tablefmt=tblfmt))
            print('  Number of active nodes = ',len(self.nodeUsage))
            print('  Number of running tasks = ',nRunning)
        return

    #########################################################################################
    #########################################################################################
    #########################################################################################
    #########################################################################################
    #########################################################################################

    def printStatusMatrix(self,runnum=None):
        ## Confirm pTasks[] has been filled
        if not self.pTasksFilled: self.getTaskData(runnum=runnum,printSummary=False)

        ## if no tasks defined, bail
        if len(self.pTasks) < 1:
            print("Nothing to do: pTasks is empty!")
            return
        else:
            #print("There are ",len(self.pTasks)," tasks in pTasks.")
            pass


        ## Tally status for each task type
        ##  Store -> taskStats{}:
        ##     taskStats{'taskname1':{#status1:num1,#status2:num2,...},...}
        statList = self.statTemplate.keys()   # list of all status names
        taskStats = {}   # {'taskname':{statTemplate}}
        tNameIndx = self.pTitles['task_name']
        tStatIndx = self.pTitles['status']
        tRunIndx  = self.pTitles['runTime']
        nTaskTypes = 0
        nTasks = 0
        statTotals=dict(self.statTemplate)
        statTotals['TOTAL'] = 0
        for task in self.pTasks:
            nTasks += 1
            tName = task[tNameIndx]
            tStat = task[tStatIndx]
            if tName not in taskStats.keys():
                nTaskTypes += 1
                taskStats[tName] = dict(self.statTemplate)
                taskStats[tName]['TOTAL'] = 0
                pass
            taskStats[tName][tStat] += 1
            taskStats[tName]['TOTAL'] += 1
            statTotals[tStat] += 1
            statTotals['TOTAL'] += 1
            pass
        taskStats['TOTAL'] = dict(statTotals)
        
        ## Then convert into a Pandas dataframe
        ##    [Defining a pandas dataframe]
        ##    df = pandas.DataFrame(columns=['a','b','c','d'], index=['x','y','z'])
        ##    df.loc['y'] = pandas.Series({'a':1, 'b':5, 'c':2, 'd':3})
        pTaskStats = pd.DataFrame(columns=list(self.statTemplate.keys())+['TOTAL'], index=taskStats.keys())
        for task in taskStats:
            pTaskStats.loc[task] = pd.Series(taskStats[task])
            pass
        #print('\nTask status matrix: \n',pTaskStats,'\n\n')    ## native DataFrame formatted output
        print('\nTask status matrix:')
        print(tabulate(pTaskStats,headers='keys',tablefmt=tblfmt))
        return




    
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

    
    #########################################################################################
    #########################################################################################
    #########################################################################################
    #########################################################################################
    #########################################################################################

    # def printTaskSummary(self,runnum=None,opt=None):
    #     ##############################################################
    #     ## (NOTE: as of 4/20/2020 this function is mostly obsolete,
    #     ## use getTaskData() instead)
    #     ##############################################################
    #     print('\n\n\n%ATTENTION: printTaskSummary is obsolete and disabled.  Use "taskSummary" instead.\n\n')
    #     return
    #     ##############################################################
    #     ##############################################################




        
    #     ## The task summary is a composite presentation of values from
    #     ## the 'task' and 'status' tables

    #     ##  Select requested Run in workflow table
    #     rowindex = self.selectRunID(runnum)
    #     wrow = self.wrows[rowindex]
    #     if runnum == None: runnum = int(self.runid2num[wrow['run_id']])

    #     ##  Header
    #     header = '\n\nTask summary for run '+str(runnum)
    #     if runnum == int(self.runmax):header += ' [most current run]'
    #     print(header,'\n===========================================')

    #     ##  Query the 'task' table for data to present
    #     runID = wrow['run_id']
    #     sql = ('select task_id,task_func_name,hostname,task_fail_count,task_time_submitted,'
    #            'task_time_running,task_time_returned,task_elapsed_time,task_stdout '
    #            'from task '
    #            'where run_id = "'+wrow['run_id']+'" '
    #            'order by task_id asc')
    #     (tRowz,tTitles) = self.stdQuery(sql)

        
    #     ## Convert from sqlite3.Row to a simple 'list'
    #     tRows = []
        
    #     ## Adjust data for presentation
    #     logDir = 'not specified'
    #     if tRowz[0]['task_stdout'] != None: logDir = os.path.dirname(tRowz[0]['task_stdout'])
    #     stdoutIndx = tTitles.index('task_stdout')
    #     elapsedIndx = tTitles.index('task_elapsed_time')
    #     subTimeIndx = tTitles.index('task_time_submitted')
    #     startTimeIndx = tTitles.index('task_time_running')
    #     endTimeIndx = tTitles.index('task_time_returned')
        
    #     for rw in tRowz:
    #         tRows.append(list(rw))
    #         if tRows[-1][stdoutIndx] != None:
    #             tRows[-1][stdoutIndx] = os.path.basename(tRows[-1][stdoutIndx])  ## Remove stdout file path
    #         if tRows[-1][elapsedIndx] != None:
    #             a = datetime.timedelta(seconds=int(tRows[-1][elapsedIndx]))
    #             tRows[-1][elapsedIndx] = str(a)
    #             pass

    #         ## Calculate run duration (wall clock time while running)
    #         startTime = tRows[-1][startTimeIndx]
    #         endTime = tRows[-1][endTimeIndx]
    #         tRows[-1][elapsedIndx] = self.timeDiff(startTime,endTime)

    #         for ix in [subTimeIndx,startTimeIndx,endTimeIndx]:
    #             if tRows[-1][ix] != None:
    #                 tRows[-1][ix] = self.stripms(tRows[-1][ix])
    #                 pass
    #             pass
    #         pass
            

    #     ## Construct summary
    #     numTasks = len(tRows)
    #     durationSec = wrow['workflow_duration']
    #     if durationSec == None:
    #         print('workflow script has not reported completion, i.e., running, crashed, or killed')
    #     else:
    #         duration = datetime.timedelta(seconds=int(durationSec))
    #         print('workflow elapsed time = ',duration,' (hh:mm:ss)')
    #         print('number of Tasks launched = ',numTasks)
    #         if numTasks == 0:return
    #         pass

    #     ## Extract status data from 'status' table
    #     tTitles.insert(2, "status")
    #     tStat = dict(self.statTemplate)
    #     for row in range(numTasks):
    #         taskID = tRows[row][0]
    #         #print('runID = ',runID,', taskID = ',taskID)
    #         sql = ('select task_id,timestamp,task_status_name '
    #                'from status '
    #                'where run_id="'+str(runID)+'" and task_id="'+str(taskID)+'" '
    #                'order by timestamp desc limit 1')
    #         (sRowz,sTitles) = self.stdQuery(sql)
    #         if self.debug > 4: self.dumpTable(sTitles,sRowz)
    #         taskStat = sRowz[0]['task_status_name'] 
    #         if taskStat not in tStat:
    #             print("%ERROR: new task status encountered: ",taskStat)
    #             tStat['unknown'] += 1
    #         else:
    #             tStat[taskStat] += 1
    #             pass
    #         tRows[row].insert(2, taskStat)
    #         pass

    #     ## Adjust titles (mostly to make them smaller)
    #     tTitles[tTitles.index('task_fail_count')] = '#fails'
    #     tTitles[tTitles.index('task_elapsed_time')] = 'duration\nhh:mm:ss'
        

    #     ## Pretty print task summary

    #     if opt == None:                 ## "Full" task summary
    #         print(tabulate(tRows,headers=tTitles,tablefmt=tblfmt))
    #         print('Task Status Summary: ',tStat)
    #         print('Log file directory: ',logDir)
    #     elif opt == "short":            ## "Short" task summary
    #         sSum = []
    #         for stat in tStat:
    #             sSum.append([stat,tStat[stat]])
    #         sSum.append(['total tasks',str(numTasks)])
    #         print(tabulate(sSum,['State','#'],tablefmt=tblfmt))
    #         pass

    #     return


    ####################
    ## Driver functions
    ####################

    def taskSummary(self,runnum=None,repType="Summary",printSummary=True,taskID=None,taskStatus=None,taskName=None):
        ## This is the new standard summary: workflow summary + summary of tasks in current run
        self.printWorkflowSummary(runnum)
        self.getTaskData(repType=repType,printSummary=printSummary,taskID=taskID,taskStatus=taskStatus,taskName=taskName)
        self.printStatusMatrix(runnum=runnum)
        return


    # def fullSummary(self,runnum=None):
    #     ## This is the standard summary: workflow summary + summary of tasks in current run
    #     self.printWorkflowSummary(runnum)
    #     self.printTaskSummary(runnum)
    #     return


    def shortSummary(self,runnum=None):
        ## This is the short summary:
        self.printWorkflowSummary(runnum)
        self.printStatusMatrix(runnum=runnum)
        ##self.printTaskSummary(runnum,opt='short')
        return

    def plot(self,runnum=None):
        self.printWorkflowSummary()
        #self.getTaskData(runnum=runnum,dig=True,printSummary=False)
        self.printStatusMatrix(runnum=runnum)
        self.plotStats(runnum=runnum)
        return

    def runHistory(self):
        ## This is the runHistory: details for each workflow 'run'
        sql = ('select workflow_name,user,host,time_began,time_completed,'
               'tasks_completed_count,tasks_failed_count,rundir '
               'from workflow '
               'order by time_began asc')
        (wrows,wtitles) = self.stdQuery(sql)
        if self.debug > 1:
            print('wtitles = ',wtitles)
            print('Number of runs = len(wrows) = ',len(wrows))
            for wrow in wrows :
                print(list(wrow))
                pass
            pass
        
        ## Adjust the result set for printing
        for i in list(range(len(wtitles))):
            if wtitles[i] == 'tasks_completed_count':wtitles[i] = '#tasks_good'
            if wtitles[i] == 'tasks_failed_count':wtitles[i] = '#tasks_bad'
            pass
        rows = []
        wtitles.insert(0,"RunNum")
        wtitles.insert(6,"RunDuration")
        for wrow in wrows:
            run_duration = self.timeDiff(wrow['time_began'],wrow['time_completed'])
            row = list(wrow)
            row.insert(5,run_duration)
            row[3] = self.stripms(row[3])
            row[4] = self.stripms(row[4])
            if run_duration is None:
                row[4] = '-> incomplete <-'
            else:
                row[5] = datetime.timedelta(days=run_duration.days,seconds=run_duration.seconds)
                pass
            row.insert(0,os.path.basename(row[8]))
            rows.append(row)
        ## Print the report
        print(tabulate(rows,headers=wtitles, tablefmt=tblfmt))
        return


    def runNums(self):
        ## Simply print a table of the serial run number vs Parsl (lengthy) run_id
        print("Sequential (time-ordered) Run Numbers vs their corresponding Parsl run_id's")
        rlist = self.runid2num.keys()
        rtable=[]
        for run in rlist:
            if self.debug > 0: print(self.runid2num[run],run)
            rtable.append([self.runid2num[run],run])
            pass
        #print(self.runnum2id)
        #print(self.runid2num)
        print(tabulate(rtable,headers=["RunNum","parsl run_id"],tablefmt=tblfmt))
        return

#############################################################################
#############################################################################
##
##                                   M A I N
##
#############################################################################
#############################################################################


if __name__ == '__main__':


    reportTypes = ['shortSummary','taskSummary','taskHistory','runNums','runHistory','plot']

    ## Parse command line arguments
    parser = argparse.ArgumentParser(description='A simple Parsl status reporter.  Available reports include:'+str(reportTypes))
    parser.add_argument('reportType',help='Type of report to display (default=%(default)s)',nargs='?',default='shortSummary')
    parser.add_argument('-f','--file',default='./monitoring.db',help='name of Parsl monitoring database file (default=%(default)s)')
    parser.add_argument('-r','--runnum',type=int,help='Specific run number of interest (default = latest)')
    parser.add_argument('-s','--schemas',action='store_true',default=False,help="only print out monitoring db schema for all tables")
    parser.add_argument('-t','--taskID',default=None,help="specify task_id")
    parser.add_argument('-n','--taskName',default=None,help="specify task_func_name")
    parser.add_argument('-S','--taskStatus',default=None,help="specify task_status_name")
    parser.add_argument('-l','--taskLimit',type=int,default=0,help="limit output to N tasks (default is no limit)")
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
    m = pmon(dbfile=args.file)
    m.debug = args.debug

    ## Print out table schemas only
    if args.schemas:
        ## Fetch a list of all tables in this database
        tableList = m.getTableList()
        print('Tables: ',tableList)

        ## Print out schema for all tables
        for table in tableList:
            schema = m.getTableSchema(table)
            print(schema[0][0])
            pass
        sys.exit()

    ## Check validity of run number
    if not args.runnum == None and (int(args.runnum) > m.runmax or int(args.runnum) < m.runmin):
        print('%ERROR: Requested run number, ',args.runnum,' is out of range (',m.runmin,'-',m.runmax,')')
        sys.exit(1)

    ## Set task output limit?
    if args.taskLimit > 0:
        m.taskLimit = args.taskLimit
        pass
        
    ## Print out requested report
    if args.reportType == 'taskSummary':
        #m.taskLimit=100
        m.taskSummary(runnum=args.runnum,repType="Summary",printSummary=True,taskID=args.taskID,taskStatus=args.taskStatus,taskName=args.taskName)
    elif args.reportType == 'shortSummary':
        m.shortSummary(runnum=args.runnum)
    elif args.reportType == 'taskHistory':
        m.taskSummary(runnum=args.runnum,repType="History",printSummary=True,taskID=args.taskID,taskStatus=args.taskStatus,taskName=args.taskName)
    elif args.reportType == 'runHistory':
        m.runHistory()
    elif args.reportType == 'plot':
        m.plot()
    elif args.reportType == 'runNums':
        m.runNums()
    else:
        print("%ERROR: Unrecognized reportType: ",args.reportType)
        print("Must be one of: ",reportTypes)
        print("Exiting...")
        sys.exit(1)
        pass
    
    ## Done
    endTime = datetime.datetime.now()
    print("wstat elapsed time = ",endTime-startTime)
    sys.exit()

