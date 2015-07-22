'''
/*#############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################ */
'''

import logging
import os
import sys
import time
import thread
import threading

from ..common.config import Config
from ..common.error import Error
from ..common.logger import Logger
from ..common.report import Report, Tee
from ..regression.suite import Suite
from ..util.ecl.cc import ECLCC
from ..util.ecl.command import ECLcmd
from ..util.expandcheck import ExpandCheck
from ..util.util import getConfig, queryWuid,  abortWorkunit, getVersionNumbers


class Regression:

    def timeoutHandler(self):
        for th in range(self.maxthreads):
            if self.timeouts[th] > 0:
                self.timeouts[th] -= 1

        if self.timeoutHandlerEnabled:
            self.timeoutThread = threading.Timer(1.0,  self.timeoutHandler)
            self.timeoutThread.start()

    def __init__(self, args):
        self.args = args
        self.config = getConfig()
        self.suites = {}
        self.log = Logger(args.loglevel)
        if args.timeout == '0':
            self.timeout = int(self.config.timeout);
        else:
            self.timeout = int(args.timeout)
        logging.debug("Suite timeout: %d sec / testcase", self.timeout)
        if not args.suiteDir:
            self.suiteDir = self.config.suiteDir
            if not self.suiteDir:
                raise Error("2002")
        else:
            self.suiteDir = args.suiteDir

        if args.keyDir == self.config.keyDir:
            self.keyDir = self.config.keyDir
            if not self.keyDir:
                raise Error("2003")
        else:
            self.keyDir = args.keyDir
            logging.debug("Try to use alternative key directory: %s", self.keyDir)

        self.suiteDir = ExpandCheck.dir_exists(self.suiteDir, True)
        self.regressionDir = ExpandCheck.dir_exists(self.config.regressionDir, True)
        self.logDir = ExpandCheck.dir_exists(self.config.logDir, True)
        self.dir_ec = ExpandCheck.dir_exists(os.path.join(self.suiteDir, self.config.eclDir), True)
        self.dir_ex = ExpandCheck.dir_exists(os.path.join(self.suiteDir, self.keyDir), True)
        self.dir_a = os.path.join(self.regressionDir, self.config.archiveDir)
        self.dir_r = os.path.join(self.regressionDir, self.config.resultDir)
        logging.debug("Suite Dir      : %s", self.suiteDir)
        logging.debug("Regression Dir : %s", self.regressionDir)
        logging.debug("Result Dir     : %s", self.dir_r)
        logging.debug("Log Dir        : %s", self.logDir)
        logging.debug("ECL Dir        : %s", self.dir_ec)
        logging.debug("Key Dir        : %s", self.dir_ex)
        logging.debug("Archive Dir    : %s", self.dir_a)


        numOfThreads=1
        if 'pq' in args:
            if args.pq == 0:
                numOfThreads = 1;
            else:
                numOfThreads = args.pq
        self.loggermutex = thread.allocate_lock()
        self.numOfCpus = 2
        self.threadPerCpu = 2
        ver = getVersionNumbers()
        if numOfThreads == -1:
            if (ver['main'] >= 2) and (ver['minor'] >= 7):
                if 'linux' in sys.platform :
                    command = "grep 'core\|processor' /proc/cpuinfo | awk '{print $3}' | sort -nru | head -1"
                    cpuInfo = os.popen(command).read()
                    if cpuInfo == "":
                        self.numOfCpus = 1
                    else:
                        self.numOfCpus = int(cpuInfo)+1
                numOfThreads = self.numOfCpus  * self.threadPerCpu
            elif (ver['main'] <= 2) and (ver['minor'] < 7):
                    numOfThreads = self.numOfCpus  * self.threadPerCpu
        logging.debug("Number of CPUs:%d, NUmber of threads:%d", self.numOfCpus, numOfThreads  )

        self.maxthreads = numOfThreads
        self.maxtasks = 0
        self.exitmutexes = [thread.allocate_lock() for i in range(self.maxthreads)]
        self.timeouts = [(-1) for i in range(self.maxthreads)]
        self.timeoutHandlerEnabled = False;
        self.timeoutThread = threading.Timer(1.0,  self.timeoutHandler)

    def setLogLevel(self, level):
        self.log.setLevel(level)

    def bootstrap(self, cluster, args,   fileList=None):
        self.createDirectory(self.regressionDir)
        self.createDirectory(self.dir_a)
        self.createDirectory(self.dir_r)
        self.createDirectory(self.logDir)

        self.suites[cluster] = Suite(cluster, self.dir_ec, self.dir_a, self.dir_ex, self.dir_r, self.logDir, args, False, fileList)
        self.maxtasks = len(self.suites[cluster].getSuite())

    def createDirectory(self, dir_n):
        if not os.path.isdir(dir_n):
            os.makedirs(dir_n)

    def Setup(self,  args):
        self.createDirectory(self.regressionDir)
        self.createDirectory(self.dir_a)
        self.createDirectory(self.dir_r)
        self.createDirectory(self.logDir)
        self.setupDir = ExpandCheck.dir_exists(os.path.join(self.suiteDir, self.config.setupDir), True)
        logging.debug("Setup Dir      : %s", self.setupDir)
        self.setupSuite = Suite(args.target, self.setupDir, self.dir_a, self.dir_ex, self.dir_r, self.logDir, args, True)
        self.maxtasks = len(self.setupSuite.getSuite())
        return self.setupSuite

    def buildLogging(self, name):
        report = Report(name)
        curTime = time.strftime("%y-%m-%d-%H-%M-%S")
        logName = name + "." + curTime + ".log"
        self.args.testId=curTime
        logHandler = os.path.join(self.logDir, logName)
        self.args.testFile=logHandler
        self.saveConfig()
        self.log.addHandler(logHandler, 'DEBUG')
        return (report, logHandler)

    def closeLogging(self):
        self.log.removeHandler()

    def saveConfig(self):
        confLogName = 'environment-'+self.args.testId + ".conf"
        logFileName = os.path.join(self.logDir, confLogName)
        try:
            log = open(logFileName, "w");
            log.write("Environment info\n")
            log.write("Args:\n")
            for arg in self.args.__dict__:
                argStr = arg +'=\"'+str(self.args.__dict__[arg])+'\"'
                log.write(argStr+"\n")

            log.write("\nConfigs:\n")
            for conf in self.config.__dict__:
                if conf != '_dict__d':
                    confStr = conf +'=\"'+str(self.config.__dict__[conf])+'\"'
                    log.write(confStr+"\n")
                else:
                    for subConf in self.config.__dict__[conf]:
                        confStr = subConf +'=\"'+str(self.config.__dict__[conf][subConf])+'\"'
                        log.write(confStr+"\n")
            log.close()
        except IOError:
            logging.error("Can't open %s file to write!" %(logFileName))

    @staticmethod
    def displayReport(report,  elapsTime=0):
        report[0].display(report[1],  elapsTime)

    def runSuiteP(self, name, suite):
        if name == "setup":
            cluster = 'hthor'
        else:
            cluster = name

        logName = name
        if 'setup' in suite.getSuiteName():
            logName ='setup_'+name
            name = name + ' (setup)'

        report = self.buildLogging(logName)

        self.taskParam = []
        self.taskParam = [{'taskId':0,  'jobName':'',  'timeoutValue':0,  'retryCount': 0} for i in range(self.maxthreads)]
        self.goodStates = ('compiling', 'blocked')

        logging.debug("runSuiteP(name:'%s', suite:'%s')" %  (name,  suite.getSuiteName()))
        logging.warn("Suite: %s ",  name)
        logging.warn("Queries: %s" % repr(len(suite.getSuite())))
        logging.warn('%s','' , extra={'filebuffer':True,  'filesort':True})
        cnt = 0
        oldCnt = -1
        suite.setStarTime(time.time())
        suiteItems = suite.getSuite()
        try:
            self.StartTimeoutThread()
            while cnt in range(self.maxtasks):
                if oldCnt != cnt:
                    query = suiteItems[cnt]
                    query.setTaskId(cnt+1)
                    query.setIgnoreResult(self.args.ignoreResult)
                    query.setJobname(time.strftime("%y%m%d-%H%M%S"))
                    timeout = query.getTimeout()
                    oldCnt = cnt

                started = False
                for threadId in range(self.maxthreads):

                    for startThreadId in range(self.maxthreads):
                        if not self.exitmutexes[startThreadId].locked():
                            # Start a new test case with a reused thread id
                            self.taskParam[startThreadId]['taskId']=cnt
                            cnt += 1
                            if timeout != 0:
                                self.timeouts[startThreadId] = timeout
                            else:
                                self.timeouts[startThreadId] = self.timeout

                            self.taskParam[startThreadId]['timeoutValue'] = self.timeout
                            query = suiteItems[self.taskParam[startThreadId]['taskId']]
                            query.setTimeout(self.timeout)
                            #logging.debug("self.timeout[%d]:%d", startThreadId, self.timeouts[startThreadId])
                            self.taskParam[startThreadId]['jobName'] = query.getJobname()
                            self.taskParam[startThreadId]['retryCount'] = int(self.config.maxAttemptCount)
                            self.exitmutexes[startThreadId].acquire()
                            sysThreadId = thread.start_new_thread(self.runQuery, (cluster, query, report, cnt, suite.testPublish(query.ecl),  startThreadId))
                            started = True
                            break

                    if started:
                        break

                    if self.exitmutexes[threadId].locked():
                        if self.timeouts[threadId] % 10 == 0:
                            self.loggermutex.acquire()
                            logging.debug("%3d. timeout counter:%d" % (self.taskParam[threadId]['taskId']+1, self.timeouts[threadId]),  extra={'taskId':self.taskParam[threadId]['taskId']+1})
                            self.loggermutex.release()
                        if self.timeouts[threadId] == 0:
                            # time out occured
                            wuid =  queryWuid(self.taskParam[threadId]['jobName'],  self.taskParam[threadId]['taskId']+1)
                            if ("Not found" in wuid['wuid'] ) or (wuid['state'] in self.goodStates):
                                #Possible blocked, give it more time if it is possible
                                self.taskParam[threadId]['retryCount'] -= 1;
                                if self.taskParam[threadId]['retryCount'] > 0:
                                    self.timeouts[threadId] =  self.taskParam[threadId]['timeoutValue']
                                    self.loggermutex.acquire()
                                    logging.warn("%3d. Does not started yet. Reset timeout to %d sec." % (self.taskParam[threadId]['taskId']+1, self.taskParam[threadId]['timeoutValue']),  extra={'taskId':self.taskParam[threadId]['taskId']+1})
                                    logging.debug("%3d. Task parameters: thread id: %d, ecl:'%s',state:'%s', retry count:%d." % (self.taskParam[threadId]['taskId']+1, threadId,  suiteItems[self.taskParam[threadId]['taskId']].ecl,   wuid['state'],  self.taskParam[threadId]['retryCount'] ),  extra={'taskId':self.taskParam[threadId]['taskId']+1})
                                    self.loggermutex.release()
                                else:
                                    # retry counter exhausted, give up and abort this test case if exists
                                    if 'W' in wuid['wuid']:
                                        abortWorkunit(wuid['wuid'])
                                        self.loggermutex.acquire()
                                        query = suiteItems[self.taskParam[threadId]['taskId']]
                                        query.setAborReason('Timeout and retry count exhausted!')
                                        logging.info("%3d. Timeout occured and no more attempt left. Force to abort... " % (self.taskParam[threadId]['taskId']),  extra={'taskId':self.taskParam[threadId]['taskId']+1})
                                        logging.debug("%3d. Task parameters: thread id:%d, wuid:'%s', state:'%s', ecl:'%s'." % (self.taskParam[threadId]['taskId']+1, threadId, wuid['wuid'], wuid['state'],  query.ecl),  extra={'taskId':self.taskParam[threadId]['taskId']+1})
                                        self.loggermutex.release()
                                    else:
                                        self.exitmutexes[threadId].release()
                                        self.loggermutex.acquire()
                                        query = suiteItems[self.taskParam[threadId]['taskId']]
                                        query.setAborReason('Timeout (does not started yet and retry count exhausted)')
                                        logging.info("%3d. Timeout occured and no more attempt left. Force to abort... " % (self.taskParam[threadId]['taskId']),  extra={'taskId':self.taskParam[threadId]['taskId']+1})
                                        logging.debug("%3d. Task parameters: thread id:%d, wuid:'%s', state:'%s', ecl:'%s'." % (self.taskParam[threadId]['taskId']+1, threadId, wuid['wuid'], wuid['state'],  query.ecl),  extra={'taskId':self.taskParam[threadId]['taskId']+1})
                                        self.loggermutex.release()

                                    self.timeouts[threadId] = -1


                            elif   wuid['state'] =='completed':
                                # It is done in HPCC System but need some more time to complete
                                self.timeouts[threadId] =  5 # sec extra time to finish
                                self.loggermutex.acquire()
                                logging.info("%3d. It is completed in HPCC Sytem, but not finised yet. Give it %d sec." % (self.taskParam[threadId]['taskId']+1, self.taskParam[threadId]['timeoutValue']),  extra={'taskId':self.taskParam[threadId]['taskId']+1})
                                logging.debug("%3d. Task parameters: thread id: %d, ecl:'%s',state:'%s'." % (self.taskParam[threadId]['taskId']+1, threadId,  suiteItems[self.taskParam[threadId]['taskId']].ecl, wuid['state']),  extra={'taskId':self.taskParam[threadId]['taskId']+1})
                                self.loggermutex.release()
                            else:
                                # Something wrong with this test case, abort it.
                                abortWorkunit(wuid['wuid'])
                                self.loggermutex.acquire()
                                query = suiteItems[self.taskParam[threadId]['taskId']]
                                query.setAborReason('Timeout')
                                logging.info("%3d. Timeout occured. Force to abort... " % (self.taskParam[threadId]['taskId']+1),  extra={'taskId':self.taskParam[threadId]['taskId']+1})
                                logging.debug("%3d. Task parameters: thread id:%d, wuid:'%s', state:'%s', ecl:'%s'." % (self.taskParam[threadId]['taskId']+1, threadId, wuid['wuid'], wuid['state'],  query.ecl),  extra={'taskId':self.taskParam[threadId]['taskId']+1})
                                self.loggermutex.release()
                                self.timeouts[threadId] = -1

                # give some time to other threads
                if not started:
                    time.sleep(0.2)

            # All tasks are scheduled
            #Some of them finished, others are not yet, but should check the still running tasks' timeout and retry state
            for threadId in range(self.maxthreads):
                    if self.exitmutexes[threadId].locked():
                        query = suiteItems[self.taskParam[threadId]['taskId']]
                        self.retryCount = int(self.config.maxAttemptCount)
                        self.CheckTimeout(self.taskParam[threadId]['taskId']+1, threadId,  query)

            self.StopTimeoutThread()
            logging.warn('%s','' , extra={'filebuffer':True,  'filesort':True})
            suite.setEndTime(time.time())
            Regression.displayReport(report, suite.getElapsTime())
            suite.close()
            self.closeLogging()

        except Exception as e:
            self.StopTimeoutThread()
            suite.close()
            raise(e)

        except KeyboardInterrupt as e:
            suite.close()
            raise(e)


    def StartTimeoutThread(self):
        self.timeoutThread.cancel()
        self.timeoutThread = threading.Timer(1.0,  self.timeoutHandler)
        self.timeoutHandlerEnabled=True
        self.timeoutThread.start()

    def CheckTimeout(self, cnt,  threadId,  query):
        while  self.exitmutexes[threadId].locked():
            sleepTime = 1.0
            if self.timeouts[threadId] >= 0:
                self.loggermutex.acquire()
                logging.debug("%3d. timeout counter:%d" % (cnt, self.timeouts[threadId]),  extra={'taskId':cnt})
                self.loggermutex.release()
                sleepTime = 1.0
            if self.timeouts[threadId] == 0:
                wuid =  queryWuid(query.getJobname(),  query.getTaskId())
                self.retryCount -= 1;
                if self.retryCount> 0:
                    self.timeouts[threadId] =  self.timeout
                    self.loggermutex.acquire()
                    logging.warn("%3d. Does not started yet. Reset timeout to %d sec (%d retry attempt(s) remains)." % (cnt, self.timeouts[threadId],  self.retryCount),  extra={'taskId':cnt})
                    logging.debug("%3d. Task parameters: thread id: %d, ecl:'%s',state:'%s', retry count:%d." % (cnt, threadId,  query.ecl,   wuid['state'],  self.retryCount),  extra={'taskId':cnt})
                    self.loggermutex.release()
                else:
                    # retry counter exhausted, give up and abort this test case if exists
                    logging.debug("%3d. Abort WUID:'%s'" % (cnt,  str(wuid)),  extra={'taskId':cnt})
                    abortWorkunit(wuid['wuid'])
                    query.setAborReason('Timeout and retry count exhausted!')
                    self.loggermutex.acquire()
                    logging.error("%3d. Timeout occured and no more attempt left. Force to abort... " % (cnt),  extra={'taskId':cnt})
                    logging.debug("%3d. Task parameters: wuid:'%s', state:'%s', ecl:'%s'." % (cnt, wuid['wuid'], wuid['state'],  query.ecl),  extra={'taskId':cnt})
                    logging.debug("%3d. Waiting for abort..." % (cnt),  extra={'taskId':cnt})
                    self.loggermutex.release()
                    self.timeouts[threadId] = -1
                    sleepTime = 1.0
            time.sleep(sleepTime)

    def StopTimeoutThread(self):
        self.timeoutHandlerEnabled=False
        self.timeoutThread.cancel()
        time.sleep(2)

    def runSuite(self, name, suite):
        if name == "setup":
            cluster = 'hthor'
        else:
            cluster = name

        logName = name
        if 'setup' in suite.getSuiteName():
            logName ='setup_'+name
            name = name + ' (setup)'

        report = self.buildLogging(logName)

        logging.debug("runSuite(name:'%s', suite:'%s')" %  (name,  suite.getSuiteName()))
        logging.warn("Suite: %s" % name)
        logging.warn("Queries: %s" % repr(len(suite.getSuite())))
        suite.setStarTime(time.time())
        cnt = 1
        th = 0
        try:
            self.StartTimeoutThread()
            for query in suite.getSuite():
                query.setJobname(time.strftime("%y%m%d-%H%M%S"))
                query.setTaskId(cnt)
                query.setIgnoreResult(self.args.ignoreResult)
                self.timeouts[th] = self.timeout
                timeout = query.getTimeout()
                if timeout != 0:
                   self.timeouts[th] = timeout
                else:
                    self.timeouts[th] = self.timeout
                self.retryCount = int(self.config.maxAttemptCount)
                query.setTimeout(self.timeouts[th])
                self.exitmutexes[th].acquire()
                thread.start_new_thread(self.runQuery, (cluster, query, report, cnt, suite.testPublish(query.ecl),  th))
                time.sleep(0.1)
                self.CheckTimeout(cnt, th,  query)
                cnt += 1

            self.StopTimeoutThread()

            suite.setEndTime(time.time())
            Regression.displayReport(report, suite.getElapsTime())
            suite.close()
            self.closeLogging()

        except Exception as e:
            self.StopTimeoutThread()
            suite.close()
            raise(e)

        except KeyboardInterrupt as e:
            suite.close()
            raise(e)

    def runSuiteQ(self, clusterName, eclfile):
        report = self.buildLogging(clusterName)
        logging.debug("runSuiteQ( clusterName:'%s', eclfile:'%s')",  clusterName,  eclfile.ecl,  extra={'taskId':0})

        if clusterName == "setup":
            cluster = 'hthor'
        else:
            cluster = clusterName

        cnt = 1
        eclfile.setTaskId(cnt)
        eclfile.setIgnoreResult(self.args.ignoreResult)
        threadId = 0
        logging.warn("Target: %s" % clusterName)
        logging.warn("Queries: %s" % 1)
        start = time.time()
        try:
            self.StartTimeoutThread()
            eclfile.setJobname(time.strftime("%y%m%d-%H%M%S"))
            self.timeouts[threadId] = self.timeout
            timeout = eclfile.getTimeout()
            if timeout != 0:
                self.timeouts[threadId] = timeout
            else:
                self.timeouts[threadId] = self.timeout
            self.retryCount = int(self.config.maxAttemptCount)
            self.exitmutexes[threadId].acquire()
            sysThreadId = thread.start_new_thread(self.runQuery, (cluster, eclfile, report, cnt, eclfile.testPublish(),  threadId))
            time.sleep(0.1)
            self.CheckTimeout(cnt, threadId,  eclfile)

            self.StopTimeoutThread()
            Regression.displayReport(report,  time.time()-start)
            eclfile.close()
            self.closeLogging()

        except Exception as e:
            self.StopTimeoutThread()
            eclfile.close()
            raise(e)

        except KeyboardInterrupt as e:
            eclfile.close()
            raise(e)

    def runQuery(self, cluster, query, report, cnt=1, publish=False,  th = 0):
        startTime = time.time()
        self.loggermutex.acquire()

        logging.debug("runQuery(cluster: '%s', query: '%s', cnt: %d, publish: %s, thread id: %d" % ( cluster, query.ecl, cnt, publish,  th))
        logging.warn("%3d. Test: %s" % (cnt, query.getBaseEclRealName()),  extra={'taskId':cnt})

        self.loggermutex.release()
        res = 0
        wuid = None
        if ECLCC().makeArchive(query):
            eclCmd = ECLcmd()
            try:
                if publish:
                    res = eclCmd.runCmd("publish", cluster, query, report[0],
                                      server=self.config.ip,
                                      username=self.config.username,
                                      password=self.config.password)
                else:
                    res = eclCmd.runCmd("run", cluster, query, report[0],
                                      server=self.config.ip,
                                      username=self.config.username,
                                      password=self.config.password)
            except Error as e:
                logging.debug("Exception raised:'%s'"  % ( str(e)),  extra={'taskId':cnt})
                res = False
                wuid = 'Not found'
                query.setWuid(wuid)
                query.diff = query.getBaseEcl()+"\n\t"+str(e)
                report[0].addResult(query)
                pass
            except:
                logging.error("Unexpected error:'%s'" %( sys.exc_info()[0]) ,  extra={'taskId':cnt})

            wuid = query.getWuid()
            logging.debug("CMD result: '%s', wuid:'%s'"  % ( res,  wuid),  extra={'taskId':cnt})
            if wuid == 'Not found':
                res = False
        else:
            res = False
            report[0].addResult(query)
            wuid="N/A"

        if wuid and wuid.startswith("W"):
            url = "http://" + self.config.ip+self.config.espSocket
            url += "/?Widget=WUDetailsWidget&Wuid="
            url += wuid
        else:
            url = "N/A"
            res = False

        self.loggermutex.acquire()
        elapsTime = time.time()-startTime
        if res:
            logging.info("%3d. Pass %s (%d sec)" % (cnt, wuid,  elapsTime),  extra={'taskId':cnt})
            logging.info("%3d. URL %s" % (cnt,url))
        else:
            if not wuid or not wuid.startswith("W"):
                logging.error("%3d. Fail No WUID (%d sec)" % (cnt,  elapsTime),  extra={'taskId':cnt})
            else:
                logging.error("%3d. Fail %s (%d sec)" % (cnt, wuid,  elapsTime),  extra={'taskId':cnt})
                logging.error("%3d. URL %s" %  (cnt,url),  extra={'taskId':cnt})
        self.loggermutex.release()
        query.setElapsTime(elapsTime)
        self.exitmutexes[th].release()

    def getConfig(self):
        return self.config

    @staticmethod
    def getTaskId(self):
        return self.taskId
