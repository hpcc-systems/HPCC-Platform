'''
/*#############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems(R).

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

from . import argparse
import platform
import logging
import os
import subprocess
import sys
import traceback
import linecache

logger = logging.getLogger('RegressionTestEngine')

from ..common.error import Error
from ..common.shell import Shell

def isPositiveIntNum(numString):
    logger.debug("%3d. isPositiveIntNum() result is: '%s'",  -1, numString)
    for i in range(0,  len(numString)):
        if (numString[i] < '0') or (numString[i] > '9'):
            return False
    return True

def checkPqParam(string):
    if isPositiveIntNum(string) or (string == '-1'):
        value = int(string)
    else:
        msg = "Wrong value of threadNumber parameter: '"+string+"' !"
        raise argparse.ArgumentTypeError(msg)

    return value

def checkXParam(string):
    param=str(string)
    if len(param):
        if ('=' in param) or ('None' == param):
            value = param
        else:
            raise Error("5000",  err="But got argument:'%s'" % (param) )
    else:
        msg = "Missing argument of -X parameter!"
        raise argparse.ArgumentTypeError(msg)

    return value

def getVersionNumbers():
    version = platform.python_version_tuple()
    verNum = {'main':0,  'minor':0,  'patch':0}
    if isPositiveIntNum(version[0]):
        verNum['main'] = int(version[0])
    if isPositiveIntNum(version[1]):
        verNum['minor'] = int(version[1])
    if isPositiveIntNum(version[2]):
        verNum['patch'] = int(version[2])
    return(verNum);

def parentPath(osPath):
    # remove current dir
    [osPath, sep,  curDir] = osPath.rpartition(os.sep)
    return osPath

def convertPath(osPath):
    hpccPath = ''
    osPath = osPath.lstrip(os.sep)
    osPath = osPath.replace(os.sep,  '::')
    for i in range(0,  len(osPath)):
        if osPath[i] >= 'A' and osPath[i] <= 'Z':
            hpccPath = hpccPath +'^'
        hpccPath = hpccPath +osPath[i]

    return hpccPath

gConfig = None

def setConfig(config):
    global gConfig
    gConfig = config

def getConfig():
    return gConfig

def getEclRunArgs(test, engine, cluster):
    retString=''
    test.setJobname("")
    retString += "ecl run -fpickBestEngine=false --target=%s --cluster=%s --port=%s " % (engine, cluster, gConfig.espSocket)
    retString += "--exception-level=warning --noroot --name=\"%s\" " % (test.getJobname())
    retString += "%s " % (" ".join(test.getFParameters()))
    retString += "%s " % (" ".join(test.getDParameters()))
    retString += "%s " % (" ".join(test.getStoredInputParameters()))
    args = []
    addCommonEclArgs(args)
    retString += "%s " % (" ".join(args))
    return retString

def addCommonEclArgs(args):
    args.append('--server=' + gConfig.espIp)
    args.append('--username=' + gConfig.username)
    args.append('--password=' + gConfig.password)
    args.append('--port=' + gConfig.espSocket)
    if gConfig.useSsl.lower() == 'true':
        args.append('--ssl')


def queryWuid(jobname,  taskId):
    shell = Shell()
    cmd = 'ecl'
    defaults = []
    args = []
    args.append('status')
    args.append('-v')
    args.append('-n=' + jobname)
    addCommonEclArgs(args)

    res, stderr = shell.command(cmd, *defaults)(*args)
    logger.debug("%3d. queryWuid(%s, cmd :'%s') result is: '%s'",  taskId,  jobname, cmd,  res)
    wuid = "Not found"
    state = 'N/A'
    result = 'Fail'
    if len(res):
        resultItems = res.split(',')
        if len(resultItems) == 3:
            result = 'OK'
            for resultItem in resultItems:
                resultItem = resultItem.strip()
                [key, val] = resultItem.split(':')
                if key == 'ID':
                    wuid = val
                if key == 'state':
                    state = val
    return {'wuid':wuid, 'state':state,  'result':result}

def queryEngineProcess(engine,  taskId):
    retVal = []
    myProc = subprocess.Popen(["ps aux | egrep '"+engine+"' | egrep -v 'grep'"],  shell=True,  bufsize=8192,  stdout=subprocess.PIPE,  stderr=subprocess.PIPE)
    result = myProc.stdout.read() + myProc.stderr.read()
    results = result.decode("utf-8").split('\n')
    logger.debug("%3d. queryEngineProcess(engine: %s): process(es) :'%s'",  taskId,  engine,  results)
    for line in results:
        line = line.replace('\n','')
        logger.debug("%3d. queryEngineProcess(engine: %s): line:'%s'",  taskId,  engine,  line)
        if len(line):
            items = line.split()
            if len(items) >= 12:
                if engine in items[10]:
                    myProc2 = subprocess.Popen(["sudo readlink -f /proc/" + items[1] + "/exe"],  shell=True,  bufsize=8192,  stdout=subprocess.PIPE,  stderr=subprocess.PIPE)
                    result2 = myProc2.stdout.read().decode("utf-8").replace ('\n', '')
                    binPath = os.path.dirname(result2)
                    logger.debug("%3d. queryEngineProcess(engine: %s): binary: '%s', binPath:'%s'",  taskId,  engine, result2,  binPath)
                    if 'slavenum' in line:
                        ind = [items.index(i) for i in items if i.startswith('--slavenum')]
                        try:
                            slaveNum = items[ind[0]].split('=')[1]
                            retVal.append({ 'process' : result2, 'name' : os.path.basename(items[10]), 'slaveNum' : slaveNum, 'pid' : items[1], 'binPath': binPath})
                        except Exception as e:
                            logger.error("%3d. queryEngineProcess(engine: %s): slave number query failed:'%s'",  taskId,  engine, repr(e))
                    else:
                        retVal.append({ 'process' : result2, 'name' : os.path.basename(items[10]), 'slaveNum' : '', 'pid' : items[1], 'binPath': binPath})
    return retVal

def createStackTrace(wuid, proc, taskId, logDir = ""):
    # Execute this function from CLI:
    # ~/MyPython/RegressionSuite$ python -c 'import hpcc.util.util as util; p = util.queryEngineProcess("thormaster"); p+= util.queryEngineProcess("thorslave"); print p; [ util.createStackTrace("na", pp, -1, "~/HPCCSystems-regression/log") for pp in p]; '

    binPath = proc['process']
    pid = proc['pid']
    if logDir == "":
        outFile = os.path.expanduser(gConfig.logDir) + '/' + wuid +'-' + proc['name'] + proc['slaveNum'] + '.trace'
    else:
        outFile = os.path.expanduser(logDir) + '/' + wuid +'-' + proc['name'] + proc['slaveNum'] + '-' + pid + '.trace'
    logger.error("%3d. Create Stack Trace for %s%s (pid:%s) into '%s'" % (taskId, proc['name'], proc['slaveNum'], pid, outFile), extra={'taskId':taskId})
    
    cmd  = 'sudo gdb --batch --quiet -ex "set interactive-mode off" '
    cmd += '-ex "echo \nBacktrace for all threads\n==========================" -ex "thread apply all bt" '
    cmd += '-ex "echo \nRegisters:\n==========================\n" -ex "info reg" '
    cmd += '-ex "echo \nDisassembler:\n==========================\n" -ex "disas" '
    cmd += '-ex "quit" ' + binPath + ' ' + pid + ' > ' + outFile + ' 2>&1' 

    myProc = subprocess.Popen([ cmd ],  shell=True,  bufsize=8192,  stdout=subprocess.PIPE,  stderr=subprocess.PIPE)
    result = myProc.stdout.read() + myProc.stderr.read()
    logger.debug("%3d. Create Stack Trace result:'%s'", taskId, result)

def abortWorkunit(wuid, taskId = -1, engine = None):
    wuid = wuid.strip()
    logger.debug("%3d. abortWorkunit(wuid:'%s', engine: '%s')", taskId, wuid, str(engine))
    logger.debug("%3d. config: generateStackTrace: '%s'", taskId, str(gConfig.generateStackTrace))
    if (gConfig.generateStackTrace and (engine !=  None)):
        if isSudoer():
            if engine.startswith('thor'):
                hpccProcesses = queryEngineProcess("thormaster", taskId)
                hpccProcesses += queryEngineProcess("thorslave", taskId)
            elif engine.startswith('hthor'):
                hpccProcesses = queryEngineProcess("eclagent", taskId)
                hpccProcesses += queryEngineProcess("hthor", taskId)
            elif engine.startswith('roxie'):
                hpccProcesses = queryEngineProcess("roxie", taskId)
                
            if len(hpccProcesses) > 0:
                for p in hpccProcesses:
                    createStackTrace(wuid, p, taskId)
            else:
                logger.error("%3d. abortWorkunit(wuid:'%s', engine:'%s') related process to generate stack trace not found.", taskId, wuid, str(engine))
            pass
        else:
            err = Error("7100")
            logger.error("%s. clearOSCache error:%s" % (taskId,  err))
            logger.error(traceback.format_exc())
            raise Error(err)
            pass

    shell = Shell()
    cmd = 'ecl'
    defaults=[]
    args = []
    args.append('abort')
    args.append('-wu=' + wuid)
    addCommonEclArgs(args)

    state=shell.command(cmd, *defaults)(*args)
    return state

def createZAP(wuid, taskId,  reason=''):
    retVal = 'Error in create ZAP'
    zapFilePath = os.path.join(os.path.expanduser(gConfig.regressionDir), gConfig.zapDir)
    shell = Shell()
    cmd = 'ecl'
    defaults=[]
    args = []
    args.append('zapgen')
    args.append(wuid)
    args.append('--path=' + zapFilePath)
    if reason != '':
        args.append('--description=' + reason)
    else:
        args.append('--description="Failed in OBT"')

    args.append('--inc-thor-slave-logs')
    addCommonEclArgs(args)

    try:
        state=shell.command(cmd, *defaults)(*args)
        logger.debug("%3d. createZAP(state:%s)",  taskId, str(state))
        if state[1] != '':
            retVal = state[1]
        else:
            retVal = state[0]
    except Exception as ex:
        state = "Unable to query "+ str(ex)
        logger.debug("%3d. %s in createZAP(%s)",  taskId,  state,  wuid)
        retVal += " (" + str(ex). replace('\n',' ') + ")"

    return retVal

def getRealIPAddress():
    ipAddress = '127.0.0.1'
    found = False
    try:
        proc = subprocess.Popen(['ip', '-o', '-4', 'addr', 'show'], shell=False,  bufsize=8192, stdout=subprocess.PIPE, stderr=None)
        result = proc.communicate()[0]
        results = result.decode("utf-8").split('\n')
        for line in results:
            if 'scope global' in line:
                items = line.split()
                ipAddress = items[3].split('/')[0]
                found = True
                break;
        if not found:
            for line in results:
                items = line.split()
                ipAddress = items[3].split('/')[0]
                break;
    except  OSError:
        pass
    finally:
        pass

    return ipAddress

def checkClusters(clusters,  targetSet):
    targetEngines =[]
    if 'all' in clusters:
        for engine in gConfig.Engines:
            targetEngines.append(str(engine))
    else:
        for engine in clusters:
            engine = engine.strip()
            if engine in gConfig.Engines:
                targetEngines.append(engine)
            else:
                logger.error("%s. Unknown engine:'%s' in %s:'%s'!" % (1,  engine,  targetSet,  clusters))
                raise Error("4000")

    return  targetEngines

def isLocalIP(ip):
    retVal=False
    if '127.0.0.1' == ip:
        retVal = True
    elif ip == getRealIPAddress():
        retVal = True

    return retVal

def checkHpccStatus():
    # Check HPCC Systems status on local/remote target
    isLocal = False
    isIpChecked={}
    config = getConfig()
    ip = config.espIp
    isIpChecked[ip] = False
    isLocal = isLocalIP(ip)

    try:
        if isLocal:
            # There is no remote version (yet)
            myProc = subprocess.Popen(["ecl --version"],  shell=True,  bufsize=8192,  stdout=subprocess.PIPE,  stderr=subprocess.PIPE)
            result = myProc.stdout.read() + myProc.stderr.read()
            results = result.decode("utf-8").split('\n')
            for line in results:
                if 'not found' in line:
                    err = Error("6000")
                    logger.error("%s. %s:'%s'" % (1,  err,  line))
                    raise  err
                    break
        else:
            # Maybe use SSH to run  "ecl --version" on a remote node
            pass

        args = []
        addCommonEclArgs(args)

        myProc = subprocess.Popen("ecl getname --wuid 'W*' --limit=5 " + " ".join(args),  shell=True,  bufsize=8192,  stdout=subprocess.PIPE,  stderr=subprocess.PIPE)
        result  = myProc.stdout.read() + myProc.stderr.read()
        results = result.decode("utf-8").split('\n')
        for line in results:
            if "Error connecting" in line:
                if isLocal:
                    err = Error("6001")
                    logger.error("%s. %s:'%s local target!'" % (1,  err,  line))
                    raise (err)
                else:
                    err = Error("6004")
                    logger.error("%s. %s:'%s remote target!'" % (1,  err,  line))
                    raise (err)
                break

            if "command not found" in line:
                err = Error("6002")
                logger.error("%s. %s:'%s'" % (1,  err,  line))
                raise (err)
                break

        isIpChecked[ip] = True

    except  OSError:
        err = Error("6002")
        logger.error("%s. checkHpccStatus error:%s!" % (1,  err))
        raise Error(err)

    except ValueError:
        err = Error("6003")
        logger.error("%s. checkHpccStatus error:%s!" % (1,  err))
        raise Error(err)

    finally:
        pass

def isSudoer(testId = -1):
    retVal = False
    if 'linux' in sys.platform :
        tryCount = 5
        cmd = "timeout -k 2 2 sudo id && echo Access granted || echo Access denied"
        while tryCount > 0:
            tryCount -= 1
            
            myProc = subprocess.Popen([cmd], shell=True, bufsize=8192, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (myStdout,  myStderr) = myProc.communicate()
            myStdout = myStdout.decode("utf-8") 
            myStderr = myStderr.decode("utf-8")
            result = "returncode:" + str(myProc.returncode) + ", stdout:\n'" + myStdout + "', stderr:\n'" + myStderr + "'."
            logger.debug("%3d. isSudoer() result is: '%s' (try count is:%d)", testId, result, tryCount)
            
            if 'timeout: invalid option' in myStderr:
                logger.debug("%3d. isSudoer() result is: '%s'", testId, result)
                cmd = "timeout 2 sudo id && echo Access granted || echo Access denied"
                logger.debug("%3d. try is without '-k 2' parameter: '%s'", testId, cmd)
                continue
                
            if 'Access denied' not in myStdout:
                retVal = True
                break



        if retVal == False:
            logger.debug("%3d. isSudoer() result is: '%s'", testId, result)
    return retVal

def clearOSCache(testId = -1):
    if 'linux' in sys.platform :
        if isSudoer(testId):
            myProc = subprocess.Popen(["free; sudo -S sync; echo 3 | sudo tee /proc/sys/vm/drop_caches; free"], shell=True, bufsize=8192, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (myStdout,  myStderr) = myProc.communicate()
            myStdout = myStdout.decode("utf-8") 
            myStderr = myStderr.decode("utf-8")
            result = "returncode:" + str(myProc.returncode) + ", stdout:\n'" + myStdout + "', stderr:\n'" + myStderr + "'."
            logger.debug("%3d. clearOSCache() result is: '%s'",  testId, result)
        else:
            err = Error("7000")
            logger.error("%s. clearOSCache error:%s" % (testId,  err))
            logger.error(traceback.format_exc())
            raise Error(err)
    else:
        logger.debug("%3d. clearOSCache() not supported on %s.", testId, sys.platform)
    pass


def PrintException(msg = ''):
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    print ('EXCEPTION IN (%s, LINE %s CODE:"%s"): %s' % ( filename, lineno, line.strip(), msg))
    print(traceback.format_exc())
