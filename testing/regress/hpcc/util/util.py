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

import argparse
import platform
import logging
import os
import subprocess

from ..common.error import Error
from ..common.shell import Shell

def isPositiveIntNum(string):
    for i in range(0,  len(string)):
        if (string[i] < '0') or (string[i] > '9'):
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

def getEclRunArgs(test,  cluster):
    retString=''
    test.setJobname("")
    retString += "ecl run -fpickBestEngine=false --target=%s --cluster=%s --port=%s " % (cluster, cluster, gConfig.espSocket)
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
    logging.debug("%3d. queryWuid(%s, cmd :'%s') result is: '%s'",  taskId,  jobname, cmd,  res)
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

def abortWorkunit(wuid):
    shell = Shell()
    cmd = 'ecl'
    defaults=[]
    args = []
    args.append('abort')
    args.append('-wu=' + wuid)
    addCommonEclArgs(args)

    state=shell.command(cmd, *defaults)(*args)
    return state

def createZAP(wuid,  taskId,  reason=''):
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
        logging.debug("%3d. createZAP(state:%s)",  taskId, str(state))
        if state[1] != '':
            retVal = state[1]
        else:
            retVal = state[0]
    except Exception as ex:
        state = "Unable to query "+ str(ex)
        logging.debug("%3d. %s in createZAP(%s)",  taskId,  state,  wuid)
        retVal += " (" + str(ex). replace('\n',' ') + ")"

    return retVal

def getRealIPAddress():
    ipAddress = '127.0.0.1'
    found = False
    try:
        proc = subprocess.Popen(['ip', '-o', '-4', 'addr', 'show'], shell=False,  bufsize=8192, stdout=subprocess.PIPE, stderr=None)
        result = proc.communicate()[0]
        results = result.split('\n')
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
    targetClusters =[]
    if 'all' in clusters:
        for cluster in gConfig.Clusters:
            targetClusters.append(str(cluster))
    else:
        for cluster in clusters:
            cluster = cluster.strip()
            if cluster in gConfig.Clusters:
                targetClusters.append(cluster)
            else:
                logging.error("%s. Unknown cluster:'%s' in %s:'%s'!" % (1,  cluster,  targetSet,  clusters))
                raise Error("4000")

    return  targetClusters

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
            results = result.split('\n')
            for line in results:
                if 'not found' in line:
                    err = Error("6000")
                    logging.error("%s. %s:'%s'" % (1,  err,  line))
                    raise  err
                    break
        else:
            # Maybe use SSH to run  "ecl --version" on a remote node
            pass

        args = []
        addCommonEclArgs(args)

        myProc = subprocess.Popen("ecl getname --wuid 'W*' --limit=5 " + " ".join(args),  shell=True,  bufsize=8192,  stdout=subprocess.PIPE,  stderr=subprocess.PIPE)
        result  = myProc.stdout.read() + myProc.stderr.read()
        results = result.split('\n')
        for line in results:
            if "Error connecting" in line:
                if isLocal:
                    err = Error("6001")
                    logging.error("%s. %s:'%s local target!'" % (1,  err,  line))
                    raise (err)
                else:
                    err = Error("6004")
                    logging.error("%s. %s:'%s remote target!'" % (1,  err,  line))
                    raise (err)
                break

            if "command not found" in line:
                err = Error("6002")
                logging.error("%s. %s:'%s'" % (1,  err,  line))
                raise (err)
                break

        isIpChecked[ip] = True

    except  OSError:
        err = Error("6002")
        logging.error("%s. checkHpccStatus error:%s!" % (1,  err))
        raise Error(err)

    except ValueError:
        err = Error("6003")
        logging.error("%s. checkHpccStatus error:%s!" % (1,  err))
        raise Error(err)

    finally:
        pass
