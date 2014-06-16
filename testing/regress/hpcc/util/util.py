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

import argparse
import platform
import logging
import os

from ..common.error import Error
from ..common.shell import Shell

def isPositiveIntNum(string):
    for i in range(0,  len(string)):
        if (string[i] < '0') or (string[i] > '9'):
            return False
    return True

def checkPqParam(string):
    param = str(string)
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
            #logging.error("%s. Missing or wrong argument '%s' after -X parameter!\nIt should be 'name=val[,name2=val2..]'\n5000\n" % (1,  param))
            value="5000"
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

def convertPath(osPath):
    hpccPath = ''
    osPath = osPath.lstrip(os.sep)
    # remove current dir
    [osPath, sep,  curDir] = osPath.rpartition(os.sep)
    osPath = osPath.replace(os.sep,  '::')
    for i in range(0,  len(osPath)):
        if osPath[i] >= 'A' and osPath[i] <= 'Z':
            hpccPath = hpccPath +'^'
        hpccPath = hpccPath +osPath[i]

    return hpccPath

import json
import urllib2

gConfig = None

def setConfig(config):
    global gConfig
    gConfig = config

def getConfig():
    return gConfig

def queryWuid(jobname,  taskId):
    shell = Shell()
    cmd = shell.which('ecl')
    defaults = []
    args = []
    args.append('status')
    args.append('-v')
    args.append('-n=' + jobname)
    args.append('--server=' + gConfig.ip)
    args.append('--username=' + gConfig.username)
    args.append('--password=' + gConfig.password)
    res = shell.command(cmd, *defaults)(*args)
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
    cmd = shell.which('ecl')
    defaults=[]
    args = []
    args.append('abort')
    args.append('-wu=' + wuid)
    args.append('--server=' + gConfig.ip)
    args.append('--username=' + gConfig.username)
    args.append('--password=' + gConfig.password)
    state=shell.command(cmd, *defaults)(*args)
    return state

import subprocess

def getRealIPAddress():
    ipAddress = '127.0.0.1'
    try:
        result = subprocess.Popen("ifconfig",  shell=False,  bufsize=8192,  stdout=subprocess.PIPE).stdout.read()
        ethernetFound=False
        results = result.split('\n')
        for line in results:
            if 'Ethernet' in line:
                ethernetFound=True

            if ethernetFound and 'inet addr' in line:
                items = line.split()
                ipAddress = items[1].split(':')[1]
                break;
    except  OSError:
        pass
    finally:
        pass

    return ipAddress
