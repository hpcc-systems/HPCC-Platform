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

import difflib
import logging
import os
import traceback
import re

from ...util.util import isPositiveIntNum, getConfig

class ECLFile:
    ecl = None
    xml_e = None
    xml_r = None
    xml_a = None
    ecl_c = None
    dir_ec = None
    dir_ex = None
    dir_r = None
    dir_a = None
    diff = ''
    wuid = None
    elapsTime = 0
    jobname = ''
    abortReason = ''
    taskId = -1
    ignoreResult=False

    def __init__(self, ecl, dir_a, dir_ex, dir_r,  cluster, args):
        self.dir_ec = os.path.dirname(ecl)
        self.dir_ex = dir_ex
        self.dir_r = dir_r
        self.dir_a = dir_a
        self.cluster = cluster;
        baseEcl = os.path.basename(ecl)
        self.basename = os.path.splitext(baseEcl)[0]
        baseXml = self.basename + '.xml'
        self.ecl = baseEcl
        self.xml_e = baseXml
        self.xml_r = baseXml
        self.xml_a = 'archive_' + baseXml
        self.jobname = self.basename
        self.diff = ''
        self.abortReason =''

        self.optX =[]
        self.optXHash={}
        self.config = getConfig()
        try:
            # Process definitions of stored input value(s) from config
            for param in self.config.Params:
                [testSpec,  val] = param.split(':')

                if '*' in testSpec:
                    testSpec = testSpec.replace('*',  '\w+')

                testSpec = testSpec.replace('.',  '\.')
                match = re.match(testSpec,  baseEcl)
                if match:
                    optXs = ("-X"+val.replace(',',  ',-X')).split(',')
                    self.processKeyValPairs(optXs,  self.optXHash)
                pass
        except AttributeError:
            # It seems there is no Params array in the config file
            pass

        # Process -X CLI parameters
        if args.X != 'None':
            args.X[0]=self.removeQuote(args.X[0])
            optXs = ("-X"+args.X[0].replace(',',  ',-X')).split(',')
            self.processKeyValPairs(optXs,  self.optXHash)
            pass

        # Process setupExtraX parameters if any
        if 'setupExtraX' in args:
            args.setupExtraX[0]=self.removeQuote(args.setupExtraX[0])
            optXs = ("-X"+args.setupExtraX[0].replace(',',  ',-X')).split(',')
            self.processKeyValPairs(optXs,  self.optXHash)
            pass

        self.mergeHashToStrArray(self.optXHash,  self.optX)
        pass

        self.optF =[]
        self.optFHash={}
        #process -f CLI parameters
        if args.f != 'None':
            args.f[0]=self.removeQuote(args.f[0])
            optFs = ("-f"+args.f[0].replace(',',  ',-f')).split(',')
            self.processKeyValPairs(optFs,  self.optFHash)
            pass

        self.mergeHashToStrArray(self.optFHash,  self.optF)
        pass

    def processKeyValPairs(self,  optArr,  optHash):
        for optStr in optArr:
            [key,  val] = optStr.split('=')
            key = key.strip().replace(' ', '')  # strip spaces around and inside the key
            val = val.strip()                               # strip spaces around the val
            if ' ' in val:
                val = '"' + val + '"'
            optHash[key] = val

    def mergeHashToStrArray(self, optHash,  strArray):
        # Merge all parameters into a string array
        for key in optHash:
            strArray.append(key+'='+optHash[key])

    def removeQuote(self, str):
        if str.startswith('\'') and str.endswith('\''):
            str=str.strip('\'')
        elif str.startswith('"') and str.endswith('"'):
            str=str.strip('"')
        return str

    def getExpected(self):
        path = os.path.join(self.dir_ec, self.cluster)
        logging.debug("%3d. getExpected() checks path:'%s' ",  self.taskId,  path )
        if os.path.isdir(path):
            # we have cluster specific key dir, check keyfile
            path = os.path.join(path, self.xml_e)
            if not os.path.isfile(path):
                # we haven't keyfile use the common
                logging.debug("%3d. getExpected() cluster specific keyfile does not exist:'%s' ",  self.taskId,  path )
                path =  os.path.join(self.dir_ex, self.xml_e)
        else:
            # we have not cluster specific key dir use common dir and file
            path =  os.path.join(self.dir_ex, self.xml_e)

        logging.debug("%3d. getExpected() returns with path:'%s'",  self.taskId,  path )
        return path

    def getResults(self):
        return os.path.join(self.dir_r, self.xml_r)

    def getArchive(self):
        return os.path.join(self.dir_a, self.xml_a)

    def getEcl(self):
        return os.path.join(self.dir_ec, self.ecl)

    def getBaseEcl(self):
        return self.ecl

    def getBaseEclName(self):
        return self.basename

    def getWuid(self):
        return self.wuid

    def addResults(self, results, wuid):
        filename = self.getResults()
        self.wuid = wuid
        if not os.path.isdir(self.dir_r):
            os.mkdir(self.dir_r)
        if os.path.isfile(filename):
            os.unlink(filename)
        FILE = open(filename, "w")
        FILE.write(results)
        FILE.close()

    def __checkSkip(self, skipText, skip):
        skipText = skipText.lower()
        skip = skip.lower()
        eclText = open(self.getEcl(), 'r')
        skipLines = []
        for line in eclText:
            line = line.lower()
            if skipText in line:
                skipLines.append(line.rstrip('\n'))
        if len(skipLines) > 0:
            for skipLine in skipLines:
                skipParts = skipLine.split()
                skipType = skipParts[1]
                skipReason = None
                if len(skipParts) == 3:
                    skipReason = skipParts[2]
                if "==" in skipType:
                    skipType = skipType.split("==")[1]
                if not skip:
                    return {'reason': skipReason, 'type': skipType}
                if skip in skipType:
                    return {'skip': True, 'type' : skipType, 'reason': skipReason}
        return {'skip': False}

    def __checkTag(self,  tag):
        tag = tag.lower()
        logging.debug("%3d.__checkTag (ecl:'%s', tag:'%s')", self.taskId, self.ecl, tag)
        retVal = False
        eclText = open(self.getEcl(), 'rb')
        for line in eclText:
            if tag in line.lower():
                retVal = True
                break
        logging.debug("%3d.__checkTag() returns with %s", self.taskId,  retVal)
        return retVal

    def testSkip(self, skip=None):
        return self.__checkSkip("//skip", skip)

    def testVarSkip(self, skip=None):
        return self.__checkSkip("//varskip", skip)

    def testExclusion(self, target):
        # Standard string has a problem with unicode characters
        # use byte arrays and binary file open instead
        tag = b'//no' + target.encode()
        logging.debug("%3d. testExclusion (ecl:'%s', target: '%s', tag:'%s')", self.taskId, self.ecl, target, tag)
        retVal = self.__checkTag(tag)
        logging.debug("%3d. testExclude() returns with: %s", self.taskId,  retVal)
        return retVal

    def testPublish(self):
        # Standard string has a problem with unicode characters
        # use byte arrays and binary file open instead
        tag = b'//publish'
        logging.debug("%3d. testPublish (ecl:'%s', tag:'%s')", self.taskId, self.ecl,  tag)
        retVal = self.__checkTag(tag)
        logging.debug("%3d. testPublish() returns with: %s",  self.taskId,  retVal)
        return retVal

    def testNoKey(self):
        # Standard string has a problem with unicode characters
        # use byte arrays and binary file open instead
        tag = b'//nokey'
        logging.debug("%3d. testNoKey (ecl:'%s', tag:'%s')", self.taskId, self.ecl,  tag)
        retVal = self.__checkTag(tag)
        logging.debug("%3d. testNoKey() returns with: %s",  self.taskId,  retVal)
        return retVal

    def testNoOutput(self):
        # Standard string has a problem with unicode characters
        # use byte arrays and binary file open instead
        tag = b'//nooutput'
        logging.debug("%3d. testNoOutput (ecl:'%s', tag:'%s')", self.taskId, self.ecl,  tag)
        retVal = self.__checkTag(tag)
        logging.debug("%3d. testNoOutput() returns with: %s",  self.taskId,  retVal)
        return retVal

    def getTimeout(self):
        timeout = 0
        # Standard string has a problem with unicode characters
        # use byte arrays and binary file open instead
        tag = b'//timeout'
        logging.debug("%3d. getTimeout (ecl:'%s', tag:'%s')", self.taskId,  self.ecl, tag)
        eclText = open(self.getEcl(), 'rb')
        for line in eclText:
            if tag in line:
                timeoutParts = line.split()
                if len(timeoutParts) == 2:
                    if (timeoutParts[1] == '-1') or isPositiveIntNum(timeoutParts[1]) :
                        timeout = int(timeoutParts[1])
                break
        logging.debug("%3d. Timeout is :%d sec",  self.taskId,  timeout)
        return timeout

    def testResults(self):
        d = difflib.Differ()
        try:
            expectedKeyPath = self.getExpected()
            logging.debug("%3d. EXP: " + expectedKeyPath,  self.taskId )
            logging.debug("%3d. REC: " + self.getResults(),  self.taskId )
            if not os.path.isfile(expectedKeyPath):
                self.diff += "KEY FILE NOT FOUND. " + expectedKeyPath
                raise IOError("KEY FILE NOT FOUND. " + expectedKeyPath)
            if not os.path.isfile(self.getResults()):
                self.diff += "RESULT FILE NOT FOUND. " + self.getResults()
                raise IOError("RESULT FILE NOT FOUND. " + self.getResults())
            expected = open(expectedKeyPath, 'r').readlines()
            recieved = open(self.getResults(), 'r').readlines()
            for line in difflib.unified_diff(expected,
                                             recieved,
                                             fromfile=self.xml_e,
                                             tofile=self.xml_r):
                self.diff += line
        except Exception as e:
            logging.debug( e, extra={'taskId':self.taskId})
            logging.debug("%s",  traceback.format_exc().replace("\n","\n\t\t"),  extra={'taskId':self.taskId} )
            logging.debug("EXP: %s",  self.getExpected(),  extra={'taskId':self.taskId})
            logging.debug("REC: %s",  self.getResults(),  extra={'taskId':self.taskId})
            return False
        finally:
            if not self.diff:
                return True
            return False

    def setElapsTime(self,  time):
        self.elapsTime = time

    def setJobname(self,  timestamp):
        self.jobname = self.basename +"-"+timestamp

    def getJobname(self):
        return self.jobname

    def setAborReason(self,  reason):
        self.abortReason = reason

    def getAbortReason(self):
        return self.abortReason

    def setTaskId(self, taskId):
        self.taskId = taskId

    def getTaskId(self):
        return self.taskId

    def setIgnoreResult(self,  ignoreResult):
        self.ignoreResult=ignoreResult

    def getIgnoreResult(self):
        logging.debug("%3d. getIgnoreResult (ecl:'%s', ignore result is:'%s')", self.taskId,  self.ecl, self.ignoreResult)
        return self.ignoreResult

    def getStoredInputParameters(self):
        return self.optX

    def getFParameters(self):
        return self.optF

