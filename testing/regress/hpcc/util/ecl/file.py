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

import difflib
import logging
import os
import traceback
import re
import xml.etree.ElementTree as ET
import unicodedata

from ...util.util import isPositiveIntNum, getConfig, PrintException
from ...common.error import Error

logger = logging.getLogger('RegressionTestEngine')

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
    jobnameVersion = ''
    aborted = False
    abortReason = ''
    taskId = -1
    ignoreResult=False

    def __del__(self):
        logger.debug("%3d. File destructor (file:%s).", self.taskId, self.ecl )

    def close(self):
        if self.tempFile:
            self.tempFile.close()
            pass

    def __init__(self, ecl, dir_a, dir_ex, dir_r, dir_inc, cluster, args):
        logger.debug("%3d. ECLFile(ecl: '%s', cluster: '%s').", self.taskId, ecl,  cluster)
        self.dir_ec = os.path.dirname(ecl)
        self.dir_ex = dir_ex
        self.dir_r = dir_r
        self.dir_a = dir_a
        self.dir_inc = dir_inc
        self.cluster = cluster;
        self.cluster = args.cluster;
        self.engine = args.engine;
        self.baseEcl = os.path.basename(ecl)
        self.basename = os.path.splitext(self.baseEcl)[0]
        self.baseXml = self.basename + '.xml'
        self.baseQueryXml = self.basename+'.queryxml'
        self.ecl = self.baseEcl
        self.xml_e = self.baseXml
        self.xml_r = self.baseXml
        self.xml_a = 'archive_' + self.cluster + '_' + self.baseXml
        self.jobname = self.basename
        self.diff = ''
        self.aborted = False
        self.abortReason =''
        self.tags={}
        self.tempFile=None
        self.paramD=[]
        self.isVersions=False
        self.version=''
        self.versionId=0
        self.timeout = self.checkFileTimeout(int(args.timeout))
        self.args = args
        self.eclccWarning = ''
        self.eclccWarningChanges = ''
        # Replace all '-' and ' ' with '_' to keep the whole suffix as one part
        self.jobNameSuffix = args.jobnamesuffix.replace('-',  '_').replace(' ','_')

        self.isFlushDiskCache = False
        if self.args.flushDiskCache:
            self.isFlushDiskCache = True

        #If there is a --publish CL parameter then force publish this ECL file
        self.forcePublish=False
        if 'publish' in self.args:
            self.forcePublish=self.args.publish

        self.optX =[]

        # The final set of stored variables are the union of queryxml, config and CLI
        # The values in the queryxml file is the lowest precedence
        # the relevant value in config file Params array it the middle and
        # -X in the CLI is the highest.
        self.optXHash=self.checkQueryxmlFile()

        self.config = getConfig()
        try:
            # Process definitions of stored input value(s) from config
            for param in self.config.Params:
                [testSpec,  val] = param.split(':')

                if '*' in testSpec:
                    testSpec = testSpec.replace('*',  '\w+')

                testSpec = testSpec.replace('.',  '\.')
                match = re.match(testSpec,  self.baseEcl)
                if match:
                    optXs = ("-X"+val.replace(',',  ',-X')).split(',')
                    self.processKeyValPairs(optXs,  self.optXHash)
                pass
        except AttributeError:
            # It seems there is no Params array in the config file
            pass

        # Process setupExtraX parameters if any
        if 'setupExtraX' in args:
            args.setupExtraX.append('destClusterName=' + self.config.ClusterNames[args.engine])
            for extraX in args.setupExtraX:
                extraX=self.removeQuote(extraX)
                optXs = ("-X"+extraX.replace(',',  ',-X')).split(',')
                self.processKeyValPairs(optXs,  self.optXHash)
            pass

        # Process -X CLI parameters
        if args.X != 'None':
            args.X[0]=self.removeQuote(args.X[0])
            optXs = ("-X"+args.X[0].replace(',',  ',-X')).split(',')
            self.processKeyValPairs(optXs,  self.optXHash)
            pass

        self.mergeHashToStrArray(self.optXHash,  self.optX)

        # Process setupExtraD parameters if any
        self.optD =[]
        self.optDHash={}
        if 'setupExtraD' in args:
            for extraDName, extraDVal in args.setupExtraD.items():
                extraDVal = extraDVal.strip()
                optDs = ["-D"+extraDName+"='"+extraDVal+"'"]
                self.processKeyValPairs(optDs,  self.optDHash)
            pass

        self.mergeHashToStrArray(self.optDHash,  self.optD)
        pass

        self.optF =[]
        self.optFHash={}
        # -f parameters from config
        try:
            for param in self.config.engineParams:
                paramf=self.removeQuote(param)
                optFs = ("-f"+paramf.replace(',',  ',-f')).split(',')
                self.processKeyValPairs(optFs,  self.optFHash)
                pass
        except AttributeError:
            # It seems there is no Params array in the config file
            pass

        #process -f CLI parameters (multiple -f enabled)
        if args.f != None:
            for argf in args.f:
                argf=self.removeQuote(argf)
                optFs = ("-f"+argf.replace(',',  ',-f')).split(',')
                self.processKeyValPairs(optFs,  self.optFHash)
            pass

        self.mergeHashToStrArray(self.optFHash,  self.optF)
        pass

    def checkQueryxmlFile(self):
        retHash = {}
        path = os.path.join(self.dir_ec, self.baseQueryXml)
        logger.debug("%3d. checkQueryxmlFile() checks path:'%s' ",  self.taskId,  path )
        if os.path.isfile(path):
            # we have defaults for stored variables in xml file.
            tree = ET.parse(path)
            root = tree.getroot()
            for child in root:
                key = '-X'+child.tag
                if child.text != None:
                    val = child.text
                else:
                    val =''
                retHash[key] = val
            pass
        logger.debug("%3d. checkQueryxmlFile() returns with %s stored parameter(s) ",  self.taskId,  len(retHash) )
        return retHash

    def processKeyValPairs(self,  optArr,  optHash):
        for optStr in optArr:
            if '=' in optStr:
                # Has value
                [key,  val] = optStr.split('=')
            else:
                # Hasn't value
                [key,  val] = [optStr, '']
            key = key.strip().replace(' ', '')  # strip spaces around and inside the key
            val = val.strip()                               # strip spaces around the val
            if ' ' in val:
                val = '"' + val + '"'
            optHash[key] = val

    def mergeHashToStrArray(self, optHash,  strArray):
        # Merge all parameters into a string array
        for key in optHash:
            if '' == optHash[key]:
                # Hasn't value
                strArray.append(key)
            else:
                strArray.append(key+'='+optHash[key])

    def removeQuote(self, str):
        if str.startswith('\'') and str.endswith('\''):
            str=str.strip('\'')
        elif str.startswith('"') and str.endswith('"'):
            str=str.strip('"')
        return str

    def getExpected(self):
        path = os.path.join(self.dir_ec, self.engine)
        logger.debug("%3d. getExpected() checks path:'%s' ",  self.taskId,  path )
        if os.path.isdir(path):
            # we have cluster specific key dir, check keyfile
            path = os.path.join(path, self.xml_e)
            if not os.path.isfile(path):
                # we haven't keyfile use the common
                logger.debug("%3d. getExpected() cluster specific keyfile does not exist:'%s' ",  self.taskId,  path )
                path =  os.path.join(self.dir_ex, self.xml_e)
        else:
            # we have not cluster specific key dir use common dir and file
            path =  os.path.join(self.dir_ex, self.xml_e)

        logger.debug("%3d. getExpected() returns with path:'%s'",  self.taskId,  path )
        return path

    def getResults(self):
        return os.path.join(self.dir_r, self.getJobname() + '.xml')

    def getArchiveName(self):
        #print("ECLFile.getArchiveName()")
        logger.debug("%3d. getArchiveName(isVersions:'%s')", self.taskId, self.isVersions )
        if self.isVersions:
            dynamicFilename='archive_' + self.cluster + '_' + self.basename
            dynamicFilename+= '_v'+ str(self.versionId)
            dynamicFilename += '.xml'
            return os.path.join(self.dir_a, dynamicFilename)
        else:
            return os.path.join(self.dir_a, self.xml_a)

    def getEcl(self):
        #print("ECLFile.getEcl()")
        return os.path.join(self.dir_ec, self.ecl)

    def getRealEclSource(self):
        logger.debug("%3d. getRealEclSource (isVersions:'%s')", self.taskId, self.isVersions )
        logger.debug("%3d. return with '%s'", self.taskId, self.ecl)
        return os.path.join(self.dir_ec, self.ecl)

    def getBaseEcl(self):
        return self.ecl

    def getBaseEclName(self):
        return self.basename

    def getBaseEclRealName(self):
        logger.debug("%3d. getBaseEclRealName (isVersions:'%s')", self.taskId, self.isVersions)
        if self.isVersions:
            realName = self.basename + '.ecl ( version: ' + self.getVersion()  + ' )'
        else:
            realName = self.getBaseEcl()
        logger.debug("%3d.return with:'%s'", self.taskId, realName)
        return realName

    def getWuid(self):
        return self.wuid.strip()

    def setWuid(self,  wuid):
        self.wuid = wuid.strip()

    def addResults(self, results, wuid):
        logger.debug("%3d. addResults (results:'%s', wuid:'%s')", self.taskId, results,  wuid)
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
        logger.debug("%3d.__checkSkip (skipText:'%s', skip:'%s')", self.taskId, skipText, skip)
        skipText = skipText.lower()
        skip = skip.lower()
        eclText = open(self.getEcl(), 'rb')
        skipLines = []
        lineNo=0
        for line in eclText:
            lineNo += 1
            try:
                line = line.decode('utf-8').lower()
            except:
                line = str(line.lower())
            if skipText in line:
                skipLines.append({'line': line.rstrip('\n'),  'lineNo' : lineNo})
        if len(skipLines) > 0:
            try:
                for skipLine in skipLines:
                    skipParts = skipLine['line'].split()
                    skipType = skipParts[1]
                    skipReason = None

                    if len(skipParts) == 3:
                        skipReason = skipParts[2]
                    splitChar = '='

                    if "==" in skipType:
                        splitChar='=='
                    skipType = skipType.split(splitChar)[1]

                    if not skip:
                        return {'reason': skipReason, 'type': skipType}

                    if skip == skipType:
                        return {'skip': True, 'type' : skipType, 'reason': skipReason}

                    if skip == 'thor' and 'thorlcr' == skipType:
                        return {'skip': True, 'type' : skipType, 'reason': skipReason}

            except Exception as e:
                logger.debug( e, extra={'taskId':self.taskId})
                logger.debug("%s",  traceback.format_exc().replace("\n","\n\t\t"),  extra={'taskId':self.taskId} )
                raise Error("6005", err='file: %s:%d\n text: \"%s\"' % (self.getEcl(), skipLine['lineNo'], skipLine['line']))

        return {'skip': False}

    def __checkTag(self,  tag):
        tag = tag.decode("utf-8").lower()
        logger.debug("%3d.__checkTag (ecl:'%s', tag:'%s')", self.taskId, self.ecl, tag)
        retVal = False
        eclText = open(self.getEcl(), 'rb')
        for line in eclText:
            _line = str(line)
            if re.search(tag+'[^\w]', _line, re.IGNORECASE):
                retVal = True
                break
        logger.debug("%3d.__checkTag() returns with %s", self.taskId,  retVal)
        return retVal

    def testSkip(self, skip=None):
        return self.__checkSkip("//skip", skip)

    def testVarSkip(self, skip=None):
        return self.__checkSkip("//varskip", skip)

    def testExclusion(self, target):
        # Standard string has a problem with unicode characters
        # use byte arrays and binary file open instead
        tag = b'//no' + target.encode()
        logger.debug("%3d. testExclusion (ecl:'%s', target: '%s', tag:'%s')", self.taskId, self.ecl, target, tag)
        retVal = self.__checkTag(tag)
        logger.debug("%3d. testExclude() returns with: %s", self.taskId,  retVal)
        return retVal

    def testPublish(self):
        if self.forcePublish:
            retVal=True
        else:
            # Standard string has a problem with unicode characters
            # use byte arrays and binary file open instead
            tag = b'//publish'
            logger.debug("%3d. testPublish (ecl:'%s', tag:'%s')", self.taskId, self.ecl,  tag)
            retVal = self.__checkTag(tag)
        logger.debug("%3d. testPublish() returns with: %s",  self.taskId,  retVal)
        return retVal

    def testNoKey(self):
        # Standard string has a problem with unicode characters
        # use byte arrays and binary file open instead
        tag = b'//nokey'
        logger.debug("%3d. testNoKey (ecl:'%s', tag:'%s')", self.taskId, self.ecl,  tag)
        retVal = self.__checkTag(tag)
        logger.debug("%3d. testNoKey() returns with: %s",  self.taskId,  retVal)
        return retVal

    def testNoOutput(self):
        # Standard string has a problem with unicode characters
        # use byte arrays and binary file open instead
        tag = b'//nooutput'
        logger.debug("%3d. testNoOutput (ecl:'%s', tag:'%s')", self.taskId, self.ecl,  tag)
        retVal = self.__checkTag(tag)
        logger.debug("%3d. testNoOutput() returns with: %s",  self.taskId,  retVal)
        return retVal

    def testInClass(self,  classDefined):
        retVal=False
        for c in classDefined:
            tag = b'//class='+c.encode()
            logger.debug("%3d. testInClass (ecl:'%s', tag:'%s')", self.taskId, self.ecl,  tag)
            retVal = self.__checkTag(tag)
            if retVal:
                break
        logger.debug("%3d. testInClass() returns with: %s",  self.taskId,  retVal)
        return retVal

    def testFail(self):
        # Standard string has a problem with unicode characters
        # use byte arrays and binary file open instead
        tag = b'//fail'
        logger.debug("%3d. testFail(ecl:'%s', tag:'%s')", self.taskId, self.ecl,  tag)
        retVal = self.__checkTag(tag)
        logger.debug("%3d. testFail() returns with: %s",  self.taskId,  retVal)
        return retVal

    # Test (and read all) //version tag in the ECL file
    def testVesion(self):
        if self.isVersions == False and not self.args.noversion:
            tag = '//version'
            logger.debug("%3d. testVesion (ecl:'%s', tag:'%s')", self.taskId, self.ecl, tag)
            self.versions = []
            eclText = open(self.getEcl(), 'rb')
            for line in eclText:
                try:
                    _line = line.decode("utf-8") # str(line)
                except UnicodeDecodeError:
                    _line = str(line)
                    
                if tag in _line.lower():
                    items = _line.replace(tag, '').strip().replace('"', '')
                    if '=' in items:
                        self.versions.append(items)
                        self.isVersions = True
                        pass
        logger.debug("%3d. testVesion() returns with isVersions = '%s'", self.taskId,  self.isVersions)
        return self.isVersions

    # Return an array of all //version tag from the original ECL file
    def getVersions(self):
        return self.versions

    # Return the //version tag parameters from the generated ECL
    def getVersion(self):
        return self.version

    def setVersionId(self,  id):
        self.versionId=id

    def checkFileTimeout(self,  timeout):
        timeout = timeout
        # Standard string has a problem with unicode characters
        # use byte arrays and binary file open instead
        tag = '//timeout'
        logger.debug("%3d. checkFileTimeout (ecl:'%s', tag:'%s')", self.taskId,  self.ecl, tag)
        eclText = open(self.getEcl(), 'rb')
        for line in eclText:
            try:
                line = line.decode("utf-8")
            except UnicodeDecodeError:
                line = str(line)
            if tag in line:
                timeoutParts = line.split()
                if len(timeoutParts) == 2:
                    if (timeoutParts[1] == '-1') or isPositiveIntNum(timeoutParts[1]) :
                        timeout = int(timeoutParts[1])
                break
        logger.debug("%3d. Timeout is :%d sec",  self.taskId,  timeout)
        return timeout

    def getTimeout(self):
        return self.timeout

    def setTimeout(self,  timeout):
        self.timeout = timeout

    def testResults(self):
        try:
            expectedKeyPath = self.getExpected()
            logger.debug("%3d. EXP: " + expectedKeyPath,  self.taskId )
            logger.debug("%3d. REC: " + self.getResults(),  self.taskId )
            if not os.path.isfile(expectedKeyPath):
                self.diff += ("%3d. Test: %s\n") % (self.taskId,  self.getBaseEclRealName())
                self.diff += "KEY FILE NOT FOUND. " + expectedKeyPath
                raise IOError("KEY FILE NOT FOUND. " + expectedKeyPath)
            if not os.path.isfile(self.getResults()):
                self.diff += ("%3d. Test: %s\n") % (self.taskId,  self.getBaseEclRealName())
                self.diff += "RESULT FILE NOT FOUND. " + self.getResults()
                raise IOError("RESULT FILE NOT FOUND. " + self.getResults())
            expected = open(expectedKeyPath, 'r').readlines()
            logger.debug("%3d. expected: " + repr(expected),  self.taskId )
            recieved = open(self.getResults(), 'r').readlines()
            logger.debug("%3d. recieved: " + repr(recieved),  self.taskId )
            diffLines = ''
            lineIndex = 1
            for line in difflib.unified_diff(expected, recieved, fromfile=self.xml_e, tofile=self.xml_r):
                diffLines += line
                logger.debug("%3d. Line" + str(lineIndex) +":" + line,  self.taskId )
                lineIndex += 1

            logger.debug("%3d. diffLines: " + diffLines,  self.taskId )
            if len(diffLines) > 0:
                self.diff += ("%3d. Test: %s\n") % (self.taskId,  self.getBaseEclRealName())
                logger.debug( "type(diffLines) is %s: ",  repr(type(diffLines)), extra={'taskId':self.taskId})
                try:
                    if type(diffLines) == type(' '):
                        #diffLines = unicodedata.normalize('NFKD', diffLines).encode('ascii','ignore').replace('\'','').replace('\\u', '\\\\u')
                        diffLines = diffLines.replace('\'','').replace('\\u', '\\\\u')
                        diffLines = repr(diffLines)
                    else:
                        diffLines = repr(diffLines)
                except Exception as e:
                    PrintException(repr(e) + " runQuery() end")
                    
                    
                self.diff += str(diffLines)
            logger.debug("%3d. self.diff: '" + self.diff +"'",  self.taskId )
        except Exception as e:
            logger.debug( e, extra={'taskId':self.taskId})
            logger.debug("%s",  traceback.format_exc().replace("\n","\n\t\t"),  extra={'taskId':self.taskId} )
            logger.debug("EXP: %s",  self.getExpected(),  extra={'taskId':self.taskId})
            logger.debug("REC: %s",  self.getResults(),  extra={'taskId':self.taskId})
            return False
        finally:
            if not self.diff:
                return True
            return False

    def setElapsTime(self,  time):
        self.elapsTime = time

    def setJobnameVersion(self,  version):
        # Overrides the global flushDiskCache parameter if --pq <= 1
        # There is no sense to clear disk cache if same test running parallel by versioning
        if ('flushDiskCache=true' in version) and (self.args.pq in (0, 1)):
            self.isFlushDiskCache = True
        if 'flushDiskCache=false' in version:
            self.isFlushDiskCache = False

        # convert this kind of version string
        #  'multiPart=false,useSequential=true'
        # to this
        #   'multiPart(false)-useSequential(true)'
        self.jobnameVersion += '-' +version.replace('=', '(').replace(',', ')-')+')'
        pass
        
    def setJobname(self,  timestamp):
        # jobname = <basename>[-<version>][-#<suffix>]-<timestamp>
        self.jobname = self.basename + self.jobnameVersion
        if len(self.jobNameSuffix) > 0:
            pos = 0
            if self.jobNameSuffix[pos] == '-':
                pos = 1
            self.jobname += '-#' + self.jobNameSuffix[pos:]

        if len(timestamp) > 0:
            self.jobname += "-" + timestamp

    def getJobname(self):
        return self.jobname

    def setAborReason(self,  reason):
        self.abortReason = reason
        self.aborted = True

    def isAborted(self):
        return self.aborted

    def getAbortReason(self):
        return self.abortReason

    def setTaskId(self, taskId):
        self.taskId = taskId

    def getTaskId(self):
        return self.taskId

    def setIgnoreResult(self,  ignoreResult):
        self.ignoreResult=ignoreResult

    def getIgnoreResult(self):
        logger.debug("%3d. getIgnoreResult (ecl:'%s', ignore result is:'%s')", self.taskId,  self.ecl, self.ignoreResult)
        return self.ignoreResult

    def getStoredInputParameters(self):
        logger.debug("%3d. getStoredInputParameters (ecl:'%s', X parameters are:'%s')", self.taskId,  self.ecl, self.optX)
        return self.optX

    def getFParameters(self):
        return self.optF

    def getExtraDParameters(self):
        return self.optD

    # Set -D parameter(s) (and generate version string for logger)
    def setDParameters(self,  param):
        self.setJobnameVersion(param)
        self.version = param.replace(',',  ', ')
        param = '-D'+param.replace(',', ' -D')+''
        self.paramD = param.split(' ')
        self.isVersions = True

    # Return the -D parameters
    def getDParameters(self):
        logger.debug("%3d. getDParameters (ecl:'%s', D parameters are:'%s')", self.taskId,  self.ecl, self.paramD)
        return self.paramD

    def setEclccWarning(self,  warning):
        logger.debug("%3d. setEclccWarning (ecl:'%s', warning(s) is:'%s')", self.taskId,  self.ecl, warning)
        self.eclccWarning = warning.strip().replace(self.dir_ec+'/','').split('\n')

    def getEclccWarning(self):
        logger.debug("%3d. getEclccWarning (ecl:'%s', warning(s) is:'%s')", self.taskId,  self.ecl, self.eclccWarning)
        return self.eclccWarning

    def isEclccWarningChanged(self):
        retVal = False
        expectedKeyPath = self.getExpected()
        expectedKeyPath = expectedKeyPath.replace('.xml', '.eclccwarn')
        logger.debug("%3d. EXP: " + expectedKeyPath,  self.taskId )
        eclccKeyContent = []
        if os.path.isfile(expectedKeyPath):
            # put warning file content into self.eclccWarningChanges
            eclccKeyContent = open(expectedKeyPath, 'r').readlines()
            eclccKeyContent = [x.strip() for x in eclccKeyContent]
            logger.debug("%3d. eclccKeyContent: " + "\n".join(eclccKeyContent),  self.taskId )
        elif '' != self.eclccWarning:
            logger.debug("%3d. Eclcc warning file '%s' doesn't exist." % (self.taskId, expectedKeyPath))
            eclccKeyContent = []
            if self.args.handleEclccWarningFile:
                logger.debug("%3d. Create '%s' eclcc warning file." %(self.taskId, expectedKeyPath ))
                open(expectedKeyPath, 'w').write("\n".join(self.eclccWarning))
                pass
        try:
            logger.debug("%3d.  self.eclccWarning: " +  self.eclccWarning,  self.taskId )
            diffLines = ''
            d = list(difflib.unified_diff(eclccKeyContent, self.eclccWarning, fromfile=expectedKeyPath, tofile="eclcc warning",  lineterm = ""))
            diffLines = "\n".join(d)

            logger.debug("%3d. diffLines: " + diffLines,  self.taskId )
            if len(diffLines) > 0:
                self.eclccWarningChanges += ("%3d. Test: %s\n") % (self.taskId,  self.getBaseEclRealName())
                self.eclccWarningChanges += "\tEclcc generated warning changed\n"
                logger.debug( "type(diffLines) is %s: ",  repr(type(diffLines)), extra={'taskId':self.taskId})
                if type(diffLines) == type(' '):
                    diffLines = unicodedata.normalize('NFKD', diffLines).encode('ascii','ignore') #.replace('\'','').replace('\\u', '\\\\u')
                    diffLines = str(diffLines).replace('\'','').replace('\\u', '\\\\u')
                else:
                    diffLines = str(diffLines)
                self.eclccWarningChanges += str(diffLines)
                retVal = True
                if self.args.handleEclccWarningFile:
                    if 0 < len(self.eclccWarning):
                        logger.debug("%3d. Overwrite '%s' with current eclcc warning!" %(self.taskId, expectedKeyPath ))
                        open(expectedKeyPath, 'w').write("\n".join(self.eclccWarning))
                    else:
                        logger.debug("%3d. Remove '%s' with current eclcc warning!" %(self.taskId, expectedKeyPath ))
                        os.unlink(expectedKeyPath)
                    pass
            logger.debug("%3d. self.diff: '" + self.eclccWarningChanges +"'",  self.taskId )
        except Exception as e:
            logger.debug( e, extra={'taskId':self.taskId})
            logger.debug("%s",  traceback.format_exc().replace("\n","\n\t\t"),  extra={'taskId':self.taskId} )
            logger.debug("EXP: %s",  eclccKeyContent,  extra={'taskId':self.taskId})
            logger.debug("REC: %s",  self.eclccWarning,  extra={'taskId':self.taskId})
            retVal = True
        finally:
            if self.eclccWarningChanges == '':
                retVal = False
            else:
                retVal = True

        return retVal

    def getEclccWarningChanges(self):
        # return with self.eclccWarningChanges
        return self.eclccWarningChanges+"\n"

    def flushDiskCache(self):
        logger.debug("%3d. isFlushDiskCache (ecl:'%s'): '%s')" % (self.taskId,  self.ecl, str(self.isFlushDiskCache)))
        return self.isFlushDiskCache

    def setFlushDiskCache(self, state):
        logger.debug("%3d. setFlushDiskCache (ecl:'%s'): '%s')" % (self.taskId,  self.ecl, str(state)))
        self.isFlushDiskCache = state

    def appendJobNameSuffix(self,  string):
        self.jobNameSuffix += '_' + string.replace('-',  '_');
