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

    def __init__(self, ecl, dir_a, dir_ex, dir_r):
        self.dir_ec = os.path.dirname(ecl)
        self.dir_ex = dir_ex
        self.dir_r = dir_r
        self.dir_a = dir_a
        baseEcl = os.path.basename(ecl)
        baseXml = os.path.splitext(baseEcl)[0] + '.xml'
        self.ecl = baseEcl
        self.xml_e = baseXml
        self.xml_r = baseXml
        self.xml_a = 'archive_' + baseXml

    def getExpected(self):
        return os.path.join(self.dir_ex, self.xml_e)

    def getResults(self):
        return os.path.join(self.dir_r, self.xml_r)

    def getArchive(self):
        return os.path.join(self.dir_a, self.xml_a)

    def getEcl(self):
        return os.path.join(self.dir_ec, self.ecl)

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
        eclText = open(self.getEcl(), 'r')
        skipLines = []
        for line in eclText:
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

    def testSkip(self, skip=None):
        return self.__checkSkip("//skip", skip)

    def testVarSkip(self, skip=None):
        return self.__checkSkip("//varskip", skip)

    def testExclusion(self, target):
        # Standard string has a problem with unicode characters
        # use byte arrays and binary file open instead
        tag = b'//no' + target.encode()
        logging.debug("testExclusion (ecl:", self.ecl,", target:", target,", tag: ", tag, ")")
        eclText = open(self.getEcl(), 'rb')
        for line in eclText:
            if tag in line:
                return True
        return False

    def testPublish(self):
        # Standard strign has a problem with unicode characters
        # use byte arrays and binary file open instead
        tag = b'//publish'
        logging.debug("testPublish (ecl:", self.ecl,", tag: ", tag, ")")
        eclText = open(self.getEcl(), 'rb')
        for line in eclText:
            if tag in line:
                return True
        return False


    def testResults(self):
        d = difflib.Differ()
        try:
            logging.debug("EXP: " + self.getExpected())
            logging.debug("REC: " + self.getResults())
            if not os.path.isfile(self.getExpected()):
                raise IOError("KEY FILE NOT FOUND. " + self.getExpected())
            if not os.path.isfile(self.getResults()):
                raise IOError("RESULT FILE NOT FOUND. " + self.getResults())
            expected = open(self.getExpected(), 'r').readlines()
            recieved = open(self.getResults(), 'r').readlines()
            for line in difflib.unified_diff(recieved,
                                             expected,
                                             fromfile=self.xml_r,
                                             tofile=self.xml_e):
                self.diff += line
        except Exception as e:
            logging.critical(e)
            return False

        if not self.diff:
            return True
        return False
