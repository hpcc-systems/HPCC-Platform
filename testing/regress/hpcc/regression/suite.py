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

import os
import sys
import time

from ..util.ecl.file import ECLFile
from ..common.error import Error

class Suite:
    def __init__(self, name, dir_ec, dir_a, dir_ex, dir_r, logDir,  isSetup=False,  fileList = None):
        self.name = name
        self.suite = []
        self.dir_ec = dir_ec
        self.dir_a = dir_a
        self.dir_ex = dir_ex
        self.dir_r = dir_r
        self.logDir = logDir
        self.exclude = []
        self.publish = []

        self.buildSuite(isSetup,  fileList)

        if len(self.exclude):
            curTime = time.strftime("%y-%m-%d-%H-%M")
            logName = name + "-exclusion." + curTime + ".log"
            self.logName = os.path.join(self.logDir, logName)
            self.log = open(self.logName, "w");
            for item in self.exclude:
                self.log.write(item+"\n")
            self.log.close();

    def buildSuite(self,  isSetup,  fileList):
        if fileList == None:
            if not os.path.isdir(self.dir_ec):
                raise Error("2001", err="Not Found: %s" % self.dir_ec)
            allfiles = os.listdir(self.dir_ec)
            allfiles.sort()
        else:
                allfiles = fileList

        for file in allfiles:
            if file.endswith(".ecl"):
                ecl = os.path.join(self.dir_ec, file)
                eclfile = ECLFile(ecl, self.dir_a, self.dir_ex,
                                  self.dir_r,  self.name)
                if isSetup:
                    skipResult = eclfile.testSkip('setup')
                else:
                    skipResult = eclfile.testSkip(self.name)

                if not skipResult['skip']:
                    if isSetup:
                        exclude = eclfile.testExclusion('setup')
                    else:
                        exclude = eclfile.testExclusion(self.name)

                    if not exclude:
                        self.suite.append(eclfile)
                    else:
                        self.exclude.append(format(file, "25")+" excluded")
                else:
                    self.exclude.append(format(file, "25")+" skipped (reason:"+skipResult['reason']+")");

                if eclfile.testPublish():
                    self.publish.append(file)

    def testPublish(self, ecl):
        if ecl in self.publish:
            return True
        return False

    def getSuite(self):
        return self.suite

    def setStarTime(self,  time):
        self.startTime = time

    def setEndTime(self,  time):
        self.endTime=time

    def getElapsTime(self):
        return self.endTime-self.startTime

    def getSuiteName(self):
        return self.name
