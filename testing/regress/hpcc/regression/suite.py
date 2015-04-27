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
import glob

from ..util.ecl.file import ECLFile
from ..common.error import Error
from ..util.util import checkClusters, getConfig

class Suite:
    def __init__(self, name, dir_ec, dir_a, dir_ex, dir_r, logDir, args, isSetup=False,  fileList = None):
        if isSetup:
            self.name = 'setup_'+name
        else:
            self.name = name
        self.args=args
        self.suite = []
        self.dir_ec = dir_ec
        self.dir_a = dir_a
        self.dir_ex = dir_ex
        self.dir_r = dir_r
        self.logDir = logDir
        self.exclude = []
        self.publish = []

        self.cleanUp()

        self.buildSuite(args, isSetup, fileList)

        if len(self.exclude):
            curTime = time.strftime("%y-%m-%d-%H-%M-%S")
            logName = self.name + "-exclusion." + curTime + ".log"
            self.logName = os.path.join(self.logDir, logName)
            args.exclusionFile=self.logName
            self.log = open(self.logName, "w");
            for item in self.exclude:
                self.log.write(item+"\n")
            self.log.close();

    def __del__(self):
        print "Suite destructor."
        pass

    def buildSuite(self, args, isSetup,  fileList):
        if fileList == None:
            if not os.path.isdir(self.dir_ec):
                raise Error("2001", err="Not Found: %s" % self.dir_ec)
            allfiles = os.listdir(self.dir_ec)
            allfiles.sort()
        else:
                allfiles = fileList

        classIncluded='all'
        if 'runclass' in args:
            classIncluded=args.runclass[0].split(',')
            pass

        classExcluded='none'
        if 'excludeclass' in args:
            classExcluded = args.excludeclass[0].split(',')
            pass

        for file in allfiles:
            if file.endswith(".ecl"):
                ecl = os.path.join(self.dir_ec, file)
                eclfile = ECLFile(ecl, self.dir_a, self.dir_ex,
                                  self.dir_r,  self.args.target,  args)
                if isSetup:
                    skipResult = eclfile.testSkip('setup')
                else:
                    skipResult = eclfile.testSkip(self.name)

                if not skipResult['skip']:
                    exclude=False
                    exclusionReason=''
                    if isSetup:
                        exclude = eclfile.testExclusion('setup')
                        exclusionReason=' setup'
                    elif ( 'all' not in  classIncluded ) or ('none' not in classExcluded):
                        included = True
                        if 'all' not in classIncluded:
                            included = eclfile.testInClass(classIncluded)
                        excluded = False
                        if 'none' not in classExcluded:
                            excluded = eclfile.testInClass(classExcluded)
                        exclude = (not included )  or excluded
                        exclusionReason=' class member excluded'
                    if not exclude:
                        exclude = eclfile.testExclusion(self.name)
                        exclusionReason=' ECL excluded'

                    if not exclude:
                        self.addFileToSuite(eclfile)
                    else:
                        self.exclude.append(format(file, "30")+exclusionReason)
                else:
                    self.exclude.append(format(file, "30")+" skipped (reason:"+skipResult['reason']+")");

                if eclfile.testPublish():
                    self.publish.append(eclfile.getBaseEcl())

    def addFileToSuite(self, eclfile):
        haveVersions = eclfile.testVesion()
        if haveVersions and not self.args.noversion:
            basename = eclfile.getEcl()
            files=[]
            versions = eclfile.getVersions()
            versionId = 1
            for version in versions:
                if 'no'+self.name in version:
                    # Exclude it from this target
                    pass
                else:
                    # Remove exclusion key(s) from version string
                    config = getConfig();
                    for cluster in config.Clusters:
                        version = version.replace(',no'+str(cluster), '')

                    files.append({'basename':basename, 'version':version,  'id':versionId })
                    versionId += 1
                pass
            pass

            # We have a list of  different versions
            # generate ECLs to suite
            for file in files:
                generatedEclFile = ECLFile(basename, self.dir_a, self.dir_ex,
                                 self.dir_r,  self.name, self.args)

                generatedEclFile.setDParameters(file['version'])
                generatedEclFile.setVersionId(file['id'])

                # add newly generated ECL to suite
                self.suite.append(generatedEclFile)

            # Clean-up, the original eclfile object not necessary anymore
            eclfile.close()
        else:
            self.suite.append(eclfile)
        pass

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

    def cleanUp(self):
        # If there are some temporary files left, then remove them
        for file in glob.glob(self.dir_ec+'/_temp*.ecl'):
            os.unlink(file)

    def close(self):
        for ecl in self.suite:
            ecl.close()
        self.cleanUp()
