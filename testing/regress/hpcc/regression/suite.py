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

import os
import time
import glob
import copy
import logging
import traceback

from ..util.ecl.file import ECLFile
from ..common.error import Error
from ..util.util import getConfig

class Suite:
    def __init__(self, engine,  clusterName, dir_ec, dir_a, dir_ex, dir_r, logDir, dir_inc, args, isSetup=False,  fileList = None):
        self.clusterName = clusterName
        self.targetName = engine
        if isSetup:
            self.targetName = 'setup_'+engine

        self.args=args
        self.suite = []
        self.dir_ec = dir_ec
        self.dir_a = dir_a
        self.dir_ex = dir_ex
        self.dir_r = dir_r
        self.logDir = logDir
        self.dir_inc = dir_inc
        self.exclude = []
        self.publish = []

        self.cleanUp()

        self.buildSuite(args, isSetup, fileList)

        if len(self.exclude):
            curTime = time.strftime("%y-%m-%d-%H-%M-%S")
            logName = self.targetName + "-exclusion." + curTime + ".log"
            self.logName = os.path.join(self.logDir, logName)
            args.exclusionFile=self.logName
            self.log = open(self.logName, "w");
            for item in self.exclude:
                self.log.write(item+"\n")
            self.log.close();

    def __del__(self):
        print("Suite destructor.")
        pass

    def buildSuite(self, args, isSetup,  fileList):
        if fileList == None or len(fileList) == 0:
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

        exceptions =  ''
        for file in allfiles:
            if file.endswith(".ecl"):
                try:
                    ecl = os.path.join(self.dir_ec, file)
                    eclfile = ECLFile(ecl, self.dir_a, self.dir_ex,
                                      self.dir_r, self.dir_inc, self.clusterName,   args)
                    if isSetup:
                        skipResult = eclfile.testSkip('setup')
                    else:
                        skipResult = eclfile.testSkip(self.targetName)

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
                            exclude = eclfile.testExclusion(self.targetName)
                            exclusionReason=' ECL excluded'

                        if not exclude:
                            if ecl in args.excludeFileSet:
                                exclude = True
                                exclusionReason=' ECL excluded by --excludeFile parameter'

                        if not exclude:
                            self.addFileToSuite(eclfile)
                        else:
                            self.exclude.append(format(file, "30")+exclusionReason)
                    else:
                        self.exclude.append(format(file, "30")+" skipped (reason:"+skipResult['reason']+")");

                    if eclfile.testPublish():
                        self.publish.append(eclfile.getBaseEcl())
                except Error as e:
                    exceptions += str(e)
                except:
                    raise

        if exceptions != '':
            raise Error("6006", err="%s" % (exceptions))

        if self.args.runcount > 1:
            multipleInstances = []
            for ecl in self.suite:
                for instance in range(self.args.runcount):
                    try:
                        newEcl = copy.copy(ecl)
                    except:
                        logging.debug( e, extra={'taskId':-1})
                        logging.debug("%s",  traceback.format_exc().replace("\n","\n\t\t"),  extra={'taskId':-1} )
                        pass
                    newEcl.appendJobNameSuffix("rteloop%02d" % (instance+1))
                    if self.args.flushDiskCache:
                        # Apply flushDiskCachePolicy
                        if self.args.flushDiskCachePolicy == 1 and instance != 0:
                            # Clear flushDiskCache flag all futher instances
                            newEcl.setFlushDiskCache(False)

                        # If flushDiskCache is enabled then in the newEcl flushDiskCache
                        # is already enabled so not need to do any further action to clear cache every time

                    multipleInstances.append(newEcl)
            self.suite = multipleInstances
        pass

    def addFileToSuite(self, eclfile):
        haveVersions = eclfile.testVesion()
        if haveVersions and not self.args.noversion:
            basename = eclfile.getEcl()
            files=[]
            versions = eclfile.getVersions()
            versionId = 1
            for version in versions:
                if 'no'+self.targetName in version:
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
                                 self.dir_r,  self.dir_inc, self.clusterName, self.args)

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
        return self.targetName

    def cleanUp(self):
        # If there are some temporary files left, then remove them
        for file in glob.glob(self.dir_ec+'/_temp*.ecl'):
            os.unlink(file)

    def close(self):
        for ecl in self.suite:
            ecl.close()
        self.cleanUp()
