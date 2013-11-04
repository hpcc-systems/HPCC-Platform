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
from ..util.ecl.file import ECLFile
from ..common.error import Error
from ..util.exclusion import Exclusion


class Suite:
    def __init__(self, name, dir_ec, dir_a, dir_ex, dir_r):
        self.name = name
        self.suite = []
        self.dir_ec = dir_ec
        self.dir_a = dir_a
        self.dir_ex = dir_ex
        self.dir_r = dir_r
        self.exclusion = Exclusion()
        self.buildSuite()
        

    def buildSuite(self):
        if not os.path.isdir(self.dir_ec):
            raise Error("2001", err="Not Found: %s" % self.dir_ec)
        for files in os.listdir(self.dir_ec):
            if files.endswith(".ecl"):
                ecl = os.path.join(self.dir_ec, files)
                eclfile = ECLFile(ecl, self.dir_a, self.dir_ex,
                                  self.dir_r)
                #if not eclfile.testSkip(self.name)['skip']:
                #    self.suite.append(eclfile)
                #print "self.name:", self.name, ", files:", files
                if not self.exclusion.checkException(self.name, files):
                    self.suite.append(eclfile)

        self.suite.reverse()

    def getSuite(self):
        return self.suite
