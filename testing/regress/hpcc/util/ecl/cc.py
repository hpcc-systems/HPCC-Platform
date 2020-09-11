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
import logging

from ...common.error import Error
from ...common.shell import Shell
from ...util.util import PrintException

logger = logging.getLogger('RegressionTestEngine')

class ECLCC(Shell):
    def __init__(self):
        super().__init__()
        self.defaults = []
        self.cmd = 'eclcc'
        self.makeArchiveError=''

    def __ECLCC(self):
        return self.command(self.cmd, *self.defaults)

    def getArchive(self, ecl):
        try:
                file = ecl.getEcl()
                return self.__ECLCC()('-E', file)
        except Error as err:
            logger.debug("getArchive exception:'%s'",  repr(err))
            self.makeArchiveError = str(err)
            return (repr(err), repr( err))

    def makeArchive(self, ecl):
        retVal = False
        try:
            self.defaults.append('--nostdinc')
            self.addIncludePath(ecl.dir_inc)
            dirname = ecl.dir_a
            filename = ecl.getArchiveName()

            if not os.path.isdir(dirname):
                os.mkdir(dirname)
            if os.path.isfile(filename):
                os.unlink(filename)
            result, stderr = self.getArchive(ecl)

            if result.startswith( 'Error()'):
                if ecl.testFail():
                    logger.debug("%3d. Fail is the expected result (ecl:'%s')", ecl.getTaskId(),  ecl.getBaseEclRealName())
                    retVal = True
                else:
                    retVal = False
                    ecl.diff += ("%3d. Test: %s\n") % (ecl.getTaskId(), ecl.getBaseEclRealName())
                    ecl.diff += '  eclcc returns with:\n\t'
                    try:
                        lines = repr(self.makeArchiveError).replace('\\n',  '\n\t').splitlines(True)
                        for line in lines:
                            lowerLine = line.lower()
                            if  (": error " in lowerLine) or (": warning " in lowerLine):
                                ecl.diff += line.replace("'",  "")
                            elif ("): error " in  line) or ("): warning " in lowerLine):
                                ecl.diff += line.replace("\\'", "'")
                            else:
                                ecl.diff += line
                    except Exception as ex:
                        logger.debug("Exception:'%s'",  str(ex))
                        ecl.diff += repr(self.makeArchiveError)
                    self.makeArchiveError=''
            else:
                logger.debug("%3d. makeArchive (result:'%s')", ecl.getTaskId(), result )
                logger.debug("%3d. makeArchive (stderr:'%s')", ecl.getTaskId(), stderr )
                if 'arning' in stderr:
                    ecl.setEclccWarning(stderr)
                logger.debug("%3d. makeArchive (filename:'%s')", ecl.getTaskId(), filename )
                FILE = open(filename, "w")
                FILE.write(str(result).replace("b'","").lstrip("'").rstrip("'"))
                FILE.close()
                retVal = True
        except Exception as e:
            PrintException(repr(e) + "ECLCC.makeArchive()")
            
        return retVal

    def setVerbose(self):
        self.defaults.append("--verbose")

    def addIncludePath(self, path):
        self.defaults.append('-I')
        self.defaults.append(path)

    def addLibraryPath(self, path):
        self.defaults.append('-L')
        self.defaults.append(path)

    def compile(self, eclfile, **kwargs):
        args = []

        if kwargs.pop('shared', False):
            args.append('-shared')

        out = kwargs.pop('out', False)
        if out:
            args.append('-o' + out)

        args.append(eclfile.getEcl())

        if not kwargs.pop('noCompile', False):
            try:
                self.__ECLCC()(*args)
            except Error as err:
                print((repr(err)))
        else:
            print(self.defaults, args)
