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
from ...common.error import Error
from ...common.shell import Shell
from ...util.ecl.file import ECLFile


class ECLCC(Shell):
    def __init__(self):
        self.defaults = []
        self.cmd = self.which('eclcc')

    def __ECLCC(self):
        return self.command(self.cmd, *self.defaults)

    def getArchive(self, file):
        try:
            return self.__ECLCC()('-E', file)
        except Error as err:
            self.makeArchiveError = str(err)
            return repr(err)

    def makeArchive(self, ecl):
        self.addIncludePath(ecl.dir_ec)
        dirname = ecl.dir_a
        filename = ecl.getArchive()
        if not os.path.isdir(dirname):
            os.mkdir(dirname)
        if os.path.isfile(filename):
            os.unlink(filename)
        result = self.getArchive(ecl.getEcl())

        if result.startswith( 'Error()'):
           retVal = False
           ecl.diff += ecl.getEcl() + '\n\t'
           ecl.diff += self.makeArchiveError.replace('\n',  '\n\t')
           self.makeArchiveError=''
        else:
            FILE = open(filename, "w")
            FILE.write(result)
            FILE.close()
            retVal = True
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
                print(repr(err))
        else:
            print self.defaults, args
