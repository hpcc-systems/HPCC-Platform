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

from ..common.shell import Shell
from ..common.error import Error


class ConfigGen(Shell):
    def __init__(self, environment='/etc/HPCCSystems/environment.xml'):
        self.defaults = []
        self.defaults.append('-env')
        self.defaults.append(environment)
        self.cmd = 'configgen'

    def __ConfigGen(self):
        return self.command(self.cmd, *self.defaults)

    def section(self):
        return 'configgen'

    def listall(self):
        args = []
        args.append('-listall')
        try:
            results = '[' + self.section() + ']\n'
            data = self.__ConfigGen()(*args)
            data = [line for line in data.split('\n') if line.strip()]
            for i in data:
                val = i.replace(',,', ',None,')
                if val[-1:] == ',':
                    val += 'None'
                ret = val.split(',', 1)
                results += ret[0] + "=" + ret[1] + "\n"
            return results
        except Error as err:
            return repr(err)

    def list(self, ip='.'):
        args = []
        args.append('-ip')
        args.append(ip)
        args.append('-list')
        try:
            results = '[' + self.section() + ']\n'
            results += self.__ConfigGen()(*args)
            return results
        except Error as err:
            return repr(err)

    def listdirs(self):
        args = []
        args.append('-listdirs')
        try:
            results = '[' + self.section() + ']\n'
            data = self.__ConfigGen()(*args)
            data = [line for line in data.split('\n') if line.strip()]
            cnt = 0
            for i in data:
                results += "dir" + repr(cnt) + "=" + i
                cnt += 1
            return results
        except Error as err:
            return repr(err)

    def listcommondirs(self):
        args = []
        args.append('-listcommondirs')
        try:
            results = '[' + self.section() + ']\n'
            results += self.__ConfigGen()(*args)
            return results
        except Error as err:
            return repr(err)

    def machines(self):
        args = []
        args.append('-machines')
        try:
            results = '[' + self.section() + ']\n'
            data = self.__ConfigGen()(*args)
            data = [line for line in data.split('\n') if line.strip()]
            cnt = 0
            for i in data:
                results += "machine" + repr(cnt) + "=" + i
                cnt += 1
            return results
        except Error as err:
            return repr(err)
