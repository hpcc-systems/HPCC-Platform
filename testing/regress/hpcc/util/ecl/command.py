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

import logging

from ...common.shell import Shell
from ...util.ecl.file import ECLFile
from ...common.error import Error
from ...util.util import queryWuid

class ECLcmd(Shell):
    def __init__(self):
        self.defaults = []
        self.cmd = self.which('ecl')

    def __ECLcmd(self):
        return self.command(self.cmd, *self.defaults)

    def runCmd(self, cmd, cluster, eclfile, report, **kwargs):
        args = []
        args.append(cmd)
        args.append('-v')
        args.append('--cluster=' + cluster)
        if cmd == 'publish':
            args.append(eclfile.getEcl())
        else:
            args.append('--noroot')
            name = kwargs.pop('name', False)
            username = kwargs.pop('username', False)
            password = kwargs.pop('password', False)
            server = kwargs.pop('server', False)
            if server:
                args.append('--server=' + server)
            if not name:
                #name = eclfile.ecl
                name = eclfile.getJobname()
            if username:
                args.append("--username=" + username)
            if password:
                args.append("--password=" + password)
            args.append("--name=" + name)
            args.append(eclfile.getArchive())
        data = ""
        wuid = ""
        state = ""
        try:
            #print "runCmd:", args
            results = self.__ECLcmd()(*args)
            data = '\n'.join(line for line in
                             results.split('\n') if line) + "\n"
            ret = data.split('\n')
            result = ""
            cnt = 0
            for i in ret:
                if "wuid:" in i:
                    wuid = i.split()[1]
                if "state:" in i:
                    state = i.split()[1]
                if "aborted" in i:
                    state = "aborted"
                if cnt > 4:
                    result += i + "\n"
                cnt += 1
            data = '\n'.join(line for line in
                             result.split('\n') if line) + "\n"

        except Error as err:
            data = repr(err)
            logging.error("------" + err + "------")
            return err + "\n"
        finally:
            eclfile.addResults(data, wuid)
            if cmd == 'publish':
                if state == 'compiled':
                    test = True
                else:
                    test = False
                    eclfile.diff = 'Error'
            else:
                if queryWuid(eclfile.getJobname())['state'] == 'aborted':
                    eclfile.diff = eclfile.ecl+'\n\t'+'Aborted ( reason: '+eclfile.getAbortReason()+' )'
                    test = False
                else:
                    test = eclfile.testResults()
            report.addResult(eclfile)
            if not test:
                return False
            else:
                return True
