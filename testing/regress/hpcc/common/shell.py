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

import logging
import sys
from subprocess import (
    PIPE,
    Popen,
    CalledProcessError
)
from ..common.error import Error

#TODO: Find a better way since which is tempramental.
CMD = {
    "eclcc": "/usr/bin/eclcc",
    "ecl": "/usr/bin/ecl",
    "configgen": "/opt/HPCCSystems/sbin/configgen"
}


class Shell:
    def command(self, *command_args):
        def __command(*args):
            all_args = command_args + args
            return self.__run(*all_args)
        return __command

    def __hidePassw(self,  item):
        if '--password' in item:
            return '--password=********'
        else:
            return item

    def __run(self, *args, **kwargs):
        _args = [i for i in args if i is not None]
        argsLog = [self.__hidePassw(i) for i in args if i is not None ]
        logging.debug("Shell _run CMD: " + " ". join(argsLog))
        process = Popen(
            _args, stdout = PIPE, stderr = PIPE, close_fds = True, **kwargs)
        stdout, stderr = process.communicate()
        retCode = process.returncode
        logging.debug("Shell _run retCode: %d",  retCode)
        logging.debug("            stdout:'%s'",  stdout)
        logging.debug("            stderr:'%s'",  stderr)

        if retCode or ((len(stderr) > 0) and ('Error' in stderr)):
            exception = CalledProcessError(
                process.returncode, repr(args))
            err_msg = "retCode: "+str(retCode)+", msg:'"+str(''.join(filter(None, [stdout, stderr])))+"'"
            exception.output = err_msg
            logging.debug("exception.output:'%s'",  err_msg)
            raise Error('1001', err=str(err_msg))
        return stdout

    # Currently hacked to use the CMD dict as which can be tempramental.
    # - What other methods can be used?
    # - Support multiple versions of eclcc?
    def which(self, command):
        if command in CMD:
            return CMD[command]
        return self.__run("which", command).rstrip('\n')
