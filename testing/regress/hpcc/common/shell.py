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

from subprocess import (
    PIPE,
    Popen,
    CalledProcessError
)
from ..common.error import Error

logger = logging.getLogger('RegressionTestEngine')

class Shell:
    def __init__(self):
        pass
        
    def command(self, *command_args):
        def __command(*args):
            all_args = command_args + args
            logger.debug("Shell.command(all_args: '%s'", all_args)
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
        logger.debug("Shell _run CMD: " + " ". join(argsLog))
        process = Popen(
            _args, stdout = PIPE, stderr = PIPE, close_fds = True, **kwargs)
        _stdout, _stderr = process.communicate()
        retCode = process.returncode
        logger.debug("Shell _run retCode: %d",  retCode)
        stdout = str(_stdout).replace("\\n",  "\n").strip('\n\' ')
        stdout = _stdout.decode("utf-8")
        if stdout.startswith("b'\n") or stdout.startswith('b"\n'):
            stdout = stdout[4:-1]
        logger.debug("            stdout:'%s'",  stdout)
        stderr = _stderr.decode("utf-8").replace("\\n",  "\n").lstrip('\n').lstrip("'")
        logger.debug("            stderr:'%s'",  stderr)

        if retCode or ((len(stderr) > 0) and ('Error' in stderr)) or ((retCode == 4) and ('<Exception>' in stdout)):
            exception = CalledProcessError(process.returncode, repr(args))
            err_msg = "retCode: "+str(retCode)+"\n'"+str(''.join([str(_f) for _f in [stderr] if _f]))
            if (retCode == 4) and ('<Exception>' in stdout):
                err_msg += str(''.join([_f for _f in [stdout] if _f]))
            exception.output = err_msg +"'"
            logger.debug("exception.output:'%s'",  err_msg)
            raise Error('1001', err=str(err_msg))
        return stdout, stderr
