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

import time
import subprocess
import hpcc.cluster.host
import logging
import hashlib

class Task(object):
    '''
    An abstract class to represent a task which will be executed for all hosts
    of cluster
    '''

    def __init__(self, id):
        '''
        Constructor
        '''
        self._id = id
        self._status = 'INIT' #INIT,RUNNING,DONE
        self._start_time = None
        self._end_time = None
        self._duration = None
        self._message = ""
        self._result = "UNKNOWN" #UNKNOWN, SUCCEED,FAILED

        self.logger = logging.getLogger("hpcc.cluster.Task." + str(id))

    @property
    def id(self):
        return self._id

    @property
    def duration(self):
        return self._duration

    @property
    def status(self):
        return self._status

    @property
    def result(self):
        return self._result

    @property
    def message(self):
        return self._message

    def pretask(self):
        self._start_time = time.time()


    def posttask(self):
        self._end_time = time.time()
        self._duration = self._end_time - self._start_time
        self._status = "DONE"


    def run (self, host):
        self.pretask()
        self._status = "RUNNING"
        self.worker(host)
        self.posttask()

    def worker(self, host):
        pass



class ScriptTask(Task):
    '''
    A subclass of Task to implement shell script task.
    '''


    def __init__(self, id, script_file=None):
        '''
        Constructor
        '''
        Task.__init__(self, id)
        self.script_file = script_file
        self.logger = logging.getLogger("hpcc.cluster.ScriptTask." + str(id))
        self._checksum = None

    @property
    def checksum(self):
        return self._checksum

    @checksum.setter
    def checksum(self, chksum):
        self._checksum = chksum

    def validateScriptFile(self):
        if not self._checksum:
            return True

        with open (self.script_file) as f:
            file_md5 = hashlib.md5(f.read()).hexdigest()
            if self._checksum == file_md5:
                return True;
            else:
                return False


    def worker(self, host):
        '''
        worker: execute provided script with subprocess module.
        Python documentation discourage 'shell=True' option to
        avoid shell injection attack. Since we need general shell
        execution environment we need this option. To add security
        we add md5 check if the checksum is provided from command0-line
        option. Also user should ensure the script permission to
        protect the script from malicious modification.
        '''
        cmd = self.script_file + " " + host.ip.decode('utf-8')
        self.logger.info(cmd)
        if not self.validateScriptFile():
            self.logger.error("Script file check sum does not match")
            self._result = "FAILED"
            return

        try:
            # subprocess.check_output is more convenvient but only available
            # on Python 2.7+
            process = subprocess.Popen(cmd, shell=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)
            process.wait()
            errcode = process.returncode
            output = process.communicate()[0]
            self._message = output

            if (errcode == 0):
                self.logger.debug(self._message)
                self._result = "SUCCEED"
            else:
                self.logger.error(self._message)
                self._result = "FAILED"

        except Exception as e:
            self._message = "Catch Exception: \n" + e.output
            self.logger.error(self._message)
            self._result = "FAILED"

        self.logger.info("result: " + self._result)
