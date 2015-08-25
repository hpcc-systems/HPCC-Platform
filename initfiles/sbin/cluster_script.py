#!/usr/bin/env python
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

import sys
import os
import os.path
import getopt
import Queue
import time
import datetime
import logging
import ConfigParser
import signal

def signal_handler(signal, frame):
        print('\n\nctrl-c\n')
        os._exit(1)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGQUIT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

from hpcc.cluster.host import Host
from hpcc.cluster.task import ScriptTask
from hpcc.cluster.thread import ThreadWithQueue

class ScriptExecution(object):
    '''
    This class implements concurrent task execution in a list of hosts.
    It provides a main function. Run cluster_script.py --help for the usage
    '''


    def __init__(self):
        '''
        Constructor
        '''

        self.env_conf         = '/etc/HPCCSystems/environment.conf'
        self.section          = 'DEFAULT'
        self.hpcc_config      = None
        self.host_list_file   = None
        self.log_file         = None
        self.script_file      = None
        self.number_of_threads  = 5
        self.exclude_local    = False
        self.log_level        = "INFO"
        self.chksum           = None

        self.quque = None
        self.hosts = []
        self.tasks = []
        self.threads = []
        self.logger = None


    def get_config(self, key):
        if not self.hpcc_config:
            self.hpcc_config = ConfigParser.ConfigParser()
            self.hpcc_config.read(self.env_conf)

        return self.hpcc_config.get(self.section, key)

    def set_logger(self):
        if not self.log_file:
            self.log_file = self.get_config('log') + \
               "/cluster/cc_" + os.path.basename(self.script_file) + \
                "_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + ".log"
        log_directory = os.path.dirname(self.log_file)
        if log_directory and not os.path.exists(log_directory):
            os.makedirs(log_directory)

        numeric_level = getattr(logging, self.log_level.upper(), None)
        self.logger = logging.getLogger("hpcc.cluster")
        self.logger.setLevel(numeric_level)
        fh = logging.FileHandler(self.log_file)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

    def setup(self):


        self.queue = Queue.Queue()

        # Get hosts information
        if self.host_list_file:
            self.hosts = Host.get_hosts_from_file( self.host_list_file, self.exclude_local )
        else:
            self.hosts = Host.get_hosts_from_env(
                self.get_config( 'configs' ) + '/' + \
                self.get_config( 'environment' ),
                self.get_config( 'path' ), self.exclude_local )

        if len(self.hosts) == 0:
            print("Could not get any host. At least one host is required.")
            print("Reference following log for more information: ")
            print(self.log_file)
            exit(0)

        self.addTasks(self.number_of_threads * 2)



    def addTasks(self, n):
        current_tasks_size = len(self.tasks)
        hosts_size = len(self.hosts)
        if current_tasks_size >= hosts_size:
            return

        number_to_add = n
        unscheduled_hosts_size = hosts_size - current_tasks_size
        if n > unscheduled_hosts_size:
            number_to_add =  unscheduled_hosts_size

        next_host_index = current_tasks_size
        last_host_to_add = next_host_index + number_to_add - 1
        self.logger.info("add " + str(number_to_add) + " tasks to thread queue")
        while (next_host_index <= last_host_to_add):
            task = ScriptTask(next_host_index, self.script_file)
            if self.chksum:
                task.checksum=self.chksum

            self.tasks.append(task)
            # Assign the task to a host and add it to schedule queue
            self.queue.put((task.run, self.hosts[next_host_index]))
            next_host_index += 1


    def execute(self):

        thread_id = 0
        for _ in range(self.number_of_threads):
            thread = ThreadWithQueue(thread_id, self.queue)
            self.threads.append(thread)
            thread.start()
            thread_id += 1

        print("\nTotal hosts to process: %d\n" % (len(self.hosts)))

        while (True):
            if self.is_done():
                self.report_status()
                for thread in self.threads:
                    thread.stop()
                break
            else:
                self.report_status()
                self.update_queue()
                time.sleep(2)


    def is_done(self):
        if len(self.tasks) < len(self.hosts):
            return False
        for task in self.tasks:
            if task.status != "DONE":
                return False
        self.logger.info("script execution done.")
        return True

    def update_queue(self):
        if len(self.tasks) >= len(self.hosts):
            return

        if self.queue.qsize() <= self.number_of_threads:
            self.addTasks(self.number_of_threads * 2)

    def report_status(self):
        current_done    = 0
        current_succeed = 0
        current_failed  = 0
        current_running = 0
        current_in_queue = 0
        for task in self.tasks:
            if task.status == 'DONE':
                current_done += 1
                if task.result == 'SUCCEED':
                    current_succeed += 1
                else:
                    current_failed += 1
            elif task.status == 'RUNNING':
                current_running += 1
            else:
                current_in_queue += 1

        progress = (current_done * 100) / len(self.hosts)
        sys.stdout.write("\rExecution progress: %d%%, running: %d, in queue: %d, succeed: %d, failed: %d" \
          % (progress, current_running, current_in_queue, current_succeed, current_failed))
        sys.stdout.flush();


    def check_error(self):

        no_error_found = True
        for task in self.tasks:
            if task.result != 'SUCCEED':
                no_error_found = False
        script_name = os.path.basename(self.script_file)
        if not no_error_found:
            print("\n\n\033[91mError found during " + script_name + " execution.\033[0m")
            print("Reference following log for more information: ")
            print(self.log_file)
        else:
            print("\n\n" + script_name + " run successfully on all hosts in the cluster")

        print("\n")

        return no_error_found

    def usage(self):
        print("Usage cluster_script.py [option(s)]\n")
        print("  -?, --help               print help")
        print("  -c  --chksum             script file md5 checksum")
        print("  -e, --env_conf           environment.conf full path. The default is")
        print("                           /etc/HPCCSystems/environment.conf")
        print("  -f, --script_file        script file")
        print("  -h  --host_list          by default hosts will be retrieved from environment.xml")
        print("  -l, --log_level          WARNING, INFO, DEBUG. The default is INFO")
        print("  -n, --number_of_threads  number of working threads for concurrent execution")
        print("  -o, --log_file           by default only log on error unless -v specified")
        print("                           default log file is se_<script name>_<yyymmdd_hhmmss>.log")
        print("                           under <log_dir>/cluster directory")
        print("  -s, --section            environment.conf section. The default is DEFAULT.")
        print("  -x, --exclude_local      script will not run on local system")
        print("\n");


    def process_args(self):

        try:
             opts, args = getopt.getopt(sys.argv[1:],":c:e:f:h:l:n:o:s:x",
                ["help", "chksum","env_conf","script_file","host_list", "number_of_threads",
                 "section", "log_file", "log_level", "exclude_local"])

        except getopt.GetoptError as err:
            print(str(err))
            self.usage()
            exit(0)


        for arg, value in opts:
            if arg in ("-?", "--help"):
                self.usage()
                exit(0)
            elif arg in ("-c", "--chksum"):
                self.chksum = value
            elif arg in ("-e", "--env_conf"):
                self.env_conf = value
            elif arg in ("-h", "--host_list"):
                self.host_list_file = value
            elif arg in ("-n", "--number_of_thread"):
                self.number_of_threads = int(value)
            elif arg in ("-o", "--log_file"):
                self.log_file = value
            elif arg in ("-f", "--script_file"):
                self.script_file = value
            elif arg in ("-l", "--log_level"):
                self.log_level = value
            elif arg in ("-s", "--section"):
                self.section = value
            elif arg in ("-x", "--exclude_local"):
                self.exclude_local = True
            else:
                print("\nUnknown option: " + arg)
                self.usage()
                exit(0)



    def validate_args(self):
        if not self.script_file:
            print("\nMissing required script file\n")
            self.usage()
            exit(0)

        if not os.path.isfile(self.script_file):
            print("\nFile " + self.script_file + " does not exist.\n")
            exit(0)




    def log_input_parameters(self):
        self.logger.info("Current parameters:")
        self.logger.info("%-20s" % "env_conf" + ":  %s" % self.env_conf )
        self.logger.info("%-20s" % "script_file" + ":  %s" % self.script_file )
        self.logger.info("%-20s" % "log_file" + ":  %s" % self.log_file )
        self.logger.info("%-20s" % "log_level" + ":  %s" % self.log_level )
        self.logger.info("%-20s" % "host_list_file" + ":  %s" % self.host_list_file )
        self.logger.info("%-20s" % "number_of_thread" + ":  %d" % self.number_of_threads )
        self.logger.info("%-20s" % "section" + ":  %s" % self.section )
        self.logger.info("%-20s" % "exclude_local" + ":  %d" % self.exclude_local )


if __name__ == '__main__':

    se = ScriptExecution()
    se.process_args()
    se.validate_args()
    se.set_logger()
    se.log_input_parameters()
    se.setup()
    se.execute()
    if se.check_error():
        exit(0)
    else:
        exit(1)
