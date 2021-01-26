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

import threading
import queue

class ThreadWithQueue(threading.Thread):
    '''
    A thread with shared queue. This will help parallelly execute the task on hosts in the cluster
    '''

    def __init__(self, tid, queue):
        threading.Thread.__init__(self)
        self.queue = queue
        self.keepAlive = True
        self._id = tid

    @property
    def id(self):
        return self._id

    def stop(self):
        self.keepAlive = False

    def run(self):
        while self.keepAlive:
            try:
                # block is false, timeout is 1 second. Ignore queue.Empty exception
                # thread is controlled by keepAlive
                items = self.queue.get(False, 1)
                func = items[0]
                args = items[1:]
                func(*args)

                self.queue.task_done()
            except queue.Empty: pass
