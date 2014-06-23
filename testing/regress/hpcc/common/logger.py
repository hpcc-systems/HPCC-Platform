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
import sys
import time

#from ..regression.regress import Regression

try:
    import curses
except:
    curses = None


class Logger(object):
    class _LogFormatter(logging.Formatter):
        def __init__(self, color, *args, **kwargs):
            logging.Formatter.__init__(self, *args, **kwargs)
            self._color = color
            if color:
                fg_color = (curses.tigetstr("setaf")
                            or curses.tigetstr("setf") or "")
                self._colors = {
                    logging.ERROR: curses.tparm(fg_color, 1),
                    logging.INFO: curses.tparm(fg_color, 2),
                    logging.CRITICAL: curses.tparm(fg_color, 3),
                    logging.WARNING: curses.tparm(fg_color, 5),
                    logging.DEBUG: curses.tparm(fg_color, 4)
                }
                self._normal = curses.tigetstr("sgr0")

        def format(self, record):
            try:
                record.message = record.getMessage()
            except Exception, e:
                record.message = "Bad message (%r): %r" % (e, record.__dict__)
            record.asctime = time.strftime(
                "%y-%m-%d %H:%M:%S", self.converter(record.created))
            if record.__dict__['levelname'] == "ERROR":
                prefix = '[Failure]' % \
                    record.__dict__
            elif record.__dict__['levelname'] == "CRITICAL":
                prefix = '[Error]' % \
                    record.__dict__
            elif record.__dict__['levelname'] == "DEBUG":
                prefix = '[Debug]' % \
                    record.__dict__
            elif record.__dict__['levelname'] == "INFO":
                prefix = '[Pass]' % \
                    record.__dict__
            elif record.__dict__['levelname'] == "WARNING":
                prefix = '[Action]' % \
                    record.__dict__
            if self._color:
                prefix = (self._colors.get(record.levelno, self._normal) +
                          prefix + self._normal)
            formatted = prefix + " " + record.message
            if record.exc_info:
                if not record.exc_text:
                    record.exc_text = self.formatException(record.exc_info)
            if record.exc_text:
                formatted = formatted.rstrip() + "\n" + record.exc_text
            return formatted.replace("\n", "\n    ")

    class ProgressFileHandler(logging.FileHandler):
        terminator = '\n'
        isBuffer = False
        logBuffer=dict()
        taskId = 0
        taskIds = {}

        def close(self):
            if len(self.logBuffer):
                stream = self.stream
                for item in self.logBuffer:
                    for line in self.logBuffer[item]:
                        stream.write(line)
                        stream.write(self.terminator)
                self.logBuffer.clear()
                self.taskIds = {}
                self.isBuffer = False
            self.flush()

        def addTaskId(self,  taskId,  threadId,  timestamp):
            if not self.taskIds.has_key(threadId):
                self.taskIds[threadId] ={'taskId':taskId, 'timestamp':'timestamp'}
            elif self.taskIds[threadId]['timestamp'] != timestamp:
                self.taskIds[threadId] ={'taskId':taskId, 'timestamp':'timestamp'}


        def getTaskId(self, threadId):
            record = self.taskIds.get(threadId,  {'taskId':0, 'timestamp':'-'})
            return record['taskId']


        def emit(self, record):
            try:
                msg = self.format(record)
                stream = self.stream
                isBuffer = hasattr(record, 'filebuffer')
                toSort = hasattr(record,  'filesort')
                taskId = 0
                if hasattr(record, 'taskId'):
                    taskId = getattr(record,  'taskId')
                    self.addTaskId(taskId,  record.thread,  record.asctime)
                else:
                    if record.threadName == "MainThread":
                        taskId = 0
                    else:
                        taskId = self.getTaskId(record.thread)
                if record.levelname == 'DEBUG':
                    msg +=" [asctime:"+record.asctime+", process:"+str(record.process)+", processName:"+record.processName+", thread:"+str(record.thread)+", threadName:"+record.threadName+"]"
                    msg = "{0:3d}".format(taskId) + ". Debug-[debug-"+record.asctime+"]: "+msg
                if record.levelname == 'CRITICAL':
                    msg += " [level: "+record.levelname+" ]"
                    msg = "{0:3d}".format(taskId) +". " + msg
                if isBuffer:
                    #toggle buffer switch
                    self.isBuffer = not self.isBuffer
                    isBuffer = False
                if self.isBuffer or isBuffer:
                    if len(msg):
                        self.logBuffer.setdefault(taskId,  []).append(msg)
                else:
                    if len(self.logBuffer):
                        for item in self.logBuffer:
                            for line in self.logBuffer[item]:
                                line = line.replace(". Debug-", ".  ")
                                stream.write(line)
                                stream.write(self.terminator)
                        self.logBuffer.clear()
                        self.taskIds = {}
                        self.isBuffer = False
                    if  len(msg):
                        stream.write(msg)
                        stream.write(self.terminator)
                    self.flush()
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as ex:
                self.handleError(record)

    def addHandler(self, fd, level='info'):
        root_logger = logging.getLogger()
        self.channel = self.ProgressFileHandler(fd)
        self.channel.setLevel(getattr(logging, level.upper()))
        root_logger.addHandler(self.channel)

    def removeHandler(self):
        root_logger = logging.getLogger()
        root_logger.removeHandler(self.channel)
        self.channel.flush()
        self.channel.close()

    def enable_pretty_logging(self):
        root_logger = logging.getLogger()
        color = False
        if curses and sys.stderr.isatty():
            try:
                curses.setupterm()
                if curses.tigetnum("colors") > 0:
                    color = True
            except:
                pass
        channel = logging.StreamHandler()
        channel.setFormatter(Logger._LogFormatter(color=color))
        root_logger.addHandler(channel)

    def setLevel(self, level):
        logging.getLogger().setLevel(getattr(logging, level.upper()))

    def __init__(self, level='info'):
        self.setLevel(level)
        self.enable_pretty_logging()
        self.taskId = 0;
