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

    def addHandler(self, fd, level='info'):
        root_logger = logging.getLogger()
        channel = logging.FileHandler(fd)
        channel.setLevel(getattr(logging, level.upper()))
        root_logger.addHandler(channel)

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
