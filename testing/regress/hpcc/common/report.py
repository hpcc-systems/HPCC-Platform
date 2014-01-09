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

from ..regression.suite import Suite
from ..util.ecl.file import ECLFile
from ..common.dict import _dict
from ..common.error import Error

import logging
import sys


class Tee(object):
    def __init__(self, name, mode):
        self.file = open(name, mode)
        self.stdout = sys.stdout
        sys.stdout = self

    def __del__(self):
        sys.stdout = self.stdout
        self.file.close()

    def write(self, data):
        self.file.write(data)
        self.stdout.write(data)


class Report:
    def __init__(self, name, suite=None):
        report = {'_pass': [], '_fail': []}
        self.report = _dict(report)
        self.name = name

    def display(self, log=None,  elapsTime = 0):
        logging.debug("Report::display(log='%s', elapsTime:%d",  log,  elapsTime)
        reportStr = "\n"
        reportStr += "Results\n"
        reportStr += "-------------------------------------------------\n"
        reportStr += "Passing: %i\n" % len(self.report._pass)
        reportStr += "Failure: %i\n" % len(self.report._fail)
        reportStr += "-------------------------------------------------\n"
        if self.report._fail:
            for result in self.report._fail:
                try:
                    reportStr += result.Diff
                except Exception as ex:
#                   logging.debug("Exception:'%s'",  str(ex))
                    reportStr += str(result.Diff)
                reportStr += "\n"
            reportStr += "-------------------------------------------------\n"
        if log:
            reportStr += "Log: %s\n" % str(log)
            reportStr += "-------------------------------------------------\n"
        if elapsTime:
            reportStr += "Elapsed time: %d sec " % (elapsTime)
            hours = elapsTime / 3600
            elapsTime = elapsTime % 3600
            mins = elapsTime / 60
            reportStr += " (%02d:%02d:%02d) \n" % (hours,  mins,  elapsTime % 60)
            reportStr += "-------------------------------------------------\n"
        logging.warn(reportStr)

    def getResult(self, eclfile):
        pass

    def addResult(self, result):
        if isinstance(result, Suite):
            self.__addSuite(result)
        elif isinstance(result, ECLFile):
            self.__addEclFile(result)
        else:
            raise Error('2000')

    def __addSuite(self, suite):
        for eclfile in suite:
            result = {}
            result['File'] = eclfile.ecl
            if eclfile.diff:
                result['Result'] = 'Fail'
                result['Diff'] = eclfile.diff
                self.report._fail.append(_dict(result))
            else:
                result['Result'] = 'Pass'
                self.report._pass.append(_dict(result))

    def __addEclFile(self, eclfile):
        result = {}
        result['File'] = eclfile.ecl
        if eclfile.diff:
            result['Result'] = 'Fail'
            result['Diff'] = eclfile.diff
            self.report._fail.append(_dict(result))
        else:
            result['Result'] = 'Pass'
            self.report._pass.append(_dict(result))
