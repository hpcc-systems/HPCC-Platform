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
import os
import sys
import time

from ..common.config import Config
from ..common.error import Error
from ..common.logger import Logger
from ..common.report import Report, Tee
from ..regression.suite import Suite
from ..util.ecl.cc import ECLCC
from ..util.ecl.command import ECLcmd
from ..util.expandcheck import ExpandCheck


class Regression:
    def __init__(self, config="regress.json", level='info', suiteDir=None):
        self.config = Config(config).configObj
        self.suites = {}
        self.log = Logger(level)
        if not suiteDir:
            self.suiteDir = self.config.suiteDir
            if not self.suiteDir:
                raise Error("2002")
        else:
            self.suiteDir = suiteDir

        self.suiteDir = ExpandCheck.dir_exists(suiteDir, True)
        self.regressionDir = ExpandCheck.dir_exists(self.config.regressionDir, True)
        self.logDir = ExpandCheck.dir_exists(self.config.logDir, True)
        self.setupDir = ExpandCheck.dir_exists(os.path.join(self.suiteDir, self.config.setupDir), True)
        self.dir_ec = ExpandCheck.dir_exists(os.path.join(self.suiteDir, self.config.eclDir), True)
        self.dir_ex = ExpandCheck.dir_exists(os.path.join(self.suiteDir, self.config.keyDir), True)
        self.dir_a = os.path.join(self.regressionDir, self.config.archiveDir)
        self.dir_r = os.path.join(self.regressionDir, self.config.resultDir)
        logging.debug("Suite Dir      : %s", suiteDir)
        logging.debug("Regression Dir : %s", self.regressionDir)
        logging.debug("Result Dir     : %s", self.dir_r)
        logging.debug("Log Dir        : %s", self.logDir)
        logging.debug("ECL Dir        : %s", self.dir_ec)
        logging.debug("Key Dir        : %s", self.dir_ex)
        logging.debug("Setup Dir      : %s", self.setupDir)
        logging.debug("Archive Dir    : %s", self.dir_a)

    def setLogLevel(self, level):
        self.log.setLevel(level)

    def bootstrap(self, cluster):
        self.createDirectory(self.regressionDir)
        self.createDirectory(self.dir_a)
        self.createDirectory(self.dir_r)
        self.createDirectory(self.logDir)
        self.setup = self.Setup()
        if cluster in self.config.Clusters:
            self.createSuite(cluster)
        os.chdir(self.regressionDir)

    def createDirectory(self, dir_n):
        if not os.path.isdir(dir_n):
            os.makedirs(dir_n)

    def createSuite(self, cluster):
        self.suites[cluster] = Suite(cluster, self.dir_ec,
                                     self.dir_a, self.dir_ex, self.dir_r, self.logDir)

    def Setup(self):
        return Suite('setup', self.setupDir, self.dir_a, self.dir_ex,
                     self.dir_r, self.logDir)

    def buildLogging(self, name):
        report = Report(name)
        curTime = time.strftime("%y-%m-%d-%H-%M")
        logName = name + "." + curTime + ".log"
        log = os.path.join(self.logDir, logName)
        self.log.addHandler(log, 'DEBUG')
        return (report, log)

    @staticmethod
    def displayReport(report):
        report[0].display(report[1])

    def runSuite(self, name, suite):
        report = self.buildLogging(name)
        if name == "setup":
            cluster = 'hthor'
        else:
            cluster = name

        logging.warn("Suite: %s" % name)
        logging.warn("Queries: %s" % repr(len(suite.getSuite())))
        cnt = 1
        for query in suite.getSuite():
            self.runQuery(cluster, query, report, cnt, suite.testPublish(query.ecl))
            cnt += 1
        Regression.displayReport(report)

    def runQuery(self, cluster, query, report, cnt=1, publish=False):
        logging.debug("runQuery(cluster:", cluster, ", query:", query, ", report:", report, ", cnt:", cnt, ", publish:", publish, ")")
        logging.warn("%s. Test: %s" % (repr(cnt), query.ecl))
        ECLCC().makeArchive(query)
        if publish:
            res = ECLcmd().runCmd("publish", cluster, query, report[0],
                              server=self.config.ip,
                              username=self.config.username,
                              password=self.config.password)
        else:
            res = ECLcmd().runCmd("run", cluster, query, report[0],
                              server=self.config.ip,
                              username=self.config.username,
                              password=self.config.password)
        wuid = query.getWuid()
        if wuid:
            url = "http://" + self.config.server
            url += "/WsWorkunits/WUInfo?Wuid="
            url += wuid

        if res:
            logging.info("Pass %s" % wuid)
            logging.info("URL %s" % url)
        else:
            if not wuid:
                logging.error("Fail No WUID")
            else:
                logging.error("Fail %s" % wuid)
                logging.error("URL %s" % url)
