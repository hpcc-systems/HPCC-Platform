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
        self.regressionDir = self.config.regressionDir
        self.logDir = self.config.logDir
        self.setupDir = os.path.join(self.suiteDir, self.config.setupDir)
        self.dir_ec = os.path.join(self.suiteDir, self.config.eclDir)
        self.dir_ex = os.path.join(self.suiteDir, self.config.keyDir)
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

    def bootstrap(self):
        self.createDirectory(self.regressionDir)
        self.createDirectory(self.dir_a)
        self.createDirectory(self.dir_r)
        self.createDirectory(self.logDir)
        self.setup = self.Setup()
        for cluster in self.config.Clusters:
            self.createSuite(cluster)
        os.chdir(self.regressionDir)

    def createDirectory(self, dir_n):
        if not os.path.isdir(dir_n):
            os.makedirs(dir_n)

    def createSuite(self, cluster):
        self.suites[cluster] = Suite(cluster, self.dir_ec,
                                     self.dir_a, self.dir_ex, self.dir_r)

    def Setup(self):
        return Suite('setup', self.setupDir, self.dir_a, self.dir_ex,
                     self.dir_r)

    def runSuite(self, name, suite):
        server = self.config.ip
        report = Report(name)
        curTime = time.strftime("%y-%m-%d-%H-%M")
        logName = name + "." + curTime + ".log"
        if name == "setup":
            cluster = 'hthor'
        else:
            cluster = name
        log = os.path.join(self.logDir, logName)
        self.log.addHandler(log, 'DEBUG')
        logging.warn("Suite: %s" % name)
        logging.warn("Queries: %s" % repr(len(suite.getSuite())))
        cnt = 1
        for query in suite.getSuite():
            logging.warn("%s. Test: %s" % (repr(cnt), query.ecl))
            ECLCC().makeArchive(query)
            res = ECLcmd().runCmd("run", cluster, query, report,
                                  server=server, username=self.config.username,
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
            cnt += 1
        report.display(log)
