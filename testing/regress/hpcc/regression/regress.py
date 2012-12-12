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

from ..common.config import Config
from ..common.logger import Logger
from ..common.report import Report, Tee
from ..regression.suite import Suite
from ..util.ecl.cc import ECLCC
from ..util.ecl.command import ECLcmd


class Regression:
    def __init__(self, config="regress.json"):
        self.config = Config(config).configObj
        self.suites = {}
        self.log = Logger("DEBUG")

    def bootstrap(self):
        archives = os.path.join(self.config.baseDir, self.config.archiveDir)
        results = os.path.join(self.config.baseDir, self.config.resultDir)
        self.createDirectory(archives)
        self.createDirectory(results)
        self.setup = self.Setup()
        for cluster in self.config.Clusters:
            self.createSuite(cluster)

    def createDirectory(self, dir_n):
        if not os.path.isdir(dir_n):
            os.makedirs(dir_n)

    def createSuite(self, cluster):
        dir_ec = os.path.join(self.config.baseDir, self.config.eclDir)
        dir_a = os.path.join(self.config.baseDir, self.config.archiveDir)
        dir_ex = os.path.join(self.config.baseDir, self.config.keyDir)
        dir_r = os.path.join(self.config.baseDir, self.config.resultDir)
        self.suites[cluster] = Suite(cluster, dir_ec, dir_a, dir_ex, dir_r)

    def Setup(self):
        setup = os.path.join(self.config.baseDir, self.config.setupDir)
        dir_a = os.path.join(self.config.baseDir, self.config.archiveDir)
        dir_ex = os.path.join(self.config.baseDir, self.config.keyDir)
        dir_r = os.path.join(self.config.baseDir, self.config.resultDir)
        return Suite('setup', setup, dir_a, dir_ex, dir_r)

    def runSuite(self, name, suite):
        logDir = os.path.join(self.config.baseDir, self.config.logDir)
        server = self.config.ip
        report = Report(name)
        logName = name + ".log"
        if name == "setup":
            cluster = 'hthor'
        else:
            cluster = name
        log = os.path.join(logDir, logName)
        self.log.addHandler(log, 'DEBUG')
        logging.debug("Suite: %s" % name)
        logging.debug("Queries: %s" % repr(len(suite.getSuite())))
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
        report.display()
