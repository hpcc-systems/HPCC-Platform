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

import configparser
import json

try:
    from io import StringIO
except:
    from io import StringIO

from collections import deque, namedtuple
from ..common.dict import _dict


class ConfigGenerator:
    def __init__(self):
        pass

    def parseConfig(self, config, section='Default'):
        self.section = section
        self.conf = configparser.ConfigParser()
        s = StringIO(config)
        self.conf.readfp(s)

    def sections(self):
        return self.conf.sections()

    def get(self, item, section=None):
        if not section:
            section = self.section
        return self.conf.get(section, item)


class Config:
    def __init__(self, file):
        self.fileName = file
        self.configObj = self.loadConfig(self.fileName)

    def loadConfig(self, file):
        try:
            fp = open(file)
            js = json.load(fp)
            rC = namedtuple("Regress", list(js.keys()))
            return _dict(getattr(rC(**js), "Regress"))
        except IOError as e:
            if e.errno == 21:
                raise IOError("Error: "+file+" "+e.strerror+", we need a JSON file!")
            else:
                raise(e)

    #implement writing out of a config.
    def writeConfig(self, file):
        pass
