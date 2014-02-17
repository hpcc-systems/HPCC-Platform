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

import os
import logging

class ExpandCheck:

    @staticmethod
    def dir_exists(path, require=False):
        logging.debug("dir_exists(path: %s, require: %s)", path, require)
        if '~' in path:
            path = os.path.expanduser(path)
        else:
            path = os.path.abspath(path)
        logging.debug("path: %s", path)
        if not os.path.exists(path):
            if require:
                logging.debug("Path: %s not found and it is required!" ,path)
                try:
                    os.mkdir(path)
                except:
                    raise IOError("REQUIRED DIRECTORY NOT FOUND. " + path)
            else:
                logging.debug( "DIRECTORY NOT FOUND. " + path)
                path = None

        return(path)

