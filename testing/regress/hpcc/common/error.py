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

ERROR = {
    "1000": "Command not found.",
    "1001": "Command return code non-zero.",
    "1002": "Command timed out.",
    "2000": "Value must be of Type Suite or ECLFile.",
    "2001": "ECL Directory does not exist.",
    "2002": "suiteDir not set. Set in ecl-test.json or use --suiteDir",
    "3000": "Return is null",
    "3001": "Return diff does not match.",
    "4000": "Unknown cluster!",
    "4001": "No ECl file!"
}


class Error(Exception):
    def __init__(self, code, **kwargs):
        self.code = code
        self.err = kwargs.pop('err', False)

    def __str__(self):
        if self.err:
            return "Error (%s): %s \n %s\n" % (self.code,
                                               ERROR[self.code], self.err)
        return "Error (%s): %s " % (self.code, ERROR[self.code])
        
    def getErrorCode(self):
        return self.code
