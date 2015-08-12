/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

EXPORT BOOLEAN DEBUG_FLAG := TRUE;

EXPORT DEBUG := MODULE
  EXPORT MAC_OUTPUT(RSET, RSET_NAME=#TEXT('')) := MACRO
           #IF(DEBUG_FLAG)
                     #UNIQUENAME(RSN)
                     %RSN% := IF(RSET_NAME <> '', RSET_NAME, #TEXT(RSET));
                     OUTPUT(RSET, NAMED(%RSN%));
           #ELSE
               EVALUATE(0);
           #END
  ENDMACRO;
  EXPORT LOG(string RSET, string RSET_NAME='') := function
        MAC_OUTPUT(RSET);
        return evaluate(0);
  END;
  EXPORT LOG2(string RSET, string RSET_NAME='') := function
            maxLineLen := 1024;
            outRec := { string line{maxlength(maxLineLen)} };
            outLine := RSET[1..MIN(LENGTH(RSET),maxLineLen)];
            RSN := IF(RSET_NAME <> '', RSET_NAME, 'DEBUG_LOG');
            RETURN IF(DEBUG_FLAG, OUTPUT(dataset([outLine], outRec), NAMED(RSN), EXTEND));
  END;
END;


EXPORT BOOLEAN debugTrace := TRUE : labelled('debugTrace');
EXPORT BOOLEAN debugTrace2 := TRUE : labelled('debugTrace2');

#constant ('debugTrace2', false);

x:='Sanjay';
DEBUG.LOG(x);
//DEBUG.MAC_OUTPUT('laslasdl');
DEBUG.LOG2(x);
DEBUG.LOG2('laslasdl');

output(debugTrace);
output(debugTrace2);
