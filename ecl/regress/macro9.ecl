/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
