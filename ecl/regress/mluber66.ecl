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

export CleanFields(inputFile,outputFile) := macro

LOADXML('<xml/>');

#EXPORTXML(doCleanFieldMetaInfo, recordof(inputFile))

#uniquename (myCleanFunction)
string %myCleanFunction%(string x) := Stringlib.StringFilter(x,'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 .#$%&()-;:,"');

#uniquename (tra)
inputFile %tra%(inputFile le) :=
TRANSFORM

#IF (%'doCleanFieldText'%='')
 #DECLARE(doCleanFieldText)
#END
#SET (doCleanFieldText, false)
#FOR (doCleanFieldMetaInfo)
 #FOR (Field)
  #IF (%'@type'% = 'string')
    #SET (doCleanFieldText, 'SELF.' + %'@name'%)
    #APPEND (doCleanFieldText, ':= ' + %'myCleanFunction'% + '(le.')
    #APPEND (doCleanFieldText, %'@name'%)
    #APPEND (doCleanFieldText, ')')
    %doCleanFieldText%;
  #END
 #END
#END
    SELF := le;
END;

outputFile := PROJECT(inputFile, %tra%(LEFT));

endmacro;


l  :=

RECORD
            INTEGER i;
            STRING10 s;
END;

d := dataset([{1,'AB ~D'},{2,'ABC'},{3,'AB .C'}],l);

CleanFields(d,ou)
ouf := ou(i != 0);
CleanFields(ouf,ou2)

output(ou2)

