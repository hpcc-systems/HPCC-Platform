/*##############################################################################

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

