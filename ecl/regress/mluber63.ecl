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

loadxml('<xml/>');

string zz := '' : stored('zz');

doCleanField(inputField) := macro
#EXPORTXML(doCleanFieldMetaInfo, inputField)
#IF (%'doCleanFieldText'%='')
 #DECLARE(doCleanFieldText)
#END
#SET (doCleanFieldText, false)
#FOR (doCleanFieldMetaInfo)
 #FOR (Field)
  #IF (%'@type'% = 'string')
    #SET (doCleanFieldText, 'SELF.' + %'@name'%)
    #APPEND (doCleanFieldText, ':= myCleanFunction(le.')
    #APPEND (doCleanFieldText, %'@name'%)
    #APPEND (doCleanFieldText, ')')
//  %doCleanFieldText%;
    SELF.%@name% := myCleanFunction(inputField);
  #END
 #END
#END
endmacro;

string myCleanFunction(string x) := '!' + x + '!';

r := RECORD
  unsigned id;
  string10 dg_firstname;
  string dg_lastname;
 END;

ds := dataset([{1,'A','B'},{11,'C','D'}],R);

r tra(ds le) :=
TRANSFORM
    doCleanField(le.dg_firstname)
    SELF := le;
END;


p := PROJECT(ds, tra(LEFT));
output(p);
