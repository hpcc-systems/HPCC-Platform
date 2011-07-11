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

LOADXML('<xml/>');

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
