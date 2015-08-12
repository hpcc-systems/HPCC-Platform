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


namesRecord :=
            RECORD
string      surname;
string      forename;
ebcdic string eforename;
varstring   vsurname;
            END;

namesTable := dataset('x',namesRecord,FLAT);


namesRecord2 :=
            RECORD
string          fullname := namesTable.forename + ' ' + namesTable.surname;
string          shortname := namesTable.forename[1] + '.' + namesTable.surname;
string          forename := namesTable.eforename;
varstring       vfullname := namesTable.vsurname + ', ' + namesTable.forename;
varstring       vsurname := namesTable.surname;
            END;

output(table(namesTable,namesRecord2),,'out.d00');
