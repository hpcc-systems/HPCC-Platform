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
big_endian integer2     age := 25;
integer2        age2 := 25;
integer8        age8 := 25;
real8           salary := 0;
ebcdic string20 surname;
string10        forename;
unsigned8       filepos{virtual(fileposition)}
            END;

namesTable := dataset('x',namesRecord,FLAT);
namesTable2 := dataset('xx.zzz',namesRecord,FLAT);
nt2 := group(namesTable, age2);
parallel(
BUILDINDEX(nt2, { surname, forename, filepos }, 'name.idx'),
BUILDINDEX(nt2, { age, age2, age8, filepos }, 'age.idx', dataset(namesTable2), backup));

