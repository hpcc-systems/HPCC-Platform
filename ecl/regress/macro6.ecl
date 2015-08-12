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
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);

glue(a) := macro
    a
    endmacro;

extractFields(ds, outDs, f1, f2='?', f3='?', f4='?') := macro

#uniquename(r)

%r% := record
glue('Gavin'+#text(f1)) := ds.f1;
#if (#TEXT(f2)<>'?')
#TEXT(f2)+':';
f2  := ds.f2;
#end
    end;

outDs := table(ds, %r%);
endmacro;

extractFields(namesTable, justSurname, surname);
output(justSurname);


extractFields(namesTable, justName, surname, forename);
output(justName);
