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
string10        forename1;
string10        forename2;
string10        forename3;
string10        forename4;
integer2        age := 25;
            END;

namesTable := dataset('names.d00', namesrecord, THOR);
sortedNames := sort(namesTable, -age, record);
output(sortedNames,,'out.d00');


output(sort(namesTable, -age, record, except age, surname));

output(sort(namesTable, forename, forename4, record));

output(sort(namesTable, except age));
