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
integer2        age := 25;
string20        surname;
string10        forename;
            END;

namesTable := dataset('x',namesRecord,FLAT);
groupedNamesTable := group(namesTable, forename);
deduped := dedup(groupedNamesTable, (string10)age,LEFT.age-RIGHT.age>10,ALL);
deduped2 := dedup(deduped, (integer4)surname);
deduped3 := dedup(deduped, LEFT.age-RIGHT.age>8);

input3 := group(deduped3);

deduped4 := dedup(input3,age,surname,ALL);

deduped5 := dedup(deduped4,age,LEFT.surname = RIGHT.surname+nofold(''));

output(deduped5,,'out.d00');
