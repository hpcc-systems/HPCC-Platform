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

#option ('foldAssign', false);
#option ('globalFold', false);

export mydataset := DATASET('household', RECORD
            unsigned integer4 person_id,
            unsigned integer4 household_id,
            unsigned integer8 holepos
            END, THOR);

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);


NOT NOT true;
output(mydataset((real)person_id > 10.1));

// remove CHOOSEN/ENTH/SAMPLE
OUTPUT(CHOOSEN(namesTable, ALL));
OUTPUT(ENTH(namesTable, 1));
OUTPUT(SAMPLE(namesTable, 100, 1));

// fold to COUNT(namesTable) * 10
//SUM(namesTable, 10);             // remove as it generates incorrect hole code

// fold to constant
MAX(namesTable, 10);
MIN(namesTable, 10);
EVALUATE(namesTable[1], 10);
AVE(namesTable, 10);


count(namesTable(age in [1,2] and not age in [1,2]));
count(namesTable(age in [1,2] or not age in [1,2]));

[1,2,3][3];
