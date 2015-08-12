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

#option ('globalFold', false);
addressRecord := RECORD
string20        street;
integer4        num;
            END;

namesRecord := RECORD
string20        surname;
string10        forename;
integer2        age := 25;
addressRecord   a;
            END;


namesTable := dataset('x',namesRecord,FLAT);
emptyTable := namesTable(false);

output(emptyTable);
output(GROUP(emptyTable));
OUTPUT(SORT(emptyTable, surname));
OUTPUT(SORTED(emptyTable, surname));
OUTPUT(DISTRIBUTE(emptyTable, HASH(surname)));
OUTPUT(DISTRIBUTED(emptyTable, HASH(surname)));
OUTPUT(DEDUP(emptyTable, surname));
OUTPUT(CHOOSEN(emptyTable, 10));
OUTPUT(ENTH(emptyTable, 10));
OUTPUT(SAMPLE(emptyTable, 10, 1));
//OUTPUT(namesTable(WITHIN emptyTable));

OUTPUT(namesTable + emptyTable);
OUTPUT(emptyTable + namesTable);
OUTPUT(emptyTable + emptyTable);

myRecord := emptyTable[1];
output(namesTable(myRecord.surname[1] = 'A'));
output(namesTable(myRecord.a.street = 'Redriff Road'));
output(namesTable(emptyTable.a.num > 25));

// what does this mean - can I optimise further?
//output(person,{ false });
//output(person,{ WITHIN household(false) });

EVALUATE(emptyTable[1], 10);


// fold to zero
COUNT(emptyTable);
MAX(emptyTable, age);
MIN(emptyTable, age);
SUM(emptyTable, age);
AVE(emptyTable, age);

// fold to false
EXISTS(emptyTable);
//WITHIN(emptyTable);

// fold to true
NOT EXISTS(emptyTable);
//NOT WITHIN(emptyTable);

