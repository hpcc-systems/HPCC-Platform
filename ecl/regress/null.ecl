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

