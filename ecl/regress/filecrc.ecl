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

#option ('targetClusterType', 'hthor');

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesRecordEx :=
            RECORD(namesRecord)
unsigned8       filepos{virtual(fileposition)}
            END;


namesRecordEx2 :=
            RECORD
namesRecord;
unsigned8       filepos{virtual(fileposition)}
            END;

namesTableRaw := dataset([
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',30},
        {'Smithe','Pru',10},
        {'X','Z'}], namesRecord);

namesTable := dataset('crcNamesTable',namesRecord,FLAT);
namesTableEx := dataset('crcNamesTable',namesRecordEx,FLAT);
namesTableEx2 := dataset('crcNamesTable',namesRecordEx2,FLAT);

namesIndexEx := index(namesTableEx, { namesTableEx }, 'crcNamesIndexEx');
namesIndexEx2 := index(namesTableEx2, { namesTableEx2 }, 'crcNamesIndexEx2');

ds := dataset([0, 64, 32], { unsigned fpos });

output(namesTableRaw,,'crcNamesTable');
buildindex(namesIndexEx);
buildindex(namesIndexEx2);

output(namesTable(age != 0));
output(namesTableEx(age != 0));
output(namesTableEx2(age != 0));


output(fetch(namesTable, ds, right.fpos, transform(left)));
output(fetch(namesTableEx, ds, right.fpos, transform(left)));
output(fetch(namesTableEx2, ds, right.fpos, transform(left)));

output(join(namesTable, namesIndexEx, left.surname = right.surname));
output(join(namesTable, namesIndexEx2, left.surname = right.surname));
output(join(namesTable, namesTableEx, left.surname = right.surname, keyed(namesIndexEx)));
output(join(namesTable, namesTableEx2, left.surname = right.surname, keyed(namesIndexEx2)));
