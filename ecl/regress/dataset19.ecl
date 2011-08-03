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


namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

d := dataset([{'Hawthorn','Gavin',35},
              {'Hawthorn','Abigail',2},
              {'Smith','John',57},
              {'Smith','Gavin',12}
              ], namesRecord);


namesRecordEx := record
namesRecord;
unsigned8           filepos{virtual(fileposition)};
                end;

namesTable := dataset('names',namesRecordEx,FLAT);
i := index(namesTable, { namesTable }, 'nameIndex');

output(d,,'names');
buildindex(i);

f := namesTable(keyed(age != 0));

t := table(f, { surname, forename, age, seq := random() % 100});

p := project(t, transform(recordof(t), self.seq := left.seq*10, self := left;));

output(p);
