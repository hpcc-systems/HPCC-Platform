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
namesTableSrc := dataset([
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',30},
        {'Smithe','Pru',10},
        {'X','Z'}], namesRecord);

output(namesTableSrc,,'~namesTable',overwrite);

namesTable := dataset('~namesTable',namesRecord,FLAT);
znamesTable := dataset('~namesTable',namesRecord,FLAT);

sortedNames :=  sort(namesTable, surname);
groupedNames := group(sortedNames, surname);

r := record
agesum := sum(zNamesTable, (decimal20_2)age);
    end;

output(namesTable, {sum(group, (decimal20_2)age); });
output(namesTable, r);

