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

namesTable := distributed(dataset('x',namesRecord,FLAT),hash(forename));


qnamesRecord := 
            RECORD
qstring20       surname;
qstring10       forename;
integer2        age := 25;
            END;

qnamesTable := distributed(dataset('x',qnamesRecord,FLAT),hash(forename));


sortNames := sort(namesTable, surname,local);
qsortNames := sort(qnamesTable, surname,local);

output(join(sortNames(age=1),sortNames(age=2),LEFT.surname=RIGHT.surname,local));
output(join(qsortNames(age=1),qsortNames(age=2),LEFT.surname=RIGHT.surname,local));
output(join(qsortNames(age=1),sortNames(age=2),LEFT.surname=RIGHT.surname,local));
output(join(sortNames(age=1),qsortNames(age=2),LEFT.surname=RIGHT.surname,local));

sortNamesQStr := sort(namesTable, (qstring)surname,local);
qsortNamesStr := sort(qnamesTable, (string)surname,local);

//Some of these should match as sorted, but very uncommon, and doesn't fall out easily
output(join(sortNamesQStr(age=1),sortNamesQStr(age=2),LEFT.surname=RIGHT.surname,local));
output(join(qsortNamesStr(age=1),qsortNamesStr(age=2),LEFT.surname=RIGHT.surname,local));
output(join(qsortNamesStr(age=1),sortNamesQStr(age=2),LEFT.surname=RIGHT.surname,local));
output(join(sortNamesQStr(age=1),qsortNamesStr(age=2),LEFT.surname=RIGHT.surname,local));
