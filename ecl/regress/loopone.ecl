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

namesTable := dataset([
        {'Hawthorn','Gavin',40},
        {'Hawthorn','Mia',30},
        {'Smithe','Pru',20},
        {'X','Z',10}], namesRecord);

//case 5: a row filter and a global condition
output(loop(namesTable, left.age < 40, count(rows(left)) > 1, project(rows(left), transform(namesRecord, self.age := left.age+3; self := left))));

output(loop(namesTable, left.age < 15, count(rows(left)) > 1, project(rows(left), transform(namesRecord, self.age := left.age+3; self := left))));

//The following are illegal - If they were allowedCOUNTER would need to be mapped for the filter condition.
//output(loop(namesTable, left.age < 40 - COUNTER, count(rows(left)) > 1, project(rows(left), transform(namesRecord, self.age := left.age+3; self := left))));

//output(loop(namesTable, left.age < 15 - COUNTER, count(rows(left)) > 1, project(rows(left), transform(namesRecord, self.age := left.age+3; self := left))));
