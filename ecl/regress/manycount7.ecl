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
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',30},
        {'Smithe','Pru',10},
        {'Smithe','Pru',10},
        {'Smithe','Pru',10},
        {'Smithe','Pru',10},
        {'X','Z'}], namesRecord);

x := table(namesTable, {unsigned4 c1 := count(group,age=10), unsigned4 c2 := count(group,age>30), unsigned4 c3 := count(group,age>20), string m1 := max(group, forename); });
y := x[1] : stored('counts');

y.c1;
y.c2;
y.c3;

