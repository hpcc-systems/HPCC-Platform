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

namesTable1 := dataset([
        {'Hawthorn','Gavin',31},
        {'X','Z'}], namesRecord);

o1 := output(namesTable1,,'~names',overwrite);


namesTable2 := dataset([
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',31},
        {'Hawthorn','Robert',0},
        {'X','Z'}], namesRecord);

o2 := output(namesTable2,,'~names',overwrite);



namesTable := dataset('~names',namesRecord,FLAT);

p1 := namesTable(age <> 0) : persist('p1');
p2 := namesTable(age <> 10) : persist('p2');



boolean use1 := true : stored('use1');

if (use1,
    sequential(o1, output(count(p1))),
    sequential(o2, output(count(p2)))
    );


/*
Expected:

1:  stored
2:  persist p1
3:  persist p2
4:  normal              output o1
5:  normal [2]          output count
6:  sequential [4,5]
7:  normal              output o2
8:  normal [3]          output count
9:  sequential [7,8]
10: normal [1]          use1
11: conditional [10, 6, 9]

*/
