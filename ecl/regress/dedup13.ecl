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

namesTable := dataset('x',namesRecord,FLAT);

x1 := dedup(namesTable, all);
y1 := dedup(x1, surname, all);
output(y1);

x2 := dedup(namesTable, surname, all);
y2 := dedup(x2, all);
output(y2);

x3 := dedup(namesTable, surname, forename);
y3 := dedup(x3, forename, surname);
output(y3);

x4 := dedup(namesTable, surname, forename, all);
y4 := dedup(x4, forename, surname, all);
output(y4);


g := group(namesTable, age);

xx1 := dedup(g, all);
yy1 := dedup(xx1, surname, all);
output(yy1);

xx2 := dedup(g, surname, all);
yy2 := dedup(xx2, all);
output(yy2);

xx3 := dedup(g, surname, forename);
yy3 := dedup(xx3, forename, surname);
output(yy3);

xx4 := dedup(g, surname, forename, all);
yy4 := dedup(xx4, forename, surname, all);
output(yy4);


//Now test subsets and supersets.