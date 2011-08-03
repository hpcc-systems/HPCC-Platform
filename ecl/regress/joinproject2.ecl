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

names2Record :=
            RECORD
string20        surname;
string10        forename;
integer2        age2 := 25;
            END;

combRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
integer2        age2 := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);
namesTable2 := dataset('y',names2Record,FLAT);


x := sort(namesTable2, surname, -forename, local);
x2 := dedup(x, surname, local);

combRecord t1(namesRecord l, names2Record r) := transform
    self := l;
    self := r;
    end;

combRecord t2(namesRecord l, names2Record r) := transform
    self.age2 := FAILCODE;
    self := r;
    self := l;
    end;

z := join(namesTable, x2, left.surname = right.surname, t1(LEFT,RIGHT), onFail(t2(left, right)), atmost(100));

z1 := project(z, transform({left.surname, left.age2 }, self.surname := left.surname; self.age2 := left.age2 ));

output(z1);
