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
string20        forename;
integer2        age := 25;
            END;

namesRecord2 :=
            RECORD
string20        forename;
string20        surname;
integer2        age := 25;
            END;

x := dataset('x',namesRecord,FLAT);

dx := x(surname <> 'Hawthorn') : persist('storedx');

dy1 := dx(forename <> 'Gavin') : persist('dy1');
dy2 := dx(forename <> 'Jason') : persist('dy2');

dz1a := dy1(age < 10) : persist('dz1a');
dz1b := dy1(age > 20) : persist('dz1b');
dz2a := dy2(age < 10) : persist('dz2a');
dz2b := dy2(age > 20) : persist('dz2b','1000way');

count(dz1a) + count(dz1b) + count(dz2a) + count(dz2b);
