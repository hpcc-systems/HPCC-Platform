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

//BUG: #13964 IF() needs to inherit the interesction of dataset information, not just 1st.

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

someCond := false : stored('someCond');

ds1 := dataset('x',namesRecord,FLAT);

ds2a := sort(ds1, surname, forename);
ds2 := ds2a(forename != '');

x := if(someCond, ds2, ds1);
y := sort(x, surname, forename);
output(y);


zds1 := dataset('x',namesRecord,FLAT);
zds2a := distribute(zds1, hash(surname));
zds2 := dedup(zds2a, forename, local);

zx := if(someCond, zds2, zds1);
zy := distribute(zx, hash(surname));
output(zy);
