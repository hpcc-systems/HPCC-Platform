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

namesTableX := dataset('x',namesRecord,FLAT);
namesTableY := dataset('y',namesRecord,FLAT);


c1 := false : stored('c1');
c2 := false : stored('c2');
c3 := false : stored('c3');
c4 := false : stored('c4');
c5 := false : stored('c5');
c6 := false : stored('c6');
c7 := false : stored('c7');
c8 := false : stored('c8');

ds1 := namesTableX(age > 1);
ds2:= namesTableY(age > 2);

i1 := if(c1, ds1, ds2);
i2 := if(c2, i1, ds2);
output(i2);

ds3 := namesTableX(age > 3);
ds4:= namesTableY(age > 4);

i3 := if(c3, ds3, ds4);
i4 := if(c4, i3, ds3);
output(i4);

ds5 := namesTableX(age > 5);
ds6:= namesTableY(age > 6);

i5 := if(c5, ds5, ds6);
i6 := if(c6, ds6, i5);
output(i6);

ds7 := namesTableX(age > 7);
ds8:= namesTableY(age > 8);

i7 := if(c7, ds7, ds8);
i8 := if(c8, ds7, i7);
output(i8);

