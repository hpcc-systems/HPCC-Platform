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


r := { unsigned value1, unsigned value2 };
makeRow(unsigned v1, unsigned v2) := ROW(TRANSFORM(r, SELF.value1 := v1; SELF.value2 := v2));

x := SERVICE
       dataset(r) myFunction(dataset(r) x) : entrypoint('doesnotexist');
       integer myFunction2(dataset(r) x) : entrypoint('doesnotexist');
   END;

row1 := makeRow(1,2);

output(x.myFunction(dataset(row1)));

row2 := makeRow(5,6);

output(x.myFunction2(dataset(row2)));
