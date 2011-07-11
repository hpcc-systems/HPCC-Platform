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

// basic type
n1 := sizeof(integer8);
n1x := sizeof(unsigned integer8);
n2 := sizeof(boolean);
n2x := sizeof(ebcdic string10);

// basic type variable
integer4 i := 3;
n3 := sizeof(i);
boolean b := true;
n4 := sizeof(b);

// expr
m1 := sizeof('abc');
m2 := sizeof(123);
m3 := sizeof(true);
m4 := sizeof('abcd'+'def');
m5 := sizeof(1+3000);
m6 := sizeof((integer8)23);

// dataset

rec := record
   boolean bx;
   string3 sx;
   integer2 ix;
end;

ds := dataset('ds', rec, flat);

n5 := sizeof(ds);
n6 := sizeof(rec);
n7 := sizeof(ds.bx);

n := n1+n1x+n2+n2x+n3+n4+n4+n5+n6+n7;

n;
m1 + m2 + m3 + m4 + m5 + m6;
