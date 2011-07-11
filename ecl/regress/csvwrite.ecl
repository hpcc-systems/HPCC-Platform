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



allRecord := record
boolean b;
little_endian integer2  li2;
little_endian integer3  li3;
little_endian integer li;
big_endian integer2 bi2;
big_endian integer3 bi3;
big_endian integer  bi;
little_endian unsigned2 lu2;
little_endian unsigned3 lu3;
little_endian unsigned lu;
big_endian unsigned2    bu2;
big_endian unsigned3    bu3;
big_endian unsigned bu;
real4   r4;
real8 r8;
string10        s10;
ebcdic string10 es10;
varstring8      vs8;
data16          d16;
qstring         qs;
utf8            uf8;
unicode20       u20;
varunicode19    vu19;
decimal8_2      d82;
udecimal10_3    ud10_3;
end;

d := DATASET('d', allRecord , FLAT);

output(d,,'o1',csv);
output(d,,'o2',xml);
