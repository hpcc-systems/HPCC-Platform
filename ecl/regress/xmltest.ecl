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

r1 :=        RECORD
unsigned4        r1f1;
            END;

r2 :=        RECORD
unsigned4        r2f1;
r1               r2f2;
            END;



tableFormat := record
integer2    i2;
integer8    i8;
unsigned2   u2;
unsigned8   u8;
            ifblock(self.i2 & 10 != 0)
string10        s20;
varstring10     v20;
qstring10       q20;
data30          d30;
            end;
string      sx;
real4       r4;
real8       r8;
decimal10_2     d10_2;
udecimal10_2    ud10_2;
bitfield3   b3;
r2          nested;
        end;


d := DATASET('table', tableFormat, FLAT);

output(d);
