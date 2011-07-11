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
string20         r1f2;
            END;

r2 :=        RECORD
unsigned4        r2f1;
r1               r2f2;
decimal10_2      r2f3;
            END;

r3 :=        RECORD
unsigned4        r3f1;
dataset(r2)      r3f2;
r2               r3f3;
                ifblock((self.r3f3.r2f2.r1f1 & 1) = 0)
r2                f4;
set of string     strs;
                end;
            END;

d := DATASET('d3', r2, FLAT);

r3 t(r2 l) := 
    TRANSFORM
        self.r3f2 := l;
        self := [];
    END;

o := PROJECT(d,t(LEFT));
output(o,,'o.d00',csv(heading));
output(o,,'o.d00',csv(heading, terminator('$$!'),separator('|')));



