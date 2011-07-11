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
string20        a;
string10        b;
integer2        c := 25;
integer2        d:= 25;
integer2        e:= 25;
integer2        f:= 25;
            END;

t1 := dataset('x',namesRecord,FLAT);

t2 := distribute(t1, hash(e));

t3 := sort(t2, a, b, c, d, local);

t4 := group(t3, a, b, c, local);

t5 := dedup(t4, d, f);

t6 := table(t5, {a, b, c, count(group)}, a, b, c, local);

output(t6,,'out.d00');


