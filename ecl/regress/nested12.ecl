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

#option ('targetClusterType', 'thorlcr');

level1Rec := RECORD
    UNSIGNED a;
    UNSIGNED b;
    UNSIGNED c;
END;

level2Rec := RECORD
    unsigned id;
    level1Rec a;
END;

ds1 := DATASET('ds1', level2Rec, thor);
ds2 := DATASET('ds2', level1Rec, thor);

j := JOIN(ds1, ds2, LEFT.id = right.a, transform(level2Rec, self.a := RIGHT; SELF := LEFT));

d := DEDUP(j, a.b, a.c);

c := table(d, { d, cnt := count(group) },  a.b);

xRec := record
    unsigned id;
    unsigned a;
END;

ds3 := dataset('ds3', xRec, thor);

j2 := JOIN(ds3, c(cnt=1), left.id=right.a.b, transform(xRec, SELF.a := RIGHT.a.c; SELF := LEFT));
output(j2);

