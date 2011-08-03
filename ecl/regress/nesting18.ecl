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

r3 :=        RECORD
unsigned4        r3f1;
r2               r3f2;
r2               r3f3;
                ifblock((self.r3f3.r2f2.r1f1 & 1) = 0)
r2                f4;
                end;
            END;

d := DATASET('d3', r3, FLAT);

r3 t(r3 l) :=
    TRANSFORM
        self.r3f1 := l.r3f1;
        //Completely obscure code to cover a case where replacing assigning a single row project, and replacing LEFT in the input dataset
        //will create an ambiguity - see doBuildRowAssignProject
        self.r3f2 := PROJECT(dataset(NOFOLD(l.r3f2)), transform(r2, SELF.r2f1 := LEFT.r2f2.r1f1; SELF.r2f2.r1f1 := project(nofold(dataset(nofold(L.r3f2))), transform(LEFT))[1].r2f2.r1f1))[1];
        self.r3f3.r2f2.r1f1 := l.r3f2.r2f1;
        self := l;                    // should assign f3 and d4, but not f3.f2.f1
    END;

o := PROJECT(d,t(LEFT));
output(o,,'o.d00');



