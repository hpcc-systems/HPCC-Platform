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


idRec := { UNSIGNED id; };

inRec := RECORD
    UNSIGNED seq;
    DATASET(idRec) ids;
    UNSIGNED f1;
    UNSIGNED f2;
END;

input := DATASET([
       {1, [1,2,2,3,3,3,4,4,4,4,5,5,5,5], 1, 2},
       {2, [5,4,4,3,3,3,2,2,2,2,1,1,1,1], 2, 3}
       ], inRec);

outOfLine(inRec l, unsigned x) := FUNCTION
    filtered := l.ids(id != x);
    d := dedup(filtered, id);
    RETURN COUNT(d);
END;


//guarded candidate is also used in the guard condition
inRec t1(inRec l) := TRANSFORM
    SELF.f1 := IF(l.f1 = 10 or outofline(l, 1) = 2, 10, outofline(l, 1));
    SELF := l;
END;

//it is impossible to guard b and c since they are mutually dependent
inRec t2(inRec l) := TRANSFORM
    a := outofline(l, 1) = 1;
    b := outofline(l, 2) = 1;
    c := outofline(l, 3) = 1;
    d := outofline(l, 4) = 1;
    SELF.f1 := IF((a OR b OR c) AND (a OR c or b), outofline(l, 5), outofline(l, 6));
    SELF := l;
END;

SEQUENTIAL(
    output(project(nofold(input), t2(LEFT)));
);
