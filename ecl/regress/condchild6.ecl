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


//complex condition on two branches
inRec t1(inRec l) := TRANSFORM
    cond1 := NOFOLD(CHOOSE(sum(l.ids,id), 'a', 'b', 'c', 'd', 'z', 'e')) != 'd';
    cond2 := NOFOLD(CHOOSE(max(l.ids,id), 'a', 'b', 'c', 'd', 'z', 'e')) != 'd';
    SELF.f1 := IF(cond1, outofline(l, 1), 1000);
    SELF.f2 := IF(cond2, outofline(l, 1), 1001);
    SELF := l;
END;

SEQUENTIAL(
    output(project(nofold(input), t1(LEFT)));
);
