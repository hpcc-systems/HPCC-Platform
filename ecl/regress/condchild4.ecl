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


//CHOOSE
inRec t1(inRec l) := TRANSFORM
    t1 := outofline(l, 1);
    t2 := outofline(l, 2);
    SELF.f1 := CHOOSE(l.f1, t1, t2, 0);
    SELF := l;
END;

//CASE
inRec t2(inRec l) := TRANSFORM
    t1 := outofline(l, 1);
    t2 := outofline(l, 2);
    t3 := outofline(l, 3);
    t4 := outofline(l, 4);
    SELF.f1 := CASE(l.f1, 5=>t1, 12=>t2, 27=>t3, t4);
    SELF := l;
END;

//CASE, also in case conditions
inRec t3(inRec l) := TRANSFORM
    t1 := outofline(l, 1);
    t2 := outofline(l, 2);
    t3 := outofline(l, 3);
    t4 := outofline(l, 4);
    SELF.f1 := CASE(l.f1, 5=>t1, t2=>t3, t4);
    SELF := l;
END;

//CASE, value used in all results
inRec t4(inRec l) := TRANSFORM
    t1 := outofline(l, 1);
    t2 := outofline(l, 2);
    t3 := outofline(l, 3);
    t4 := outofline(l, 4);
    SELF.f1 := CASE(l.f1, 5=>t1*5, t2=>t1*6, t3=>t1*8, t1*10);
    SELF := l;
END;

//use of t1s actually unconditional (tricky to spot?)
inRec t5(inRec l) := TRANSFORM
    t1 := outofline(l, 1);
    t2 := outofline(l, 2);
    t3 := outofline(l, 3);
    t4 := outofline(l, 4);
    SELF.f1 := CASE(l.f1, 5=>t1, t1*12=>t2, 27=>t3, t4);
    SELF := l;
END;

//Some case uses are also conditional, some unconditional
inRec t6(inRec l) := TRANSFORM
    t1 := outofline(l, 1);
    t2 := outofline(l, 2);
    t3 := outofline(l, 3);
    t4 := outofline(l, 4);
    SELF.f1 := CASE(l.f1, 5=>t1, 12=>t2, 27=>t3, t4);
    SELF.f2 := t1 * IF(l.f2> 10, t2, t3);
    SELF := l;
END;

//make cond5 same as this, but using map.
//MORE: CHOOSE
//MORE: MAP/CASE
//MORE: Some in REJECTED, some conditional
//MORE: Some in REJECTED, some unconditional

SEQUENTIAL(
    output(project(nofold(input), t1(LEFT)));
    output(project(nofold(input), t2(LEFT)));
    output(project(nofold(input), t3(LEFT)));
    output(project(nofold(input), t4(LEFT)));
    output(project(nofold(input), t5(LEFT)));
    output(project(nofold(input), t6(LEFT)));
);
