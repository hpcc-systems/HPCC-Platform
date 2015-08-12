/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
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


//both uses are unconditional
inRec t1(inRec l) := TRANSFORM
    SELF.f1 := outofline(l, 1);
    SELF.f2 := outofline(l, 1);
    SELF := l;
END;

//One use is conditional, the other is unconditional
inRec t2(inRec l) := TRANSFORM
    SELF.f1 := outofline(l, 1);
    SELF.f2 := IF(l.f1 > 2, outofline(l, 1), 12);
    SELF := l;
END;

//Conditional comes first
inRec t3(inRec l) := TRANSFORM
    SELF.f1 := IF(l.f1 > 2, outofline(l, 1), 12);
    SELF.f2 := outofline(l, 1);
    SELF := l;
END;


//Both uses are conditional - on the same condition
inRec t4(inRec l) := TRANSFORM
    SELF.f1 := IF(l.f1 > 2, outofline(l, 1), 12);
    SELF.f2 := IF(l.f1 > 2, outofline(l, 1), 13);
    SELF := l;
END;

//Both uses are conditional - different conditions
inRec t5(inRec l) := TRANSFORM
    SELF.f1 := IF(l.f1 > 2, outofline(l, 1), 12);
    SELF.f2 := IF(l.f1 > 3, outofline(l, 1), 13);
    SELF := l;
END;

//Both uses are conditional - same condition, but not inside a statement
inRec t6(inRec l) := TRANSFORM
    SELF.f1 := 5 + IF(l.f1 > 2, outofline(l, 1), 12);
    SELF.f2 := 10 + IF(l.f1 > 3, outofline(l, 1), 13);
    SELF := l;
END;

//Both uses are conditional - inside same and different conditions
inRec t7(inRec l) := TRANSFORM
    SELF.f1 := IF(l.f2 > 8, IF(l.f1 > 2, outofline(l, 1), 12), 9);
    SELF.f2 := IF(l.f2 > 8, IF(l.f1 > 3, outofline(l, 1), 13), 10);
    SELF := l;
END;


//Both uses are conditional - on the same condition, different out of lines
inRec t8(inRec l) := TRANSFORM
    SELF.f1 := IF(l.f1 > 2, outofline(l, 1), 12);
    SELF.f2 := IF(l.f1 > 2, outofline(l, 2), 13);
    SELF := l;
END;

#if (0)
    output(project(nofold(input), t7(LEFT)));
#else
SEQUENTIAL(
    output(project(nofold(input), t1(LEFT)));
    output(project(nofold(input), t2(LEFT)));
    output(project(nofold(input), t3(LEFT)));
    output(project(nofold(input), t4(LEFT)));
    output(project(nofold(input), t5(LEFT)));
    output(project(nofold(input), t6(LEFT)));
    output(project(nofold(input), t7(LEFT)));
    output(project(nofold(input), t8(LEFT)));
);

#end
