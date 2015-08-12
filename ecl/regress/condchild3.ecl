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


//used in AND conditions => second use is guarded
inRec t1(inRec l) := TRANSFORM
    t1 := outofline(l, 1) > 10;
    t2 := outofline(l, 2) < 12;
    SELF.f1 := IF(t1 AND t2, 10, 12);
    SELF := l;
END;

//used in OR conditions => second use is guarded
inRec t2(inRec l) := TRANSFORM
    t1 := outofline(l, 1) > 10;
    t2 := outofline(l, 2) < 12;
    SELF.f1 := IF(t1 OR t2, 10, 12);
    SELF := l;
END;

//used in OR conditions => second use is guarded
inRec t3(inRec l) := TRANSFORM
    t1 := outofline(l, 1) > 10;
    t2 := outofline(l, 2) < 12;
    t3 := outofline(l, 3) < 8;
    SELF.f1 := IF((t1 AND t2) OR t3, 10, 12);
    SELF := l;
END;

//used in WHICH conditions => second use is guarded
inRec t4(inRec l) := TRANSFORM
    t1 := outofline(l, 1) > 10;
    t2 := outofline(l, 2) < 12;
    t3 := outofline(l, 3) < 8;
    SELF.f1 := WHICH(t1, t2, t2);
    SELF := l;
END;

//used in WHICH conditions => second use is guarded
inRec t5(inRec l) := TRANSFORM
    t1 := outofline(l, 1) > 10;
    t2 := outofline(l, 2) < 12;
    t3 := outofline(l, 3) < 8;
    SELF.f1 := REJECTED(t1, t2, t2);
    SELF := l;
END;

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
);
