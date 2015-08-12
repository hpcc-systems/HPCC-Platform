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
