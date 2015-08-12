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
