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



