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
string20         r1f2;
            END;

r2 :=        RECORD
unsigned4        r2f1;
r1               r2f2;
decimal10_2      r2f3;
            END;

r3 :=        RECORD
unsigned4        r3f1;
r2               r3f2;
r2               r3f3;
                ifblock((self.r3f3.r2f2.r1f1 & 1) = 0)
r2                f4;
                end;
            END;

d := DATASET('d3', r2, FLAT);

r3 t(r2 l) :=
    TRANSFORM
        self.r3f2 := l;
        self := [];
    END;

o := PROJECT(d,t(LEFT));
output(o,,'o.d00');



