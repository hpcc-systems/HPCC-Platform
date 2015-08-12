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


namesRecord :=
            RECORD
string20        a;
string10        b;
integer2        c := 25;
integer2        d:= 25;
integer2        e:= 25;
integer2        f:= 25;
            END;

t1 := dataset('x',namesRecord,FLAT);

t2 := distribute(t1, hash(e));

t3 := sort(t2, a, b, c, d, local);

t4 := group(t3, a, b, c, local);

t5 := dedup(t4, d, f);

t6 := table(t5, {a, b, c, count(group)}, a, b, c, local);

output(t6,,'out.d00');


