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

#option ('targetClusterType', 'thorlcr');

level1Rec := RECORD
    UNSIGNED a;
    UNSIGNED b;
    UNSIGNED c;
END;

level2Rec := RECORD
    unsigned id;
    level1Rec a;
END;

ds1 := DATASET('ds1', level2Rec, thor);
ds2 := DATASET('ds2', level1Rec, thor);

j := JOIN(ds1, ds2, LEFT.id = right.a, transform(level2Rec, self.a := RIGHT; SELF := LEFT));

d := DEDUP(j, a.b, a.c);

c := table(d, { d, cnt := count(group) },  a.b);

xRec := record
    unsigned id;
    unsigned a;
END;

ds3 := dataset('ds3', xRec, thor);

j2 := JOIN(ds3, c(cnt=1), left.id=right.a.b, transform(xRec, SELF.a := RIGHT.a.c; SELF := LEFT));
output(j2);

