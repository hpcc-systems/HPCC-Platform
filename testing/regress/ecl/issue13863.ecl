/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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


r := { unsigned id1, unsigned id2 };
ro := { unsigned id, DATASET(r) child };



ro t2(ro main) := TRANSFORM

    ds := main.child;
    r t(ds l) := TRANSFORM
        oneIds := ds(id1 = 1);

        SELF.id2 := oneIds[1].id2;
        SELF := l;
    END;

    p := PROJECT(ds, t(LEFT));
    f := p(id1 != 1);

    SELF.child := f;
    SELF := main;
END;

ds(unsigned base) := DATASET(10, TRANSFORM(r, SELF.id1 := COUNTER, SELF.id2 := ((base + COUNTER) % 4)));

ds2 := DATASET(10, TRANSFORM(ro, SELF.id := COUNTER, SELF.child := ds(COUNTER)));

output(PROJECT(ds2, t2(LEFT)));
