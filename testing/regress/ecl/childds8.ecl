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

//This is needed to prevent cntBad being hoisted before the resourcing, and becoming unconditional
//The tests are part of the work aiming to remove this code.
#option ('workunitTemporaries', false);

idRec := { unsigned id; };
mainRec := { unsigned seq, dataset(idRec) ids };

idRec createId(unsigned id) := TRANSFORM
    SELF.id := id;
END;

mainRec createMain(unsigned c, unsigned num) := TRANSFORM
    SELF.seq := c;
    SELF.ids := DATASET(num, createId(c DIV 2 + (COUNTER-1)));
END;

emptyIds := DATASET([], idRec);

ds := NOFOLD(DATASET(4, createMain(COUNTER, 3)));

ds1 := SORT(ds, ids);
ds2 := DEDUP(ds1, ids);
ds3 := ds2(ids != emptyIds);
output(ds3);
ds4 := ROLLUP(ds1, TRANSFORM(mainRec, SELF.ids := LEFT.ids + RIGHT.ids, SELF := LEFT), ids);
output(ds4);
