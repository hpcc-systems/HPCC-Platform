/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

idRecord := { unsigned id; };

myDataset := DATASET(100, TRANSFORM(idRecord, SELF.id := COUNTER), DISTRIBUTED);

filtered := NOFOLD(myDataset)(id % 20 != 0);

filter1 := NOFOLD(filtered)(id % 3 != 0);

filter2 := NOFOLD(filtered)(id % 3 != 1);

p1 := PROJECT(NOFOLD(filtered), TRANSFORM(idRecord, SELF.id := LEFT.id + COUNT(filter1)));

p2 := PROJECT(NOFOLD(filtered), TRANSFORM(idRecord, SELF.id := LEFT.id + COUNT(filter2)));

boolean test := false : stored('test');

r := IF(test, NOFOLD(p1), NOFOLD(p2));

output(CHOOSEN(NOFOLD(r), 10));
