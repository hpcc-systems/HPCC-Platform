/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

//Some of these tests need to be verified by looking at the generate code
//MORE: Remove these lines when the code has been implemented in the engines.
//nothor
//noroxie
//nohthor

//MORE: Include a much larger scale version of this test in the performance suite.

unsigned numRows := 1000;

ids := dataset(numRows, transform({unsigned id}, SELF.id := COUNTER), DISTRIBUTED);
//Same ids, opposite nodes
ids2 := dataset(numRows, transform({unsigned id}, SELF.id := numRows - (COUNTER-1)), DISTRIBUTED);

//Same ids, ids duplicated twice
ids3 := dataset(numRows*2, transform({unsigned id}, SELF.id := 1 + (numRows - COUNTER) % numRows), DISTRIBUTED);

j1 := JOIN(ids, ids2, LEFT.id = RIGHT.id, LOOKUP);
o1 := output(count(j1) - numRows);

//Check that distribute all followed by a local join produces the same result
d2 := distribute(ids2, ALL);
j2 := JOIN(ids, d2, LEFT.id = RIGHT.id, LOCAL, LOOKUP);
o2 := output(count(j2) - numRows);

j3 := JOIN(ids, ids3, LEFT.id = RIGHT.id, LOOKUP, MANY);
o3 := output(count(j3) - numRows*2);

//Check that distribute all followed by a local join produces the same result
d4 := distribute(ids3, ALL);
j4 := JOIN(ids, d4, LEFT.id = RIGHT.id, LOCAL, LOOKUP, MANY);
o4 := output(count(j4) - numRows*2);

//Now check if multiple local lookup joins are resourced into the same graph
j5a := JOIN(j4, d4, LEFT.id = RIGHT.id, LOCAL, LOOKUP, MANY);
j5b := JOIN(j5a, d4, LEFT.id = RIGHT.id, LOCAL, LOOKUP, MANY);
o5 := output(count(j5b) - numRows*2*2*2);

SEQUENTIAL(
    o1,
    o2,
    o3,
    o4,
    o5,
    );
