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

unsigned numRows := 1000;

ids := dataset(numRows, transform({unsigned id}, SELF.id := COUNTER), DISTRIBUTED);

//Check that all the nodes are duplicated
d1 := distribute(ids, ALL);
o1 := output(count(d1) - numRows * CLUSTERSIZE);

//Check combinations of distributes are not combined
d2 := distribute(d1, ALL);
o2 := output(count(d1) - numRows * CLUSTERSIZE * CLUSTERSIZE);

//Check that distribute does not remove distribute,set
d3a := distribute(d1, hash(id));
d3b := DISTRIBUTED(NOFOLD(d3a), hash(id), ASSERT);
o3 := output(count(d3b) - numRows * CLUSTERSIZE);

//Distribute, ALL can removeCheck that distribute does not remove distribute,set
d4a := distribute(ids, hash(id));
d4b := DISTRIBUTE(d4a, ALL);
o4 := output(count(d4b) - numRows * CLUSTERSIZE);

//Distribute, ALL on an empty dataset is an empty dataset
d5a := distribute(ids(false), ALL);
o5 := output(count(d5a));

SEQUENTIAL(
    o1,
    o2,
    o3,
    o4,
    o5,
    );
