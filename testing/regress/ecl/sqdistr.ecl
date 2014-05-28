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

import $.setup;
sq := setup.sq('hthor');

ds := sq.SimplePersonBookDs;

distr := distribute(ds, hash32(surname));

sdistr := nofold(sort(nofold(distr), surname, local));

//Grouped aggregate
g := group(sdistr, surname);
s1 := sort(nofold(g), surname); // grouped
t1 := TABLE(nofold(s1)(aage != 0), { surname, cnt := count(group) });   // grouped
o1 := output(sort(t1, surname));

//Distributed loses the current grouping...
d2 := distributed(NOFOLD(g), hash32(surname));
t2 := TABLE(nofold(d2)(aage != 0), { cnt := count(group) });
o2 := output(sort(t2, cnt));

//Distributed loses the current grouping...
d3 := distributed(NOFOLD(g), hash32(surname), assert);
t3 := TABLE(nofold(d3)(aage != 0), { cnt := count(group) });
o3 := output(sort(t3, cnt));

sequential(o1, o2, o3);
