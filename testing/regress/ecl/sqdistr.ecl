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

import $.setup.sq;


ds := sq.SimplePersonBookDs;

distr := distribute(ds, hash32(surname));

sdistr := nofold(sort(nofold(distr), surname, local));

g := group(sdistr, surname);

check1 := distributed(NOFOLD(g), hash32(surname));
check2 := distributed(NOFOLD(g), hash32(surname), assert);
s1 := sort(nofold(check1), surname);
s2 := sort(nofold(check2), surname);
t1 := TABLE(nofold(s1)(aage != 0), { surname, cnt := count(group) });
t2 := TABLE(nofold(s2)(aage != 0), { surname, cnt := count(group) });

sequential(
output(sort(t1, surname));
output(sort(t2, surname));
);
