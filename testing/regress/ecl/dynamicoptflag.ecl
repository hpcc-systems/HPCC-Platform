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

import $.setup;
prefix := setup.Files(false, false).QueryFilePrefix;

#onwarning (4522, ignore);
#onwarning (4523, ignore);
#onwarning(10140, ignore);

d := dataset(DYNAMIC(prefix + 'no_such_file'), {string10 f}, FLAT, OPT);
output(d);
count(d);
output(d(f='NOT'));
count(d(f='NOT'));

p := PRELOAD(dataset(DYNAMIC(prefix + 'no_such_file_either'), {string10 f}, FLAT, OPT));
output(p);
count(p);
output(p(f='NOT'));
count(p(f='NOT'));

p2 := PRELOAD(dataset(DYNAMIC(prefix + 'no_such_file_again'), {string10 f}, FLAT, OPT), 2);
output(p2);
count(p2);
output(p2(f='NOT'));
count(p2(f='NOT'));

i := INDEX(d,{f},{ unsigned dummyFpos := 0 },DYNAMIC(prefix + 'nor_this'), OPT);
output(i);
count(i);
output(i(f='NOT'));
count(i(f='NOT'));
MAX(i(f>'NOT'), f);

j := JOIN(d, i, KEYED(LEFT.f = right.f));
output(j);

j1 := JOIN(d(f='NOT'), d, KEYED(LEFT.f = right.f), KEYED(i));
output(j1);

output(FETCH(d, i(f='not'), 0));
