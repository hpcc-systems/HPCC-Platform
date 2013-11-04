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


idRecord :=
            RECORD
UNSIGNED        id1;
UNSIGNED        id2;
UNSIGNED        id3;
            END;

ds0 := dataset([
    {1,1,1},
    {1,1,2},
    {1,2,1},
    {2,1,1},
    {2,2,1},
    {2,3,1},
    {3,1,1},
    {99,99,99}
    ], idRecord);
ds := sorted(ds0, id1, id2);

agg1 := TABLE(ds, { id1, id2, cnt := count(group) }, id1);
agg2 := TABLE(ds, { id2, cnt := count(group) }, id2);
agg3 := TABLE(GROUP(ds,id1), { id2, cnt := count(group) }, id2);
agg4 := TABLE(GROUP(ds,id1), { id1, id2, cnt := count(group) }, id2, grouped);
agg5 := TABLE(GROUP(ds,id1,id2), { id1, cnt := count(group) }, id1);
agg6 := TABLE(GROUP(ds,id1,id2), { id1, cnt := count(group) }, id1, grouped);

sequential(
    output(agg1);
    output(sort(group(nofold(agg2)),id2));
    output(sort(group(nofold(agg3)),id2));
    output(sort(group(nofold(agg4)),id1, id2));
    output(agg5);
    output(sort(group(nofold(agg6)), id1, cnt));
);
    