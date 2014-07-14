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

dedup1 := DEDUP(ds, id1, ALL);
dedup2 := DEDUP(ds, id2, ALL);
dedup4 := DEDUP(GROUP(ds,id1), id2, ALL); // should be within the grouping
dedup5 := DEDUP(GROUP(ds,id1,id2), id1, ALL); // should be within the grouping

sequential(
    output(SORT(TABLE(dedup1, { id1, cnt := count(GROUP) }, id1),id1));
    output(SORT(TABLE(group(nofold(dedup2)),{id2, cnt := count(GROUP)}, id2), id2));
    output(SORT(TABLE(group(nofold(dedup4)), { id1, id2, cnt := count(GROUP) }, id1, id2), id1, id2));
    output(SORT(TABLE(dedup5,{id1, id2, cnt := count(GROUP)}), id1, id2));
);
    