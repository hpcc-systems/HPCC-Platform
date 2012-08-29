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

//UseStandardFiles
//nothor
#option ('optimizeDiskSource',true)
#option ('optimizeChildSource',true)
#option ('pickBestEngine', false)
#option ('newChildQueries', true)
#option ('hoistResourced', true)

ded := dedup(sqHousePersonBookDs.persons, forename);
cnt := table(ded, { cnt := count(group); })[1].cnt;

cond := if (cnt > 2, sort(ded[2..99], forename), sort(sqHousePersonBookDs.persons, surname, -forename));

p := table(sqHousePersonBookDs, { id, dataset children := cond });
output(p);
