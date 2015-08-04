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

#option ('childQueries', true);
#option ('optimizeDiskSource',true);
#option ('optimizeChildSource',true);
#option ('newChildQueries', true);
#option ('targetClusterType', 'roxie');
#option ('hoistResourced', true);
#option ('commonUpChildGraphs', true);

import sq;
sq.DeclareCommon();


//Items should be combined....
ded := dedup(sqHousePersonBookDs.persons, forename);
p := table(sqHousePersonBookDs, { id, dataset children := ded(aage < 18), dataset adults := ded(aage >= 18) });
output(p);


//Items within projects should be combined, but not moved over the filter
p1 := table(sqHousePersonBookDs, { id, dataset persons := persons, dataset children := ded(aage < 18), dataset adults := ded(aage >= 18), t1 := random() });
ded2 := dedup(p1.persons, forename);
p2 := table(p1(t1*5 != 1), { id, dataset children := ded2(aage < 18), dataset adults := ded2(aage >= 18), dataset children2 := children; dataset adults2 := adults });
output(p2);
