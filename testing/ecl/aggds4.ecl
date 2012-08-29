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
#option ('optimizeDiskSource',true)
#option ('optimizeChildSource',true)
#option ('optimizeIndexSource',true)
#option ('optimizeThorCounts',false)
#option ('countIndex',false)

pr:= table(sqNamesTable1, { fullname := trim(surname) + ', ' + trim(forename), aage });

//Aggregate on a projected table that can't be merged
pr2:= table(sqNamesTable2, { surname, forename, aage, unsigned8 seq := (random() % 100) / 2000 + aage; });

//Filtered Aggregate on a projected table.
output(sort(table(pr(aage > 20), { aage, max(group, fullname) }, aage, few), record));

//Filtered Aggregate on a projected table.
output(sort(table(pr2(seq > 10), { surname, ave(group, aage) }, surname, few), record));
