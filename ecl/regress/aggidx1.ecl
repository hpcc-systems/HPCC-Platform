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

import AggCommon;
AggCommon.CommonDefinitions();

//Simple disk aggregate
output(table(sqNamesIndex1, { sum(group, aage),exists(group),exists(group,aage>0),exists(group,aage>100),count(group,aage>20) }));

//Filtered disk aggregate, which also requires a beenProcessed flag
output(table(sqNamesIndex2(surname != 'Hawthorn'), { max(group, aage) }));

//Special case count.
output(table(sqNamesIndex3(forename = 'Gavin'), { count(group) }));

count(sqNamesIndex4);

//Special case count.
output(table(sqNamesIndex5, { count(group, (forename = 'Gavin')) }));
