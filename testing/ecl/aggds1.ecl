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
#option ('optimizeIndexSource',true)
#option ('optimizeThorCounts',false)
#option ('countIndex',false)

//Check fixed size disk count correctly checks canMatchAny()
inlineDs := dataset([1,2],{integer value});

//Simple disk aggregate
output(table(sqNamesTable1, { sum(group, aage),exists(group),exists(group,aage>0),exists(group,aage>100),count(group,aage>20) })) : independent;

//Filtered disk aggregate, which also requires a beenProcessed flag
output(table(sqNamesTable2(surname != 'Halliday'), { max(group, aage) })): independent;

//Special case count.
output(table(sqNamesTable3(forename = 'Gavin'), { count(group) })): independent;

output(count(sqNamesTable4)): independent;

//Special case count.
output(table(sqNamesTable5, { count(group, (forename = 'Gavin')) })): independent;

output(table(inlineDs, { count(DG_FetchFile(inlineDs.value = 1)); })): independent;

//existance checks
output(exists(sqNamesTable4)): independent;
output(exists(sqNamesTable4(forename = 'Gavin'))): independent;
output(exists(sqNamesTable4(forename = 'Joshua'))): independent;
