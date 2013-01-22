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

//A not-so-simple out of line subquery
secondBookName := (string20)sort(sqNamesTable5.books, name)[2].name;

sequential(
//Simple disk aggregate
output(sort(table(sqNamesTable1, { surname, sumage := sum(group, aage) }, surname, few),surname)),

//Filtered disk aggregate, which also requires a beenProcessed flag
output(sort(table(sqNamesTable2(surname != 'Halliday'), { max(group, aage), surname }, surname, few),surname)),

//check literals are assigned
output(sort(table(sqNamesTable3(forename = 'Gavin'), { 'Count: ', count(group), 'Name: ', surname }, surname, few),surname)),

//Sub query needs serializing or repeating....

// A simple inline subquery
output(sort(table(sqNamesTable4, { cnt := count(books), sumage := sum(group, aage) }, count(books), few),cnt)),

output(sort(table(sqNamesTable5, { sbn := secondBookName, sumage := sum(group, aage) }, secondBookName, few),sbn)),

// An out of line subquery (caused problems accessing parent inside sort criteria
output(sort(table(sqNamesTable7, { cnt := count(books(id != 0)), sumage := sum(group, aage) }, count(books(id != 0)), few),cnt)),

//Bizarre - add a dataset that needs serialization/deserialisation to enusre cloned correctly
output(sort(table(nofold(sqNamesTable2)(surname != 'Halliday'), { max(group, aage), surname, dataset books := books }, surname, few),surname))
);

