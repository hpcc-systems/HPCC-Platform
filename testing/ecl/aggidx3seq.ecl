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
secondBookName := (string20)sort(sqNamesIndex5.books, name)[2].name;

sequential(
//Simple disk aggregate
output(sort(table(sqNamesIndex1, { string surname := trim(sqNamesIndex1.surname), sum(group, aage) }, trim(surname), few), surname)),


//Filtered disk aggregate, which also requires a beenProcessed flag
output(sort(table(sqNamesIndex2(surname != 'Halliday'), { max(group, aage), surname }, surname, few), surname)),

//Special case count.
output(sort(table(sqNamesIndex3(forename = 'Gavin'), { count(group), surname }, surname, few), surname)),

//Sub query needs serializing or repeating....

// A simple inline subquery
output(sort(table(sqNamesIndex4, { cntBooks:= count(books), sumage := sum(group, aage) }, count(books), few), cntBooks, sumage)),

output(sort(table(sqNamesIndex5, { secondBookName, sumage := sum(group, aage) }, secondBookName, few), secondbookname)),

//Access to file position - ensure it is serialized correctly!
output(sort(table(sqNamesIndex6, { pos := filepos DIV 8192, sumage := sum(group, aage) }, filepos DIV 8192, few), pos, sumage)),

// An out of line subquery (caused problems accessing parent inside sort criteria
output(sort(table(sqNamesIndex7, { numBooks := count(books(id != 0)), sumage := sum(group, aage) }, count(books(id != 0)), few), numbooks, sumage))
);
