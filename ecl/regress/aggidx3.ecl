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
output(table(sqNamesIndex1, { surname, sum(group, aage) }, surname, few));

//Filtered disk aggregate, which also requires a beenProcessed flag
output(table(sqNamesIndex2(surname != 'Hawthorn'), { max(group, aage), surname }, surname, few));

//Special case count.
output(table(sqNamesIndex3(forename = 'Gavin'), { count(group), surname }, surname, few));

//Sub query needs serializing or repeating....

// A simple inline subquery
output(table(sqNamesIndex4, { count(books), sumage := sum(group, aage) }, count(books), few));

//A not-so-simple out of line subquery
secondBookName := (string20)sort(sqNamesIndex5.books, name)[2].name;
output(table(sqNamesIndex5, { secondBookName, sumage := sum(group, aage) }, secondBookName, few));

//Access to file position - ensure it is serialized correctly!
output(table(sqNamesIndex6, { filepos DIV 8192, sumage := sum(group, aage) }, filepos DIV 8192, few));

// An out of line subquery (caused problems accessing parent inside sort criteria
output(table(sqNamesIndex7, { count(books(id != 0)), sumage := sum(group, aage) }, count(books(id != 0)), few));
