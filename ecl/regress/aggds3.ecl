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
output(table(sqNamesTable1, { surname, sum(group, aage) }, surname, few, keyed));

//Filtered disk aggregate, which also requires a beenProcessed flag
output(table(sqNamesTable2(surname != 'Hawthorn'), { max(group, aage), surname }, surname, few, keyed));

//check literals are assigned
output(table(sqNamesTable3(forename = 'Gavin'), { 'Count: ', count(group), 'Name: ', surname }, surname, few, keyed));

//Sub query needs serializing or repeating....

// A simple inline subquery
output(table(sqNamesTable4, { count(books), sumage := sum(group, aage) }, count(books), few, keyed));

//A not-so-simple out of line subquery
secondBookName := (string20)sort(sqNamesTable5.books, name)[2].name;
output(table(sqNamesTable5, { secondBookName, sumage := sum(group, aage) }, secondBookName, few, keyed));




// An out of line subquery (caused problems accessing parent inside sort criteria
output(table(sqNamesTable7, { count(books(id != 0)), sumage := sum(group, aage) }, count(books(id != 0)), few, keyed));

