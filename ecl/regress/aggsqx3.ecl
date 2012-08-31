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

import AggCommon;
AggCommon.CommonDefinitions();

//Simple disk aggregate
output(preload(sqHousePersonBookDs), { dataset people := table(persons, { surname, sum(group, aage) }, surname, few)});

//Filtered disk aggregate, which also requires a beenProcessed flag
output(sqHousePersonBookDs, { dataset people := table(persons(surname != 'Hawthorn'), { max(group, aage), surname }, surname, few)});

//check literals are assigned
output(sqHousePersonBookDs, { dataset people := table(persons(forename = 'Gavin'), { 'Count: ', count(group), 'Name: ', surname }, surname, few)});

//Sub query needs serializing or repeating....

// A simple inline subquery
output(sqHousePersonBookDs, { dataset people := table(persons, { count(books), sumage := sum(group, aage) }, count(books), few)});

//A not-so-simple out of line subquery
secondBookName := (string20)sort(sqHousePersonBookDs.persons.books, name)[2].name;
output(sqHousePersonBookDs, { dataset people := table(persons, { secondBookName, sumage := sum(group, aage) }, secondBookName, few)});
