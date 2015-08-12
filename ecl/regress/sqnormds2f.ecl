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


//Normalized, count
output(table(sqNamesTable1.books(rating100>50), { count(group) }, KEYED));

//Normalized, aggregate
output(table(sqNamesTable2.books, { max(group, rating100) }, keyed));

//Normalized, grouped aggregate
output(table(sqNamesTable3.books, { count(group), rating100}, rating100, keyed));

//Normalized, grouped aggregate - criteria is in parent dataset
output(table(sqNamesTable4.books, { count(group), sqNamesTable4.surname }, sqNamesTable4.surname, keyed));
