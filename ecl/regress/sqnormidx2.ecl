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

sequential(
//Normalized, count
output(sqNamesIndex1.books(rating100>50), { count(group) }),

//Normalized, aggregate
output(sqNamesIndex2(surname != '').books, { max(group, rating100) }),

//Normalized, grouped aggregate
output(table(sqNamesIndex3.books, { count(group), rating100}, rating100)),

//Normalized, grouped aggregate - criteria is in parent dataset
output(table(sqNamesIndex4(surname != '').books, { count(group), sqNamesIndex4.surname }, sqNamesIndex4.surname))
  //more: Make the ,few implicit...
);