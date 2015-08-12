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

//Normalized, no filter
output(sqNamesIndex1.books, { name, author, rating100 });

//Normalized, filter on inner level
output(sqNamesIndex2.books(rating100>50), { name, author, rating100 });

//Normalized, filter on outer level
output(sqNamesIndex3.books(sqNamesIndex3.surname='Hawthorn',sqNamesIndex3.dob*2!=0), { name, author, rating100 });

//Normalized, filter on both levels
output(sqNamesIndex4.books(rating100>50, sqNamesIndex4.surname='Hawthorn'), { name, author, rating100 });

//Normalized, filter on both levels - diff syntax, location of filter is optimized.
output(sqNamesIndex5(surname='Hawthorn',dob*2!=0).books(rating100>50), { name, author, rating100 });

//No filter or project - need to make sure we create correctly
output(sqNamesIndex6.books);
