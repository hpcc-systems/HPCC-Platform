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

import $.setup.sq;

//Normalized, count
output(table(sq.SimplePersonBookIndex.books(rating100>50), { count(group) }, keyed));

//Normalized, aggregate
output(table(sq.SimplePersonBookIndex(surname != '').books, { max(group, rating100) }, keyed));

//Normalized, grouped aggregate
output(sort(table(sq.SimplePersonBookIndex.books, { count(group), rating100}, rating100, keyed),RECORD));

//Normalized, grouped aggregate - criteria is in parent dataset
output(sort(table(sq.SimplePersonBookIndex(surname != '').books, { count(group), sq.SimplePersonBookIndex.surname }, sq.SimplePersonBookIndex.surname, keyed),RECORD));  //more: Make the ,few implicit...
