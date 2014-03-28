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

//Normalized, no filter
output(sq.SimplePersonBookIndex.books, { name, author, rating100 });

//Normalized, filter on inner level
output(sq.SimplePersonBookIndex.books(rating100>50), { name, author, rating100 });

//Normalized, filter on outer level
output(sq.SimplePersonBookIndex.books(sq.SimplePersonBookIndex.surname='Halliday',sq.SimplePersonBookIndex.dob*2!=0), { name, author, rating100 });

//Normalized, filter on both levels
output(sq.SimplePersonBookIndex.books(rating100>50, sq.SimplePersonBookIndex.surname='Halliday'), { name, author, rating100 });

//Normalized, filter on both levels - diff syntax, location of filter is optimized.
output(sq.SimplePersonBookIndex(surname='Halliday',dob*2!=0).books(rating100>50), { name, author, rating100 });

//No filter or project - need to make sure we create correctly
output(sq.SimplePersonBookIndex.books);
