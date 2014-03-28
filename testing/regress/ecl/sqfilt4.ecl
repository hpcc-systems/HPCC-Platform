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

// Test skipped level iterating - including iterating grand children of processed children

// Different child operators, all inline.
house := sq.HousePersonBookDs;
persons := sq.HousePersonBookDs.persons;
books := persons.books;

output(house, { count(persons(dob < 19700101).books) });


deduped := dedup(persons, surname);

output(house, { count(deduped(dob < 19700101).books) });
