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

#option ('childQueries', true);
#option ('pickBestEngine', false);

unsigned8 skipId := 4 : stored('skipId');
string searchAuthor := 'Dr. Seuss' : stored('searchAuthor');

import sq;
sq.DeclareCommon();

persons := sqHousePersonBookDs.persons;
books := persons.books;

output(sqHousePersonBookDs, { numPeopleWithAuthoredBooks := count(persons(exists(books(author <> '')))), numPeople := count(persons) });
output(sqHousePersonBookDs, {count(persons(id != sqHousePersonBookDs.id, exists(books(id != sqHousePersonBookDs.id)))), count(persons) });
output(sqHouseDs, { count(sqPersonDs(houseid=sqHouseDs.id,exists(sqBookDs(personid=sqPersonDs.id,id != sqHouseDs.id,name != sqHouseDs.addr)))) });

// table invariant
output(sqHousePersonBookDs, {count(persons(id != skipId, exists(books(id != skipId, searchAuthor = author)))), count(persons) });
output(sqHousePersonBookDs, {count(persons(id != sqHousePersonBookDs.id, exists(books(id != skipId)))), count(persons) });

// cse
string fullname := trim(persons.surname) + ', ' + trim(persons.forename);
output(sqHousePersonBookDs, {count(persons(fullname != '', exists(books(author != fullname)))), count(persons) });
