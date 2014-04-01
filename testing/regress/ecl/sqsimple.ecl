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

unsigned8 skipId := 4 : stored('skipId');
string searchAuthor := 'Dr. Seuss' : stored('searchAuthor');

persons := sq.HousePersonBookDs.persons;
books := persons.books;

output(sq.HousePersonBookDs, { numPeopleWithAuthoredBooks := count(persons(exists(books(author <> '')))), numPeople := count(persons) });
output(sq.HousePersonBookDs, {count(persons(id != sq.HousePersonBookDs.id, exists(books(id != sq.HousePersonBookDs.id)))), count(persons) });
output(sq.HouseDs, { count(sq.PersonDs(houseid=sq.HouseDs.id,exists(sq.BookDs(personid=sq.PersonDs.id,id != sq.HouseDs.id,name != sq.HouseDs.addr)))) });

// table invariant
output(sq.HousePersonBookDs, {count(persons(id != skipId, exists(books(id != skipId, searchAuthor = author)))), count(persons) });
output(sq.HousePersonBookDs, {count(persons(id != sq.HousePersonBookDs.id, exists(books(id != skipId)))), count(persons) });

// cse
string fullname := trim(persons.surname) + ', ' + trim(persons.forename);
output(sq.HousePersonBookDs, {count(persons(fullname != '', exists(books(author != fullname)))), count(persons) });
