/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#option ('childQueries', true);
#option ('maxInlineDepth', 0);
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
