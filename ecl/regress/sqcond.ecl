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

import sq;
sq.DeclareCommon();

// Test the different child operators.  Try and test inline and out of line, also part of a compound
// source activity and not part.

udecimal8 todaysDate := 20040602D;
unsigned4 age(udecimal8 dob) := ((todaysDate - dob) / 10000D);

//MORE: books[1] ave(books)

// Different child operators, all inline.
persons := sqHousePersonBookDs.persons;
books := persons.books;

//nofold() are there to ensure the subquery is evaluated as a child query.

whichPersons := if (sqHousePersonBookDs.id % 2 = 1, nofold(persons(surname[1] < 'N')), nofold(persons(surname[1] >= 'N')));
personByAgeDesc := sort(whichPersons, -dob);

output(sqHousePersonBookDs, { addr, count(whichPersons), ave(whichPersons, age(dob)), max(whichPersons, dob)});
output(sqHousePersonBookDs, { addr, oldest := personByAgeDesc[1].forename + ' ' + personByAgeDesc[1].surname });
output(sqHousePersonBookDs, { addr, firstPerson := whichPersons[1].forename + ' ' + whichPersons[1].surname });

