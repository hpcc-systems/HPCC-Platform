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
#option ('targetClusterType', 'roxie');
//Working

import sq;
sq.DeclareCommon();

boolean missingLevelOk := false;

udecimal8 todaysDate := 20040602D;
unsigned4 age(udecimal8 dob) := ((todaysDate - dob) / 10000D);

// Test the different child operators on related datasets.

// Different child operators, all inline.
//persons := relatedPersons(sqHouseDs);
//books := relatedBooks(persons);
persons := sqPersonDs(houseid = sqHouseDs.id);
books := sqBookDs(personid = persons.id);

personByAgeDesc := sort(persons, dob);

output(sqHouseDs, { addr, count(persons), ave(persons, age(dob)), max(persons, dob)});
output(sqHouseDs, { addr, oldest := personByAgeDesc[1].forename + ' ' + personByAgeDesc[1].surname });
output(sqHouseDs, { addr, firstPerson := persons[1].forename + ' ' + persons[1].surname });

// Grand children, again all inline.

booksByRatingDesc := sort(books, -rating100);

//More: Need to think about walking 3rd level children e.g., in ave, and [1]:
output(sqHouseDs, { addr, numBooks := sum(persons, count(books)), max(persons, max(books, rating100))});
output(sqHouseDs, { addr, firstBook := evaluate(persons[1], books[1].name) + ': ' + evaluate(persons[1], books[1].author) });
#if (missingLevelOk)
//This really needs the idea of implicit relationships between files before it is going to work
output(sqHouseDs, { addr, numBooks := count(books), ave(books, rating100), max(books, rating100)});
output(sqHouseDs, { addr, bestBook := booksByRatingDesc[1].name + ': ' + booksByRatingDesc[1].author});
output(sqHouseDs, { addr, firstBook := books[1].name + ': ' + books[1].author });       //NB: Different from above.
#end

//--------- Now perform the aggregate operations with person as outer iteration ----------

output(sqPersonDs, { surname, numBooks := count(books), ave(books, rating100), max(books, rating100)});
output(sqPersonDs, { surname, bestBook := booksByRatingDesc[1].name + ': ' + booksByRatingDesc[1].author});
output(sqPersonDs, { surname, firstBook := books[1].name + ': ' + books[1].author });       //NB: Different from above.

//More: Need to think about acceessing fields in house - need some sort of relation construct
#if (false)
output(sqPersonDs, { sqHouseDs.addr, surname, numBooks := count(books), ave(books, rating100), max(persons.books, rating100)});
output(sqPersonDs, { sqHouseDs.addr, surname, bestBook := booksByRatingDesc[1].name + ': ' + booksByRatingDesc[1].author});
output(sqPersonDs, { sqHouseDs.addr, surname, firstBook := books[1].name + ': ' + books[1].author });       //NB: Different from above.
#end

