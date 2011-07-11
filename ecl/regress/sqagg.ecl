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
#option ('optimizeDiskSource',true);
#option ('optimizeChildSource',true);
#option ('targetClusterType', 'roxie');
//Working.

import * from sq;
DeclareCommon();

boolean missingLevelOk := false;

// Test the different child operators.  Try and test inline and out of line, also part of a compound
// source activity and not part.

udecimal8 todaysDate := 20040602D;
unsigned4 age(udecimal8 dob) := ((todaysDate - dob) / 10000D);

//MORE: books[1] ave(books)

// Different child operators, all inline.
persons := sqHousePersonBookDs.persons;
books := persons.books;
personByAgeDesc := sort(sqHousePersonBookDs.persons, dob);

output(sqHousePersonBookDs, { addr, numPeople := count(persons), aveAge:= ave(persons, age(dob)), maxDob := max(persons, dob)});
output(sqHousePersonBookDs, { addr, oldest := personByAgeDesc[1].forename + ' ' + personByAgeDesc[1].surname });
output(sqHousePersonBookDs, { addr, firstPerson := persons[1].forename + ' ' + persons[1].surname });

// Grand children, again all inline.

booksByRatingDesc := sort(sqHousePersonBookDs.persons.books, -rating100);

//More: Need to think about walking 3rd level children e.g., in ave, and [1]:
output(sqHousePersonBookDs, { addr, numBooks := sum(persons, count(books)), maxRating := max(persons, max(books, rating100))});
output(sqHousePersonBookDs, { addr, firstBook := persons[1].books[1].name + ': ' + persons[1].books[1].author });
#if (true)
output(sqHousePersonBookDs, { addr, numBooks := count(persons.books), ave(persons.books, rating100), max(persons.books, rating100)});
output(sqHousePersonBookDs, { addr, ave(persons.books(persons.booklimit > 0), rating100)});
output(sqHousePersonBookDs, { addr, bestBook := booksByRatingDesc[1].name + ': ' + booksByRatingDesc[1].author});
output(sqHousePersonBookDs, { addr, firstBook := persons.books[1].name + ': ' + persons.books[1].author });     //NB: Different from above.
#end

//Now do the same, but unsure the children and main activity are not inline or compound

//sort order is deliberatley different from anything that will be used later
xpersons := sort(sqHousePersonBookDs.persons, surname + (string)dob + forename)[1..200];
xpersonByAgeDesc := sort(xpersons, dob);
xbooks := sort(sqHousePersonBookDs.persons.books, name + (string)rating100 + author)[1..200];
xbooksByRatingDesc := sort(xbooks, -rating100);
xxbooks := sort(xpersons.books, name + (string)rating100 + author)[1..200];
xxbooksByRatingDesc := sort(xxbooks, -rating100);

// Different child operators, out of line inline.
output('**** The following results should be identical - calculated using a subquery ****');

output(sqHousePersonBookDs, { addr, numPeople := count(xpersons), aveAge := ave(xpersons, age(dob)), maxDob := max(xpersons, dob)});
output(sqHousePersonBookDs, { addr, oldest := xpersonByAgeDesc[1].forename + ' ' + xpersonByAgeDesc[1].surname });
output(sqHousePersonBookDs, { addr, firstPerson := xpersons[1].forename + ' ' + xpersons[1].surname });

// Grand children out of line, children are inline

output(sqHousePersonBookDs, { addr, numBooks := sum(persons, count(xbooks)), maxRating := max(persons, max(xbooks, rating100))});
output(sqHousePersonBookDs, { addr, firstBook := evaluate(persons[1], xbooks[1].name) + ': ' + evaluate(persons[1], xbooks[1].author) });
#if (true)
output(sqHousePersonBookDs, { addr, numBooks := count(xbooks), ave(xbooks, rating100), max(xbooks, rating100)});
output(sqHousePersonBookDs, { addr, bestBook := xbooksByRatingDesc[1].name + ': ' + xbooksByRatingDesc[1].author});
output(sqHousePersonBookDs, { addr, firstBook := xbooks[1].name + ': ' + xbooks[1].author });       //NB: Different from above.
#end

// Grand children out of line, children also out of line

output('**** The following results should be similar persons are reordered ****');
output(sqHousePersonBookDs, { addr, numBooks := sum(xpersons, count(xxbooks)), max(xpersons, max(xxbooks, rating100))});
output(sqHousePersonBookDs, { addr, firstBook := evaluate(xpersons[1], xxbooks[1].name) + ': ' + evaluate(xpersons[1], xxbooks[1].author) });
#if (true)
output(sqHousePersonBookDs, { addr, numBooks := count(xxbooks), ave(xxbooks, rating100), max(xxbooks, rating100)});
output(sqHousePersonBookDs, { addr, bestBook := xxbooksByRatingDesc[1].name + ': ' + xxbooksByRatingDesc[1].author});
output(sqHousePersonBookDs, { addr, firstBook := xxbooks[1].name + ': ' + xxbooks[1].author });     //NB: Different from above.
#end

//--------- Now perform the aggregate operations with person as outer iteration ----------
// note: sqHousePersonDs fields are still accessible!
output(sqHousePersonBookDs.persons, { surname, numBooks := count(books), ave(books, rating100), max(books, rating100)});
output(sqHousePersonBookDs.persons, { surname, bestBook := booksByRatingDesc[1].name + ': ' + booksByRatingDesc[1].author});
output(sqHousePersonBookDs.persons, { surname, firstBook := books[1].name + ': ' + books[1].author });

output(xpersons, { surname, numBooks := count(xxbooks), ave(xxbooks, rating100), max(xxbooks, rating100)});
output(xpersons, { surname, bestBook := xxbooksByRatingDesc[1].name + ': ' + xxbooksByRatingDesc[1].author});
output(xpersons, { surname, firstBook := xxbooks[1].name + ': ' + xxbooks[1].author });

// note: sqHousePersonDs fields are still accessible!

#if (false)
output(sqHousePersonBookDs.persons, { sqHousePersonBookDs.addr, surname, numBooks := count(books), ave(books, rating100), max(books, rating100)});
output(sqHousePersonBookDs.persons, { sqHousePersonBookDs.addr, surname, bestBook := booksByRatingDesc[1].name + ': ' + booksByRatingDesc[1].author});
output(sqHousePersonBookDs.persons, { sqHousePersonBookDs.addr, surname, firstBook := books[1].name + ': ' + books[1].author });
#end

#if (missingLevelOk)
output(xpersons, { sqHousePersonBookDs.addr, surname, numBooks := count(xxbooks), ave(xxbooks, rating100), max(xxbooks, rating100)});
output(xpersons, { sqHousePersonBookDs.addr, surname, bestBook := xxbooksByRatingDesc[1].name + ': ' + xxbooksByRatingDesc[1].author});
output(xpersons, { sqHousePersonBookDs.addr, surname, firstBook := xxbooks[1].name + ': ' + xxbooks[1].author });
#end
