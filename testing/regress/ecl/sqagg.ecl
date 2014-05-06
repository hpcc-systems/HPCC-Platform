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

import $.setup;
sq := setup.sq('hthor');

boolean missingLevelOk := false;

// Test the different child operators.  Try and test inline and out of line, also part of a compound
// source activity and not part.

udecimal8 todaysDate := 20040602D;
unsigned4 age(udecimal8 dob) := ((todaysDate - dob) / 10000D);

//MORE: books[1] ave(books)

// Different child operators, all inline.
persons := sq.HousePersonBookDs.persons;
books := persons.books;
personByAgeDesc := sort(sq.HousePersonBookDs.persons, dob);

output(sq.HousePersonBookDs, { addr, numPeople := count(persons), aveAge:= ave(persons, age(dob)), maxDob := max(persons, dob)});
output(sq.HousePersonBookDs, { addr, oldest := personByAgeDesc[1].forename + ' ' + personByAgeDesc[1].surname });
output(sq.HousePersonBookDs, { addr, firstPerson := persons[1].forename + ' ' + persons[1].surname });

// Grand children, again all inline.

booksByRatingDesc := sort(sq.HousePersonBookDs.persons.books, -rating100);


//sort order is deliberately different from anything that will be used later
xpersons := sort(sq.HousePersonBookDs.persons, surname + (string)dob + forename)[1..200];
xpersonByAgeDesc := sort(xpersons, dob);
xbooks := sort(sq.HousePersonBookDs.persons.books, name + (string)rating100 + author)[1..200];
xbooksByRatingDesc := sort(xbooks, -rating100);
xxbooks := sort(xpersons.books, name + (string)rating100 + author)[1..200];
xxbooksByRatingDesc := sort(xxbooks, -rating100);

//More: Need to think about walking 3rd level children e.g., in ave, and [1]:
output(sq.HousePersonBookDs, { addr, numBooks := sum(persons, count(books)), maxRating := max(persons, max(books, rating100))});
output(sq.HousePersonBookDs, { addr, firstBook := persons[1].books[1].name + ': ' + persons[1].books[1].author });
#if (true)
output(sq.HousePersonBookDs, { addr, numBooks := count(persons.books), ave(persons.books, rating100), max(persons.books, rating100)});
output(sq.HousePersonBookDs, { addr, ave(persons.books(persons.booklimit > 0), rating100)});
output(sq.HousePersonBookDs, { addr, bestBook := booksByRatingDesc[1].name + ': ' + booksByRatingDesc[1].author});
output(sq.HousePersonBookDs, { addr, firstBook := persons.books[1].name + ': ' + persons.books[1].author });     //NB: Different from above.
#end

//Now do the same, but unsure the children and main activity are not inline or compound
// Different child operators, out of line inline.
output('**** The following results should be identical - calculated using a subquery ****');

output(sq.HousePersonBookDs, { addr, numPeople := count(xpersons), aveAge := ave(xpersons, age(dob)), maxDob := max(xpersons, dob)});
output(sq.HousePersonBookDs, { addr, oldest := xpersonByAgeDesc[1].forename + ' ' + xpersonByAgeDesc[1].surname });
output(sq.HousePersonBookDs, { addr, firstPerson := xpersons[1].forename + ' ' + xpersons[1].surname });

// Grand children out of line, children are inline

output(sq.HousePersonBookDs, { addr, numBooks := sum(persons, count(xbooks)), maxRating := max(persons, max(xbooks, rating100))});
output(sq.HousePersonBookDs, { addr, firstBook := evaluate(persons[1], xbooks[1].name) + ': ' + evaluate(persons[1], xbooks[1].author) });
#if (true)
output(sq.HousePersonBookDs, { addr, numBooks := count(xbooks), ave(xbooks, rating100), max(xbooks, rating100)});
output(sq.HousePersonBookDs, { addr, bestBook := xbooksByRatingDesc[1].name + ': ' + xbooksByRatingDesc[1].author});
output(sq.HousePersonBookDs, { addr, firstBook := xbooks[1].name + ': ' + xbooks[1].author });       //NB: Different from above.
#end

// Grand children out of line, children also out of line

output('**** The following results should be similar persons are reordered ****');
output(sq.HousePersonBookDs, { addr, numBooks := sum(xpersons, count(xxbooks)), max(xpersons, max(xxbooks, rating100))});
output(sq.HousePersonBookDs, { addr, firstBook := evaluate(xpersons[1], xxbooks[1].name) + ': ' + evaluate(xpersons[1], xxbooks[1].author) });
#if (true)
output(sq.HousePersonBookDs, { addr, numBooks := count(xxbooks), ave(xxbooks, rating100), max(xxbooks, rating100)});
output(sq.HousePersonBookDs, { addr, bestBook := xxbooksByRatingDesc[1].name + ': ' + xxbooksByRatingDesc[1].author});
output(sq.HousePersonBookDs, { addr, firstBook := xxbooks[1].name + ': ' + xxbooks[1].author });     //NB: Different from above.
#end

//--------- Now perform the aggregate operations with person as outer iteration ----------
// note: sq.HousePersonDs fields are still accessible!
output(sq.HousePersonBookDs.persons, { surname, numBooks := count(books), ave(books, rating100), max(books, rating100)});
output(sq.HousePersonBookDs.persons, { surname, bestBook := booksByRatingDesc[1].name + ': ' + booksByRatingDesc[1].author});
output(sq.HousePersonBookDs.persons, { surname, firstBook := books[1].name + ': ' + books[1].author });

output(xpersons, { surname, numBooks := count(xxbooks), ave(xxbooks, rating100), max(xxbooks, rating100)});
output(xpersons, { surname, bestBook := xxbooksByRatingDesc[1].name + ': ' + xxbooksByRatingDesc[1].author});
output(xpersons, { surname, firstBook := xxbooks[1].name + ': ' + xxbooks[1].author });

// note: sq.HousePersonDs fields are still accessible!

#if (false)
output(sq.HousePersonBookDs.persons, { sq.HousePersonBookDs.addr, surname, numBooks := count(books), ave(books, rating100), max(books, rating100)});
output(sq.HousePersonBookDs.persons, { sq.HousePersonBookDs.addr, surname, bestBook := booksByRatingDesc[1].name + ': ' + booksByRatingDesc[1].author});
output(sq.HousePersonBookDs.persons, { sq.HousePersonBookDs.addr, surname, firstBook := books[1].name + ': ' + books[1].author });
#end

#if (missingLevelOk)
output(xpersons, { sq.HousePersonBookDs.addr, surname, numBooks := count(xxbooks), ave(xxbooks, rating100), max(xxbooks, rating100)});
output(xpersons, { sq.HousePersonBookDs.addr, surname, bestBook := xxbooksByRatingDesc[1].name + ': ' + xxbooksByRatingDesc[1].author});
output(xpersons, { sq.HousePersonBookDs.addr, surname, firstBook := xxbooks[1].name + ': ' + xxbooks[1].author });
#end
