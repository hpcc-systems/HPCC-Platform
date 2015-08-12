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

