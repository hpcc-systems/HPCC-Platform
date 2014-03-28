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

//nothorlcr

import $.setup.sq;

boolean storedTrue := true : stored('storedTrue');

udecimal8 todaysDate := 20040602D;
unsigned4 age(udecimal8 dob) := ((todaysDate - dob) / 10000D);

// Test the different child operators on related datasets.

// Different child operators, all inline.
//persons := relatedPersons(sq.HouseDs);
//books := relatedBooks(persons);
persons := sq.PersonDs(houseid = sq.HouseDs.id);
books := sq.BookDs(personid = persons.id);

personByAgeDesc := sort(persons, dob);

o1 := table(sq.HouseDs, { addr, count(persons), ave(persons, age(dob)), max(persons, dob)});
o2 := table(sq.HouseDs, { addr, oldest := personByAgeDesc[1].forename + ' ' + personByAgeDesc[1].surname });
o3 := table(sq.HouseDs, { addr, firstPerson := persons[1].forename + ' ' + persons[1].surname });

// Grand children, again all inline.

booksByRatingDesc := sort(books, -rating100);

//More: Need to think about walking 3rd level children e.g., in ave, and [1]:
o4 := table(sq.HouseDs, { addr, numBooks := sum(persons, count(books(storedTrue))), max(persons, max(books, rating100))});
o5 := table(sq.HouseDs, { addr, firstBook := evaluate(persons[1], books[1].name) + ': ' + evaluate(persons[1], books[1].author) });

//--------- Now perform the aggregate operations with person as outer iteration ----------

o9 := table(sq.PersonDs, { surname, numBooks := count(books), ave(books, rating100), max(books, rating100)});
o10 := table(sq.PersonDs, { surname, bestBook := booksByRatingDesc[1].name + ': ' + booksByRatingDesc[1].author});
o11 := table(sq.PersonDs, { surname, firstBook := books[1].name + ': ' + books[1].author });     //NB: Different from above.

output(allnodes(local(o1)));
output(allnodes(local(o2)));
output(allnodes(local(o3)));
output(allnodes(local(o4)));
output(allnodes(local(o5)));

output(allnodes(local(o9)));
output(allnodes(local(o10)));
output(allnodes(local(o11)));
