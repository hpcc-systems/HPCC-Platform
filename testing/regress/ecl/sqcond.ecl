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

// Test the different child operators.  Try and test inline and out of line, also part of a compound
// source activity and not part.

udecimal8 todaysDate := 20040602D;
unsigned4 age(udecimal8 dob) := ((todaysDate - dob) / 10000D);

//MORE: books[1] ave(books)

// Different child operators, all inline.
persons := sq.HousePersonBookDs.persons;
books := persons.books;

//nofold() are there to ensure the subquery is evaluated as a child query.

whichPersons := if (sq.HousePersonBookDs.id % 2 = 1, nofold(persons(surname[1] < 'N')), nofold(persons(surname[1] >= 'N')));
personByAgeDesc := sort(whichPersons, -dob);

output(sq.HousePersonBookDs, { addr, count(whichPersons), ave(whichPersons, age(dob)), max(whichPersons, dob)});
output(sq.HousePersonBookDs, { addr, oldest := personByAgeDesc[1].forename + ' ' + personByAgeDesc[1].surname });
output(sq.HousePersonBookDs, { addr, firstPerson := whichPersons[1].forename + ' ' + whichPersons[1].surname });
