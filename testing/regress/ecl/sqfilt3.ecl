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

// Test filtering at different levels, making sure parent fields are available in the child query.
// Also tests scoping of sub expressions using within.

udecimal8 todaysDate := 20040602D;
unsigned4 age(udecimal8 dob) := ((todaysDate - dob) / 10000D);

//MORE: books[1] ave(books)

// Different child operators, all inline.
house := sq.HousePersonBookDs;
persons := sq.HousePersonBookDs.persons;
books := persons.books;

booksDs := sq.BookDs(personid = persons.id);
personsDs := sq.PersonDs(houseid = sq.HousePersonBookDs.id);
booksDsDs := sq.BookDs(personid = personsDs.id);
personsDsDs := sq.PersonDs(houseid = sq.HouseDs.id);
booksDsDsDs := sq.BookDs(personid = personsDsDs.id);

//Check that count(books) is evaluated in the correct scope.
output(sq.HousePersonBookDs, { addr, count(books), count(persons(count(books)>0)) });

//More: really need another level, so that can check count(another) is done in the correct scope.
//output(sq.HousePersonBookDs, { addr, count(books, count(another)>0), count(persons, count(another)>0) });
