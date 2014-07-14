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

// Test filtering at different levels - especially including subqueries at different levels


udecimal8 todaysDate := 20040602D;
unsigned4 age(udecimal8 dob) := ((todaysDate - dob) / 10000D);
unsigned4 yob(udecimal8 dob) := dob / 10000D;

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

//Someone in the house is older than the house and total price of books is less than book limit
//count(house(exists(persons(yob(dob) > house.yearBuilt))));
count(house(exists(persons(booklimit = 0 or (sum(books, price) <= booklimit)))));
//count(house(exists(personsDs(yob(dob) > house.yearBuilt))));
count(house(exists(persons(booklimit = 0 or (sum(booksDs, price) <= booklimit)))));

//count(house(exists(persons(yob(dob) > house.yearBuilt)), exists(persons(booklimit = 0 or (sum(books, price) <= booklimit)))));
//count(house(exists(personsDs(yob(dob) > house.yearBuilt)), exists(persons(booklimit = 0 or (sum(booksDs, price) <= booklimit)))));
