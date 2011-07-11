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

//UseStandardFiles
//nothor
#option ('optimizeDiskSource',true)
#option ('optimizeChildSource',true)
#option ('optimizeIndexSource',true)
#option ('optimizeThorCounts',false)
#option ('countIndex',false)

// Test filtering at different levels - especially including subqueries at different levels


udecimal8 todaysDate := 20040602D;
unsigned4 age(udecimal8 dob) := ((todaysDate - dob) / 10000D);
unsigned4 yob(udecimal8 dob) := dob / 10000D;

//MORE: books[1] ave(books)

// Different child operators, all inline.
house := sqHousePersonBookDs;
persons := sqHousePersonBookDs.persons;
books := persons.books;

booksDs := sqBookDs(personid = persons.id);
personsDs := sqPersonDs(houseid = sqHousePersonBookDs.id);
booksDsDs := sqBookDs(personid = personsDs.id);
personsDsDs := sqPersonDs(houseid = sqHouseDs.id);
booksDsDsDs := sqBookDs(personid = personsDsDs.id);

//Someone in the house is older than the house and total price of books is less than book limit
//count(house(exists(persons(yob(dob) > house.yearBuilt))));
count(house(exists(persons(booklimit = 0 or (sum(books, price) <= booklimit)))));
//count(house(exists(personsDs(yob(dob) > house.yearBuilt))));
count(house(exists(persons(booklimit = 0 or (sum(booksDs, price) <= booklimit)))));

//count(house(exists(persons(yob(dob) > house.yearBuilt)), exists(persons(booklimit = 0 or (sum(books, price) <= booklimit)))));
//count(house(exists(personsDs(yob(dob) > house.yearBuilt)), exists(persons(booklimit = 0 or (sum(booksDs, price) <= booklimit)))));
