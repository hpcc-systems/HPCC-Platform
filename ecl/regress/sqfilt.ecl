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

import sq;
sq.DeclareCommon();

#option ('childQueries', true);

// Test filtering at different levels, making sure parent fields are available in the child query.
// Also tests scoping of sub expressions using within.

udecimal8 todaysDate := 20040602D;
unsigned4 age(udecimal8 dob) := ((todaysDate - dob) / 10000D);

//MORE: books[1] ave(books)

// Different child operators, all inline.
house := sqHousePersonBookDs.persons;
persons := sqHousePersonBookDs.persons;
books := persons.books;

booksDs := sqBookDs(personid = persons.id);
personsDs := sqPersonDs(houseid = sqHousePersonBookDs.id);
booksDsDs := sqBookDs(personid = personsDs.id);
personsDsDs := sqPersonDs(houseid = sqHouseDs.id);
booksDsDsDs := sqBookDs(personid = personsDsDs.id);

//Who has a book worth more than their book limit? (nest, nest), (nest, ds) (ds, ds)
output(sqHousePersonBookDs, { addr, max(persons,booklimit), max(persons.books,price), count(persons(exists(books(price>persons.booklimit)))); }, named('NumPeopleExceedBookLimit'));
output(sqHousePersonBookDs, { addr, count(persons(exists(booksDs(price>persons.booklimit)))); }, named('NumPeopleExceedBookLimitDs'));
output(sqHouseDs, { addr, count(personsDsDs(exists(booksDsDsDs(price>personsDs.booklimit)))); }, named('NumPeopleExceedBookLimitDsDs'));

//The following currently give the wrong results because there is no differentiation between the two iterators on books
//which means sizeof(row-of-books) is wrong, and the iterators etc. don't work correctly!

// How many people have books worth more than twice their average book price
output(sqHousePersonBookDs, { addr, count(persons(exists(books(price>ave(books, price)*2)))) });
output(sqHousePersonBookDs, { addr, count(persons(exists(booksDs(price>ave(books, price)*2)))) });
output(sqHousePersonBookDs, { addr, count(persons(exists(booksDs(price>ave(booksDs, price)*2)))) });
//output(sqHousePersonBookDs, { addr, count(personsDs(exists(booksDsDs(price>ave(booksDs, price)*2)))) });
output(sqHousePersonBookDs, { addr, count(personsDs(exists(booksDsDs(price>ave(booksDsDs, price)*2)))) });
output(sqHouseDs, { addr, count(personsDsDs(exists(booksDsDsDs(price>ave(booksDsDsDs, price)*2)))) });

/**** Within not supported yet ******
// How many people have books worth more than twice the average book price for the house
output(sqHousePersonBookDs, { addr, count(persons(exists(books(price>ave(books(within house), price)*2)))) });
output(sqHousePersonBookDs, { addr, count(persons(exists(booksDs(price>ave(books(within house), price)*2)))) });
output(sqHousePersonBookDs, { addr, count(persons(exists(booksDs(price>ave(booksDs(within house), price)*2)))) });
//output(sqHousePersonBookDs, { addr, count(personsDs(exists(booksDsDs(price>ave(booksDs(within house), price)*2)))) });
output(sqHousePersonBookDs, { addr, count(personsDs(exists(booksDsDs(price>ave(booksDsDs(within house), price)*2)))) });
output(sqHouseDs, { addr, count(personsDsDs(exists(booksDsDsDs(price>ave(booksDsDsDs(within sqHouseDs), price)*2)))) });
***** end within *****/

output(sqHouseDs, { addr, filename, count(personsDsDs(exists(booksDsDsDs(price>personsDs.booklimit)))); }, named('NumPeopleFilename'));
output(sqHouseDs, { addr, count(personsDsDs(exists(booksDsDsDs(price>(integer)sqHouseDs.filename)))); }, named('NumPeopleFilenameChild'));

