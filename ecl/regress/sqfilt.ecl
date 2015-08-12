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
output(sqHousePersonBookDs, { addr, count(persons(exists(books(price>ave(__ALIAS__(books), price)*2)))) });
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

