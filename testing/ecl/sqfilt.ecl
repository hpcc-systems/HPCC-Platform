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
//nothorlcr
#option ('optimizeDiskSource',true)
#option ('optimizeChildSource',true)
#option ('optimizeIndexSource',true)
#option ('optimizeThorCounts',false)
#option ('countIndex',false)

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
t1 := table(sqHousePersonBookDs, { addr, max(persons,booklimit), max(persons.books,price), count(persons(exists(books(price>persons.booklimit)))); });
t2 := table(sqHousePersonBookDs, { addr, count(persons(exists(booksDs(price>persons.booklimit)))); });
t3 := table(sqHouseDs, { addr, count(personsDsDs(exists(booksDsDsDs(price>personsDs.booklimit)))); });

output(t1,,named('NumPeopleExceedBookLimit'));
output(t2,,named('NumPeopleExceedBookLimitDs'));
output(t3,,named('NumPeopleExceedBookLimitDsDs'));

output(allnodes(local(t1)),,named('NumPeopleExceedBookLimitLocal'));
output(allnodes(local(t2)),,named('NumPeopleExceedBookLimitDsLocal'));
output(allnodes(local(t3)),,named('NumPeopleExceedBookLimitDsDsLocal'));

