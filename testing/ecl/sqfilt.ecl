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

