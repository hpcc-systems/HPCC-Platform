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

import sq;
sq.DeclareCommon();

#option ('childQueries', true);

// Test filtering at different levels, making sure parent fields are available in the child query.
// Also tests scoping of sub expressions using within.

udecimal8 todaysDate := 20040602D;
unsigned4 age(udecimal8 dob) := ((todaysDate - dob) / 10000D);

//MORE: books[1] ave(books)

// Different child operators, all inline.
persons := sqHousePersonBookDs.persons;
books := persons.books;
books_2 := table(books);

//people with a book worth more than the rest of their books.
//Iterate books twice, and ensure that the two cursors are separately accessed.
validBooks := nofold(books(max(id*7,99) != 0));
validBooks2 := table(validBooks);

output(sqHousePersonBookDs.persons, { exists(validBooks(price > sum(validBooks2(id != books.id), price))); });
