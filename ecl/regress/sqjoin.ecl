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

// Kind of Joining datasets


udecimal8 todaysDate := 20040602D;
unsigned4 age(udecimal8 dob) := ((todaysDate - dob) / 10000D);
unsigned4 yob(udecimal8 dob) := dob / 10000D;

house := sqHousePersonBookDs;
persons := sqHousePersonBookDs.persons;
books := persons.books;

booksDs := sqBookDs(personid = persons.id);
personsDs := sqPersonDs(houseid = sqHousePersonBookDs.id);
booksDsDs := sqBookDs(personid = personsDs.id);
personsDsDs := sqPersonDs(houseid = sqHouseDs.id);
booksDsDsDs := sqBookDs(personid = personsDsDs.id);


//How many other people also have the same book, and return any books someone else has got.
sharedBooks := books(count(sqBookDs(name = books.name, author = books.author)) > 1);
output(persons, { surname, dataset(sqBookIdRec) xshared := sharedBooks });

//The same but this time build a sub-query invariant list of potential matches
allSharedBooks := table(sqBookDs, { unsigned8 cnt := count(group), name, author }, name, author)(cnt > 1);
sharedBooks2 := books(exists(allSharedBooks(name = books.name, author = books.author)));
output(persons, { surname, dataset(sqBookIdRec) xshared := sharedBooks2 });

//MORE: We need a way of aliasing a dataset, otherwise the following doesn't work
booksDsDsLite := sqBookDs(personid = sqPersonDs.id);
sqBookDsAlias := sqBookDs;//alias(sqBookDs);
sharedBooksDs := booksDsDsLite(count(sqBookDsAlias(name = books.name, author = booksDsDsLite.author)) > 1);
output(sqPersonDs, { surname, dataset(sqBookIdRec) xshared := sharedBooksDs });


//Now do the same thing using a project instead of a table.

simpleResultRec :=
        record
string      surname;
dataset(sqBookIdRec) xshared;
        end;

//How many other people also have the same book, and return any books someone else has got.
simpleResultRec t1(sqPersonBookIdRec l) :=
        transform
            self.xshared := l.books(count(sqBookDs(name = l.books.name, author = l.books.author)) > 1);
            self := l;
        end;
output(project(persons, t1(LEFT)));


simpleResultRec t2(sqPersonBookIdRec l) :=
        transform
            self.xshared := l.books(exists(allSharedBooks(name = l.books.name, author = l.books.author)));
            self := l;
        end;
output(project(persons, t2(LEFT)));


sqBookIdRec removeBookRelation(sqBookRelatedIdRec l) :=
            TRANSFORM
                SELF := l;
            END;

simpleResultRec t3(sqPersonRelatedIdRec l) :=
        transform
            joinedBooks := project(sqBookDs(personid = l.id), removeBookRelation(LEFT));
            self.xshared := joinedBooks(count(sqBookDs(name = joinedBooks.name, author = joinedBooks.author)) > 1);
            self := l;
        end;
output(project(sqPersonDs, t3(LEFT)));

simpleResultRec t4(sqPersonRelatedIdRec l) :=
        transform
            joinedBooks := project(sqBookDs(personid = l.id), removeBookRelation(LEFT));
            self.xshared := joinedBooks(exists(allSharedBooks(name = joinedBooks.name, author = joinedBooks.author)));
            self := l;
        end;
output(project(sqPersonDs, t4(LEFT)));

