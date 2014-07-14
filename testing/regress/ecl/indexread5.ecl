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

somePeople := sq.PersonBookDs(id % 2 = 1);

sq.PersonBookIdRec gatherOtherBooks(sq.PersonBookRelatedIdRec in) := TRANSFORM

    myBookIds := set(in.books, in.books.id);
    otherPeople := sq.SimplePersonBookIndex(KEYED(surname = in.surname) and not exists(books(sq.SimplePersonBookIndex.books.id in myBookIds)));
    newBooks := normalize(otherPeople, left.books, transform(right));
    self.books := project(newBooks, transform(sq.BookIdRec, self := left));
    self := in;
end;

peopleWithNewBooks := project(somePeople, gatherOtherBooks(left));



sq.PersonBookIdRec gatherOtherBooks2(sq.PersonBookRelatedIdRec in) := TRANSFORM

    myBookIds := set(in.books, in.books.id);
    otherPeople := sq.SimplePersonBookIndex(KEYED(surname = in.surname) and not exists(books(sq.SimplePersonBookIndex.books.id in myBookIds)));
    newBooks := otherPeople[1].books;
    self.books := project(newBooks, transform(sq.BookIdRec, self := left));
    self := in;
end;

peopleWithNewBooks2 := project(somePeople, gatherOtherBooks2(left));


sequential(
    output(peopleWithNewBooks),
    output(peopleWithNewBooks2)
);
