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

//------------

sq.PersonBookIdRec gatherOtherBooks(sq.PersonBookRelatedIdRec l, sq.SimplePersonBookIndex r) := TRANSFORM

    self.books := project(r.books, transform(sq.BookIdRec, self := left));
    self := l;
end;

peopleWithNewBooks := join(somePeople, sq.SimplePersonBookIndex,
                           KEYED(left.surname = right.surname) and not exists(left.books(id in set(right.books, id))),
                           gatherOtherBooks(left, right));

//------------

slimPeople := table(somePeople, { surname, dataset books := books; });

recordof(slimPeople) gatherOtherBooks2(recordof(slimPeople) l, sq.SimplePersonBookIndex r) := TRANSFORM
    self.books := project(r.books, transform(sq.BookIdRec, self := left));
    self := l;
end;

peopleWithNewBooks2 := join(slimPeople, sq.SimplePersonBookIndex,
                           KEYED(left.surname = right.surname) and not exists(left.books(id in set(right.books, id))),
                           gatherOtherBooks2(left, right));



//------------
//full keyed join

sq.PersonBookIdRec gatherOtherBooksFull(sq.PersonBookRelatedIdRec l, sq.SimplePersonBookDs r) := TRANSFORM

    self.books := project(r.books, transform(sq.BookIdRec, self := left));
    self := l;
end;

peopleWithNewBooksFull := join(somePeople, sq.SimplePersonBookDs,
                           KEYED(left.surname = right.surname) and not exists(left.books(id in set(right.books, id))),
                           gatherOtherBooksFull(left, right), keyed(sq.SimplePersonBookIndex));

recordof(slimPeople) gatherOtherBooksFull2(recordof(slimPeople) l, sq.SimplePersonBookDs r) := TRANSFORM
    self.books := project(r.books, transform(sq.BookIdRec, self := left));
    self := l;
end;

peopleWithNewBooksFull2 := join(slimPeople, sq.SimplePersonBookDs,
                           KEYED(left.surname = right.surname) and not exists(left.books(id in set(right.books, id))),
                           gatherOtherBooksFull2(left, right), keyed(sq.SimplePersonBookIndex));


sequential(
    output(sort(peopleWithNewBooks, surname, forename)),
    output(sort(peopleWithNewBooks2, surname)),
    output(sort(peopleWithNewBooksFull, surname, forename)),
    output(sort(peopleWithNewBooksFull2, surname)),
    output('done')
);
