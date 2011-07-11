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
#option ('optimizeDiskSource',true)
#option ('optimizeChildSource',false)
#option ('optimizeIndexSource',true)
#option ('optimizeThorCounts',false)
#option ('countIndex',false)


somePeople := sqPersonBookDs(id % 2 = 1);

//------------
 
sqPersonBookIdRec gatherOtherBooks(sqPersonBookRelatedIdRec l, sqSimplePersonBookIndex r) := TRANSFORM

    self.books := project(r.books, transform(sqBookIdRec, self := left));
    self := l;
end;

peopleWithNewBooks := join(somePeople, sqSimplePersonBookIndex, 
                           KEYED(left.surname = right.surname) and not exists(left.books(id in set(right.books, id))),
                           gatherOtherBooks(left, right));

//------------

slimPeople := table(somePeople, { surname, dataset books := books; });

recordof(slimPeople) gatherOtherBooks2(recordof(slimPeople) l, sqSimplePersonBookIndex r) := TRANSFORM
    self.books := project(r.books, transform(sqBookIdRec, self := left));
    self := l;
end;

peopleWithNewBooks2 := join(slimPeople, sqSimplePersonBookIndex, 
                           KEYED(left.surname = right.surname) and not exists(left.books(id in set(right.books, id))),
                           gatherOtherBooks2(left, right));



//------------
//full keyed join

sqPersonBookIdRec gatherOtherBooksFull(sqPersonBookRelatedIdRec l, sqSimplePersonBookDs r) := TRANSFORM

    self.books := project(r.books, transform(sqBookIdRec, self := left));
    self := l;
end;

peopleWithNewBooksFull := join(somePeople, sqSimplePersonBookDs, 
                           KEYED(left.surname = right.surname) and not exists(left.books(id in set(right.books, id))),
                           gatherOtherBooksFull(left, right), keyed(sqSimplePersonBookIndex));

recordof(slimPeople) gatherOtherBooksFull2(recordof(slimPeople) l, sqSimplePersonBookDs r) := TRANSFORM
    self.books := project(r.books, transform(sqBookIdRec, self := left));
    self := l;
end;

peopleWithNewBooksFull2 := join(slimPeople, sqSimplePersonBookDs, 
                           KEYED(left.surname = right.surname) and not exists(left.books(id in set(right.books, id))),
                           gatherOtherBooksFull2(left, right), keyed(sqSimplePersonBookIndex));


sequential(
    output(sort(peopleWithNewBooks, surname, forename)),
    output(sort(peopleWithNewBooks2, surname)),
    output(sort(peopleWithNewBooksFull, surname, forename)),
    output(sort(peopleWithNewBooksFull2, surname)),
    output('done')
);
