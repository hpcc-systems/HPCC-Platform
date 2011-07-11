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
#option ('optimizeChildSource',false)
#option ('optimizeIndexSource',true)
#option ('optimizeThorCounts',false)
#option ('countIndex',false)


somePeople := sqPersonBookDs(id % 2 = 1);
 
sqPersonBookIdRec gatherOtherBooks(sqPersonBookRelatedIdRec in) := TRANSFORM

    myBookIds := set(in.books, in.books.id);
    otherPeople := sqSimplePersonBookIndex(KEYED(surname = in.surname) and not exists(books(sqSimplePersonBookIndex.books.id in myBookIds)));
    newBooks := normalize(otherPeople, left.books, transform(right));
    self.books := project(newBooks, transform(sqBookIdRec, self := left));
    self := in;
end;

peopleWithNewBooks := project(somePeople, gatherOtherBooks(left));



sqPersonBookIdRec gatherOtherBooks2(sqPersonBookRelatedIdRec in) := TRANSFORM

    myBookIds := set(in.books, in.books.id);
    otherPeople := sqSimplePersonBookIndex(KEYED(surname = in.surname) and not exists(books(sqSimplePersonBookIndex.books.id in myBookIds)));
    newBooks := otherPeople[1].books;
    self.books := project(newBooks, transform(sqBookIdRec, self := left));
    self := in;
end;

peopleWithNewBooks2 := project(somePeople, gatherOtherBooks2(left));


sequential(
    output(peopleWithNewBooks),
    output(peopleWithNewBooks2)
);
