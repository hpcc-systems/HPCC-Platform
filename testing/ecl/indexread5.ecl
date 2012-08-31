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
