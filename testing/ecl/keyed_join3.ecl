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
