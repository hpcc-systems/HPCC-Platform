/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

//Check that nested counters work correctly!
//Working


import sq;
sq.DeclareCommon();

#option ('optimizeDiskSource',true);
#option ('optimizeChildSource',true);
#option ('optimizeIndexSource',true);
#option ('optimizeThorCounts',false);
#option ('countIndex',false);

// Nested definitions with additional ids...

newPersonIdRec :=
            record
sqPersonIdRec;
boolean         idMatchesBook;
            end;

newPersonIdRec tPerson(sqPersonBookIdRec l, unsigned8 cnt, unsigned8 houseId) :=
        transform
            self.id := cnt;
            self.idMatchesBook := cnt = houseId;
            self := l;
        end;

newPeople(sqHousePersonBookDs ds, unsigned8 houseId) := sort(project(ds.persons, tPerson(LEFT, COUNTER, houseId)),id);


newHousePersonRec :=
            record
sqHouseIdRec;
dataset(newPersonIdRec) persons;
            end;

newHousePersonRec tHouse(sqHousePersonBookDs l, unsigned8 cnt) :=
        transform
            self.id := cnt;
            self.persons := newPeople(l, cnt);
            self := l;
        end;

persons := sqHousePersonBookDs.persons;
books := persons.books;

newHouse := project(sqHousePersonBookDs, tHouse(LEFT, COUNTER));
output(newHouse);
