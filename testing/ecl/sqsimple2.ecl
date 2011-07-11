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
#option ('optimizeChildSource',true)
#option ('optimizeIndexSource',true)
#option ('optimizeThorCounts',false)
#option ('countIndex',false)

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

newPeople(sqHousePersonBookDs ds, unsigned8 houseId) := project(ds.persons, tPerson(LEFT, COUNTER, houseId));


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
