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

//Check a single child records can be treated as a blob

childRecord := RECORD
unsigned4 person_id;
string20  per_surname;
string20  per_forename;
unsigned8 holepos;
    END;

parentRecord :=
                RECORD
unsigned8           id;
string20            address1;
string20            address2;
string20            address3;
unsigned2           numPeople;
DATASET(childRecord, COUNT(SELF.numPeople))   children;
string10            postcode;
                END;

parentDataset := DATASET('test',parentRecord,FLAT);

rollupParentRecord :=
                RECORD
unsigned8           id;
string20            address1;
string20            address2;
string20            address3;
unsigned4           count_person_id;
unsigned4           sum_person_id;
string20            max_per_surname;
string20            min_per_forename;
                END;

childAggRecord(parentRecord l) :=
            RECORD
               f1 := COUNT(GROUP);
               f2 := SUM(GROUP, l.children.person_id);
               f3 := MAX(GROUP, l.children.per_surname);
               f4 := min(group, l.children.per_forename);
               f5 := l.children.person_id;      // doesn't make much sense
            END;


rollupParentRecord rollupPeople(parentRecord l) :=
TRANSFORM
    SELF := l;
//    SELF.count_person_id := l.children[1].holepos;
    SELF.count_person_id := table(l.children, childAggRecord(l))[1].f1;
    SELF.sum_person_id := SUM(l.children, person_id);
    SELF.max_per_surname := MAX(l.children, per_surname);
    SELF.min_per_forename := MIN(l.children, per_forename);
END;

rolledupParent := project(parentDataset, rollupPeople(LEFT));

output(rolledupParent,,'out.d00');
