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

childAggRecord(childRecord ds) := 
            RECORD
               f1 := COUNT(GROUP);
               f2 := SUM(GROUP, ds.person_id);
               f3 := MAX(GROUP, ds.per_surname);
               f4 := min(group, ds.per_forename);
            END;


firstChild := parentDataset.children[1];

rolledupParent := table(parentDataset, { firstChild.person_id, firstChild.per_surname, firstChild.per_forename } );

output(rolledupParent,,'out.d00');


projectedFirstChild := table(parentDataset.children, { per_name := per_surname + ',' + per_forename, id := person_id * 3; })[1];

rolledupParent2 := table(parentDataset, { projectedFirstChild.per_name, projectedFirstChild.id } );

output(rolledupParent2,,'out.d00');
