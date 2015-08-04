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
