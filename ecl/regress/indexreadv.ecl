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

#option ('targetClusterType', 'roxie');

childRecord := RECORD
unsigned4 person_id;
string20  per_surname := 'x';
string  per_forename;
unsigned8 holepos;
    END;

parentRecord :=
                RECORD
integer8            id;
string20            address1;
string20            address2;
string20            address3;
unsigned2           numPeople;
DATASET(childRecord, COUNT(SELF.numPeople))   children;
string10            postcode;
childRecord         myChild;
                END;

parentDataset := DATASET('test',parentRecord,FLAT);


i := index(parentDataset, { id, address1, address2, address3, postcode }, { parentDataset }, 'superkey');


string searchSurname := 'Hawthorn' : stored('searchName');
unsigned8 searchId := 12345 : stored('id');


count(i(id = searchId, exists(children(per_surname=searchSurname))));


idRecord :=
                RECORD
unsigned8           id;
                END;

idDataset := dataset('ids', idRecord, thor);

parentRecord t(i r) := transform
    self := r;
    end;

j := join(idDataset(id != 0), i, left.id = right.id and right.postcode<>'', t(RIGHT), left outer);
output(j);
