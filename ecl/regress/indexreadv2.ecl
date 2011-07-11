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

#option ('targetClusterType', 'thor');
#option ('_Probe', true);

childRecord := RECORD
unsigned4 person_id;
string20  per_surname := 'x';
string20  per_forename;
unsigned8 holepos;
    END;

parentRecord := 
                RECORD
integer8            id;
string20            address1;
string20            address2;
string20            address3;
unsigned2           numPeople;
DATASET(childRecord, COUNT(SELF.numPeople))   children{blob};
string              postcode{blob};
set of string2      states{blob};
childRecord         myChild{blob};
                END;

parentDataset := DATASET('test',parentRecord,FLAT);


i := index(parentDataset, { id, address1, address2, address3 }, { parentDataset }, 'superkey');


string searchSurname := 'Halliday' : stored('searchName');
unsigned8 searchId := 12345 : stored('id');
string2 searchState := 'FL' : stored('state');

buildindex(i);

count(i(id = searchId, exists(children(per_surname=searchSurname)), searchState in states, postcode != ''));
output(i(id = searchId, exists(children(per_surname=searchSurname)), searchState in states, postcode != 'unknown'));




idRecord := 
                RECORD
unsigned8           id;
                END;

idDataset := dataset('ids', idRecord, thor);

j := join(idDataset(id != 0), i, left.id = right.id and right.postcode<>'', left outer);
output(j);

j2 := join(idDataset(id != 0), parentDataset, left.id = right.id and exists(right.children), keyed(i), left outer);
output(j2);
