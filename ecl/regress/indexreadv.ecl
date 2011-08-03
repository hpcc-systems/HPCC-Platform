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
