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

childRecord := RECORD
unsigned4 person_id;
string20  per_surname := 'x';
string20  per_forename;
unsigned8 holepos;
    END;

parentRecord := 
                RECORD,maxlength(50000)
unsigned8           id;
string20            address1;
string20            address2;
string20            address3;
                END;

payloadRecord :=
                RECORD
unsigned2           numPeople;
DATASET(childRecord, COUNT(SELF.numPeople))   children;
string10            postcode;
                END;

parentDataset := DATASET('test',parentRecord,FLAT);


i := index(parentRecord, payloadRecord, 'superkey');
i2 := index(i, 'renamedSuperKey');

output(i2(id=3));

