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

//Project used inside a child dataset operation.
//First example should be more efficient since should assign directly to the target dataset
//second example will need to create a temporary row.

import dt;

childRecord := RECORD
unsigned4 person_id;
dt.pstring per_forename;
string20  per_surname;
unsigned8 holepos;
    END;

parentRecord :=
                RECORD
unsigned8           id;
string20            address1;
string20            address2;
string20            address3;
unsigned2           numPeople;
DATASET(childRecord)   children;
string10            postcode;
                END;

parentDataset := DATASET('test',parentRecord,FLAT);


childRecord selectBestChild(parentRecord l) :=
TRANSFORM
    SELF := sort(l.children, per_surname, -per_forename)[1];
END;

output(PROJECT(parentDataset,selectBestChild(LEFT)),,'out1.d00');

