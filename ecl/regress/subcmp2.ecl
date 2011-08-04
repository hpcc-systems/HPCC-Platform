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

//Normalize a denormalised dataset... Using element indexing on a child record.

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
childRecord         children;
string10            postcode;
                END;

parentDataset := DATASET('test',parentRecord,FLAT);


outRecord := RECORD
typeof(parentDataset.id)    household_id;
boolean                     sameChildren;
    END;



outRecord compareChildren(parentRecord l, parentRecord r) :=
                TRANSFORM
                    SELF.household_id := l.id;
                    SELF.sameChildren := l.children = r.children;
                END;

o1 := join(parentDataset, parentDataset, LEFT.id = RIGHT.id , compareChildren(LEFT, RIGHT));

output(o1,,'out.d00');
