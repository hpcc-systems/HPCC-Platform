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

//Normalize a denormalised dataset...

householdRecord := RECORD
unsigned4 house_id;
string20  address1;
string20  zip;
    END;


personRecord := RECORD
unsigned4 house_id;
unsigned4 person_id;
string20  surname;
string20  forename;
    END;

childPersonRecord := RECORD
unsigned4 person_id;
string20  surname;
string20  forename;
    END;

combinedRecord := 
                RECORD
householdRecord;
unsigned4           numPeople;
childPersonRecord   children[SELF.numPeople];
                END;


personDataset := DATASET('person',personRecord,FLAT);
householdDataset := DATASET('household',householdRecord,FLAT);

combinedRecord householdToCombined(householdRecord l) := 
                TRANSFORM
                    SELF.numPeople := 0;
                    SELF := l;
                    SELF.children := [];
                    //MORE What about self.children?
                END;


combinedRecord doDenormalize(combinedRecord l, personRecord r, integer c) := 
                TRANSFORM
                    SELF.numPeople := l.numPeople + 1;
                    SELF := l;
                    SELF.children[c] := r;  //NB: Need to allow multiple assignements to same field.
                END;


o1 := PROJECT(householdDataset, householdToCombined(LEFT));

o2 := denormalize(o1, personDataset, LEFT.house_id = RIGHT.house_id, doDenormalize(LEFT, RIGHT, COUNTER));

output(o2,,'out.d00');
