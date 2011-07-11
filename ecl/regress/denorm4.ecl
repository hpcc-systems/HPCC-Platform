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
string8   house_id;
string20  address1;
string20  zip;
string2   nl;   
    END;


childPersonRecord := RECORD
string8   person_id;
string20  surname;
string22  forename;
string2   nl;
    END;

personRecord := RECORD
string8      person_houseid;
childPersonRecord x;
    END;

combinedRecord := 
                RECORD
householdRecord;
unsigned4           numPeople;
DATASET(childPersonRecord, COUNT(SELF.numPeople))   children;
                END;


personDataset := DATASET('person.d01',personRecord,FLAT);
householdDataset := DATASET('househ.d01',householdRecord,FLAT);



combinedRecord householdToCombined(householdRecord l) := 
                TRANSFORM
                    SELF.numPeople := 0;
                    SELF.children := [];
                    SELF := l;
                END;


combinedRecord doDenormalize(combinedRecord l, personRecord r) := 
                TRANSFORM
                    SELF.numPeople := l.numPeople + 1;
                    SELF.children := l.children + r.x;
                    SELF := l;
                END;


o1 := PROJECT(householdDataset, householdToCombined(LEFT));

o2 := denormalize(o1, personDataset, LEFT.house_id = RIGHT.person_houseid, doDenormalize(LEFT, RIGHT));

output(o2,,'denorm4.d00');
