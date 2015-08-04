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
