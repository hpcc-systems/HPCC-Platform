/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
